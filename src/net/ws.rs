//! Binance Spot WebSocket session: one combined stream, kept alive across disconnects.
//!
//! The session turns everything that happens on the link into frames, in order, each with
//! the local receive time: `connected`, every text or binary message, `disconnected` with
//! the reason. The decoding side never needs to know about sockets, and a recorded session
//! replays exactly.
//!
//! Rules from the Binance Spot documentation that shape this code:
//! - the server sends a ping every 20 s and drops a client that does not answer within a
//!   minute; tungstenite queues the pong itself and sends it while reading, so reading
//!   continuously is enough;
//! - a connection lives at most 24 hours, and a `serverShutdown` event announces an
//!   earlier closing; on that notice the session reconnects right away;
//! - at most 300 connection attempts per 5 minutes per IP: failed attempts back off
//!   exponentially with jitter, so many clients do not retry in step.
//!
//! A connection that goes silent for `stale_after` is treated as dead.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;

use crate::frames::{FrameKind, OwnedFrame};
use crate::json::TimeUnit;

pub const DEFAULT_BASE: &str = "wss://stream.binance.com:9443";

/// Payload of the `disconnected` frame written when the recorder stops on purpose. Every
/// other reason means the link was lost.
pub const STOPPED: &str = "stopped";

/// Combined stream of depth diffs (100 ms), best bid and ask, and trades for one symbol.
pub fn stream_url(base: &str, symbol: &str, unit: TimeUnit) -> String {
    let s = symbol.to_ascii_lowercase();
    let mut url = format!(
        "{}/stream?streams={s}@depth@100ms/{s}@bookTicker/{s}@trade",
        base.trim_end_matches('/')
    );
    if unit == TimeUnit::Micro {
        url.push_str("&timeUnit=MICROSECOND");
    }
    url
}

/// Local wall-clock time in nanoseconds since the Unix epoch.
pub fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
}

#[derive(Debug, Clone, Copy)]
pub struct BackoffConfig {
    /// Pause after the first failure.
    pub initial: Duration,
    pub max: Duration,
    /// A connection that lasted this long resets the pause to `initial`.
    pub stable_after: Duration,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(500),
            max: Duration::from_secs(30),
            stable_after: Duration::from_secs(60),
        }
    }
}

/// Exponential backoff with "equal jitter": the n-th pause is uniform in [d/2, d] where
/// d = min(max, initial * 2^n).
#[derive(Debug, Clone)]
pub struct Backoff {
    cfg: BackoffConfig,
    attempt: u32,
    rng: u64,
}

impl Backoff {
    pub fn new(cfg: BackoffConfig, seed: u64) -> Self {
        Self {
            cfg,
            attempt: 0,
            rng: seed | 1,
        }
    }

    pub fn next_delay(&mut self) -> Duration {
        let factor = 1u32.checked_shl(self.attempt.min(30)).unwrap_or(u32::MAX);
        let d = self.cfg.initial.saturating_mul(factor).min(self.cfg.max);
        self.attempt = self.attempt.saturating_add(1);
        // xorshift64*: enough for jitter, no dependency
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        let unit =
            (self.rng.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as f64 / (1u64 << 53) as f64;
        d / 2 + d.mul_f64(unit / 2.0)
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

#[derive(Debug, Clone)]
pub struct WsConfig {
    pub url: String,
    pub stale_after: Duration,
    pub connect_timeout: Duration,
    pub backoff: BackoffConfig,
    pub seed: u64,
}

impl WsConfig {
    pub fn new(url: String) -> Self {
        Self {
            url,
            stale_after: Duration::from_secs(15),
            connect_timeout: Duration::from_secs(10),
            backoff: BackoffConfig::default(),
            seed: now_ns() as u64,
        }
    }
}

/// How a connection ended.
#[derive(Debug, Clone, PartialEq, Eq)]
enum End {
    /// Reconnect after a backoff pause.
    Failed(String),
    /// The server announced shutdown: reconnect at once.
    Planned,
    /// The caller asked to stop, or nobody reads the frames any more.
    Stop(String),
}

const SHUTDOWN_NOTICE: &[u8] = br#"{"stream":"!serverShutdown""#;

/// Run until `stop` becomes true or the receiver of `tx` is dropped. Connection numbers
/// start at 1 and grow with every attempt.
pub async fn run(cfg: WsConfig, tx: mpsc::Sender<OwnedFrame>, mut stop: watch::Receiver<bool>) {
    let mut backoff = Backoff::new(cfg.backoff, cfg.seed);
    let mut conn = 0u64;
    loop {
        if *stop.borrow() {
            return;
        }
        conn += 1;
        let attempt = tokio::time::timeout(
            cfg.connect_timeout,
            tokio_tungstenite::connect_async(cfg.url.as_str()),
        );
        let end = tokio::select! {
            r = attempt => match r {
                Ok(Ok((ws, _))) => {
                    if send(&tx, FrameKind::Connected, conn, cfg.url.as_bytes().to_vec()).await.is_err() {
                        return;
                    }
                    let started = Instant::now();
                    let end = pump(ws, conn, &tx, &mut stop, cfg.stale_after).await;
                    if started.elapsed() >= cfg.backoff.stable_after {
                        backoff.reset();
                    }
                    end
                }
                Ok(Err(e)) => End::Failed(format!("connect failed: {e}")),
                Err(_) => End::Failed(format!("connect timed out after {:?}", cfg.connect_timeout)),
            },
            _ = stop.changed() => End::Stop("stopped while connecting".into()),
        };
        let reason = match &end {
            End::Failed(r) | End::Stop(r) => r.clone(),
            End::Planned => "server announced shutdown".into(),
        };
        if send(&tx, FrameKind::Disconnected, conn, reason.into_bytes())
            .await
            .is_err()
        {
            return;
        }
        match end {
            End::Stop(_) => return,
            End::Planned => backoff.reset(),
            End::Failed(_) => {
                let pause = backoff.next_delay();
                tokio::select! {
                    _ = tokio::time::sleep(pause) => {}
                    _ = stop.changed() => return,
                }
            }
        }
    }
}

async fn pump<S>(
    mut ws: tokio_tungstenite::WebSocketStream<S>,
    conn: u64,
    tx: &mpsc::Sender<OwnedFrame>,
    stop: &mut watch::Receiver<bool>,
    stale_after: Duration,
) -> End
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let next = tokio::select! {
            r = tokio::time::timeout(stale_after, ws.next()) => r,
            _ = stop.changed() => {
                let _ = ws.close(None).await;
                return End::Stop(STOPPED.into());
            }
        };
        let msg = match next {
            Err(_) => return End::Failed(format!("stale: no message for {stale_after:?}")),
            Ok(None) => return End::Failed("stream ended".into()),
            Ok(Some(Err(e))) => return End::Failed(format!("read error: {e}")),
            Ok(Some(Ok(m))) => m,
        };
        match msg {
            Message::Text(t) => {
                let planned = t.as_bytes().starts_with(SHUTDOWN_NOTICE);
                if send(tx, FrameKind::Text, conn, t.as_bytes().to_vec())
                    .await
                    .is_err()
                {
                    return End::Stop("frame receiver closed".into());
                }
                if planned {
                    let _ = ws.close(None).await;
                    return End::Planned;
                }
            }
            Message::Binary(b) => {
                if send(tx, FrameKind::Binary, conn, b.to_vec()).await.is_err() {
                    return End::Stop("frame receiver closed".into());
                }
            }
            Message::Close(Some(cf)) => {
                return End::Failed(format!(
                    "closed by server: {} {}",
                    u16::from(cf.code),
                    cf.reason
                ));
            }
            Message::Close(None) => return End::Failed("closed by server".into()),
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {} // pongs are sent by tungstenite
        }
    }
}

async fn send(
    tx: &mpsc::Sender<OwnedFrame>,
    kind: FrameKind,
    aux: u64,
    payload: Vec<u8>,
) -> Result<(), ()> {
    tx.send(OwnedFrame {
        kind,
        recv_ts: now_ns(),
        aux,
        payload,
    })
    .await
    .map_err(|_| ())
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::{Arc, Mutex};

    use futures_util::SinkExt;
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    use super::*;

    #[test]
    fn url_for_one_symbol() {
        assert_eq!(
            stream_url("wss://stream.binance.com:9443/", "BTCUSDT", TimeUnit::Micro),
            "wss://stream.binance.com:9443/stream?streams=btcusdt@depth@100ms/btcusdt@bookTicker/btcusdt@trade&timeUnit=MICROSECOND"
        );
        assert!(!stream_url(DEFAULT_BASE, "ETHUSDT", TimeUnit::Milli).contains("timeUnit"));
    }

    #[test]
    fn backoff_grows_is_capped_and_resets() {
        let cfg = BackoffConfig {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(2),
            stable_after: Duration::ZERO,
        };
        let mut b = Backoff::new(cfg, 42);
        let bounds = [100u64, 200, 400, 800, 1600, 2000, 2000, 2000];
        for (i, &d) in bounds.iter().enumerate() {
            let pause = b.next_delay().as_millis() as u64;
            assert!(
                pause >= d / 2 && pause <= d,
                "attempt {i}: {pause} ms not in [{}, {d}]",
                d / 2
            );
        }
        b.reset();
        assert!(b.next_delay() <= Duration::from_millis(100));
    }

    #[test]
    fn jitter_differs_between_clients() {
        let cfg = BackoffConfig::default();
        let a: Vec<_> = {
            let mut b = Backoff::new(cfg, 1);
            (0..5).map(|_| b.next_delay()).collect()
        };
        let c: Vec<_> = {
            let mut b = Backoff::new(cfg, 2);
            (0..5).map(|_| b.next_delay()).collect()
        };
        assert_ne!(a, c);
    }

    /// What the mock server does with each accepted connection, in order of acceptance.
    #[derive(Clone)]
    pub(crate) enum Script {
        /// Send these messages, then close with a reason.
        SendThenClose(Vec<Message>, &'static str),
        /// Send a ping and record the pong's payload; then wait.
        PingAndRecord(Arc<Mutex<Option<Vec<u8>>>>),
        /// Accept and say nothing.
        Silent,
        /// Send these messages with a pause between them, then keep the connection open.
        SendSlowlyThenWait(Vec<Message>, Duration),
    }

    pub(crate) async fn server(scripts: Vec<Script>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/stream", listener.local_addr().unwrap());
        tokio::spawn(async move {
            for script in scripts {
                let Ok((sock, _)) = listener.accept().await else {
                    return;
                };
                let mut ws = tokio_tungstenite::accept_async(sock).await.unwrap();
                tokio::spawn(async move {
                    match script {
                        Script::SendThenClose(msgs, reason) => {
                            for m in msgs {
                                ws.send(m).await.unwrap();
                            }
                            let _ = ws
                                .close(Some(CloseFrame {
                                    code: CloseCode::Away,
                                    reason: reason.into(),
                                }))
                                .await;
                            while let Some(Ok(_)) = ws.next().await {}
                        }
                        Script::PingAndRecord(slot) => {
                            ws.send(Message::Ping(b"hello"[..].into())).await.unwrap();
                            while let Some(Ok(m)) = ws.next().await {
                                if let Message::Pong(p) = m {
                                    *slot.lock().unwrap() = Some(p.to_vec());
                                }
                            }
                        }
                        Script::SendSlowlyThenWait(msgs, pause) => {
                            for m in msgs {
                                if ws.send(m).await.is_err() {
                                    return;
                                }
                                tokio::time::sleep(pause).await;
                            }
                            while let Some(Ok(_)) = ws.next().await {}
                        }
                        Script::Silent => {
                            tokio::time::sleep(Duration::from_secs(60)).await;
                            drop(ws);
                        }
                    }
                });
            }
        });
        url
    }

    /// The mock server on its own thread, for code that runs its own runtime.
    pub(crate) fn server_thread(scripts: Vec<Script>) -> String {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                tx.send(server(scripts).await).unwrap();
                std::future::pending::<()>().await;
            });
        });
        rx.recv().unwrap()
    }

    pub(crate) fn fast(url: String) -> WsConfig {
        WsConfig {
            url,
            stale_after: Duration::from_millis(300),
            connect_timeout: Duration::from_secs(2),
            backoff: BackoffConfig {
                initial: Duration::from_millis(10),
                max: Duration::from_millis(50),
                stable_after: Duration::from_secs(60),
            },
            seed: 7,
        }
    }

    /// Run a session and collect frames until `n` have arrived.
    async fn collect(cfg: WsConfig, n: usize) -> Vec<OwnedFrame> {
        let (tx, mut rx) = mpsc::channel(64);
        let (stop_tx, stop_rx) = watch::channel(false);
        let task = tokio::spawn(run(cfg, tx, stop_rx));
        let mut out = Vec::new();
        while out.len() < n {
            match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
                Ok(Some(f)) => out.push(f),
                _ => break,
            }
        }
        stop_tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
        out
    }

    fn describe(frames: &[OwnedFrame]) -> Vec<String> {
        frames
            .iter()
            .map(|f| {
                format!(
                    "{} {} {}",
                    f.kind.name(),
                    f.aux,
                    String::from_utf8_lossy(&f.payload)
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn messages_and_reconnect_after_close() {
        let url = server(vec![
            Script::SendThenClose(
                vec![
                    Message::text("a"),
                    Message::text("b"),
                    Message::binary(vec![1u8, 2]),
                ],
                "maintenance",
            ),
            Script::SendThenClose(vec![Message::text("c")], "bye"),
        ])
        .await;
        let frames = collect(fast(url.clone()), 8).await;
        let d = describe(&frames);
        assert_eq!(d[0], format!("connected 1 {url}"));
        assert_eq!(
            d[1..4],
            [
                "text 1 a".to_owned(),
                "text 1 b".to_owned(),
                "binary 1 \u{1}\u{2}".to_owned()
            ]
        );
        assert_eq!(d[4], "disconnected 1 closed by server: 1001 maintenance");
        assert_eq!(d[5], format!("connected 2 {url}"));
        assert_eq!(d[6], "text 2 c");
        assert!(
            frames.windows(2).all(|w| w[0].recv_ts <= w[1].recv_ts),
            "receive times never go back"
        );
    }

    #[tokio::test]
    async fn answers_server_pings() {
        let slot = Arc::new(Mutex::new(None));
        let url = server(vec![Script::PingAndRecord(slot.clone())]).await;
        let (tx, mut rx) = mpsc::channel(64);
        let (stop_tx, stop_rx) = watch::channel(false);
        let mut cfg = fast(url);
        cfg.stale_after = Duration::from_secs(5);
        let task = tokio::spawn(run(cfg, tx, stop_rx));
        assert_eq!(rx.recv().await.unwrap().kind, FrameKind::Connected);
        for _ in 0..50 {
            if slot.lock().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(slot.lock().unwrap().as_deref(), Some(&b"hello"[..]));
        stop_tx.send(true).unwrap();
        task.await.unwrap();
        let last = rx.recv().await.unwrap();
        assert_eq!(
            (last.kind, last.payload.as_slice()),
            (FrameKind::Disconnected, &b"stopped"[..])
        );
    }

    #[tokio::test]
    async fn silent_connection_is_dropped_as_stale() {
        let url = server(vec![
            Script::Silent,
            Script::SendThenClose(vec![Message::text("x")], "bye"),
        ])
        .await;
        let d = describe(&collect(fast(url), 4).await);
        assert!(
            d[1].starts_with("disconnected 1 stale: no message for 300ms"),
            "{d:?}"
        );
        assert!(d[2].starts_with("connected 2"));
        assert_eq!(d[3], "text 2 x");
    }

    #[tokio::test]
    async fn server_shutdown_notice_reconnects_at_once() {
        let notice = r#"{"stream":"!serverShutdown","data":{"e":"serverShutdown","E":1,"T":2}}"#;
        let url = server(vec![
            Script::SendThenClose(vec![Message::text(notice)], "unused"),
            Script::SendThenClose(vec![Message::text("after")], "bye"),
        ])
        .await;
        let mut cfg = fast(url);
        cfg.backoff.initial = Duration::from_secs(30); // a backoff pause would make the test time out
        cfg.backoff.max = Duration::from_secs(30);
        let d = describe(&collect(cfg, 5).await);
        assert_eq!(d[1], format!("text 1 {notice}"));
        assert_eq!(d[2], "disconnected 1 server announced shutdown");
        assert!(d[3].starts_with("connected 2"));
        assert_eq!(d[4], "text 2 after");
    }

    #[tokio::test]
    async fn refused_connection_backs_off_and_retries() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("ws://{}/stream", listener.local_addr().unwrap());
        drop(listener);
        let frames = collect(fast(url), 3).await;
        let d = describe(&frames);
        for (i, line) in d.iter().enumerate() {
            assert!(
                line.starts_with(&format!("disconnected {} connect failed", i + 1)),
                "{line}"
            );
        }
        let gaps: Vec<i64> = frames
            .windows(2)
            .map(|w| w[1].recv_ts - w[0].recv_ts)
            .collect();
        assert!(
            gaps.iter().all(|&g| g >= 5_000_000),
            "pauses of at least half the 10 ms initial backoff: {gaps:?}"
        );
    }

    #[tokio::test]
    async fn stops_when_nobody_reads() {
        let url = server(vec![Script::SendThenClose(vec![Message::text("a")], "bye")]).await;
        let (tx, rx) = mpsc::channel(1);
        let (_stop_tx, stop_rx) = watch::channel(false);
        drop(rx);
        tokio::time::timeout(Duration::from_secs(2), run(fast(url), tx, stop_rx))
            .await
            .expect("session must end");
    }
}
