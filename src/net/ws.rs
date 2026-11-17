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
//!   earlier closing; in both cases the session connects a successor before the old
//!   connection goes (make-before-break, see `run`);
//! - at most 300 connection attempts per 5 minutes per IP: failed attempts back off
//!   exponentially with jitter, so many clients do not retry in step.
//!
//! A connection that goes silent for `stale_after` is treated as dead.
//!
//! Every `probe_every` the session sends its own ping with the send time as payload; the
//! pong comes back as an `rtt` frame, the round trip of the link (`crate::timing`).

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot, watch};
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
    /// Pause between the session's own round-trip pings.
    pub probe_every: Duration,
    /// Age at which a connection is replaced, before the exchange's 24-hour limit.
    pub rotate_after: Duration,
    /// How long a successor delivers alongside the connection it replaces.
    pub overlap: Duration,
    pub connect_timeout: Duration,
    pub backoff: BackoffConfig,
    pub seed: u64,
}

impl WsConfig {
    pub fn new(url: String) -> Self {
        Self {
            url,
            stale_after: Duration::from_secs(15),
            probe_every: PROBE_EVERY,
            rotate_after: ROTATE_AFTER,
            overlap: OVERLAP,
            connect_timeout: Duration::from_secs(10),
            backoff: BackoffConfig::default(),
            seed: now_ns() as u64,
        }
    }
}

/// Default pause between round-trip pings.
pub const PROBE_EVERY: Duration = Duration::from_secs(5);
/// Default age at which a connection is replaced: an hour before Binance's 24-hour limit.
pub const ROTATE_AFTER: Duration = Duration::from_secs(23 * 3600);
/// Default time a successor delivers alongside the connection it replaces.
pub const OVERLAP: Duration = Duration::from_secs(5);

/// How a connection ended.
#[derive(Debug, Clone, PartialEq, Eq)]
enum End {
    /// The link failed: replace it after a backoff pause.
    Failed(String),
    /// The session closed it: to stop, or because a successor took over.
    Closed(String),
}

/// Payload of the `disconnected` frame of a connection closed because its successor took
/// over.
pub const HANDOVER: &str = "handover";

const SHUTDOWN_NOTICE: &[u8] = br#"{"stream":"!serverShutdown""#;

/// What a connection task tells the session.
#[derive(Debug)]
enum LineEvent {
    Connected,
    FirstMessage,
    /// The exchange announced that it will close this connection.
    Notice,
    Ended {
        end: End,
        lived: Duration,
    },
}

/// One connection, as the session sees it.
struct Line {
    conn: u64,
    close: Option<oneshot::Sender<String>>,
    connected_at: Option<Instant>,
    first_message_at: Option<Instant>,
    /// To be replaced: it is near the 24-hour limit or the exchange announced its end.
    retire: bool,
}

impl Line {
    fn close(&mut self, reason: &str) {
        if let Some(c) = self.close.take() {
            let _ = c.send(reason.to_owned());
        }
    }
}

async fn sleep_until(t: Option<Instant>) {
    match t {
        Some(t) => tokio::time::sleep_until(t.into()).await,
        None => std::future::pending().await,
    }
}

/// Run until `stop` becomes true or the receiver of `tx` is dropped. Connection numbers
/// start at 1 and grow with every attempt.
///
/// A connection is replaced make-before-break: when it nears `rotate_after` or the exchange
/// announces its end, a successor connects while it keeps delivering, and it is closed
/// (reason `handover`) once the successor has delivered for `overlap`. Both deliver the
/// same stream meanwhile; the pipeline keeps the first copy of every message, so the book
/// carries on without a gap and without a new snapshot. A connection that fails is
/// replaced after a backoff pause.
pub async fn run(cfg: WsConfig, tx: mpsc::Sender<OwnedFrame>, mut stop: watch::Receiver<bool>) {
    let mut backoff = Backoff::new(cfg.backoff, cfg.seed);
    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<(u64, LineEvent)>();
    let mut lines: Vec<Line> = Vec::new();
    let mut next_conn = 0u64;
    let mut start_at: Option<Instant> = Some(Instant::now());
    let mut stopping = *stop.borrow();
    loop {
        if stopping || tx.is_closed() {
            stopping = true;
            start_at = None;
            for l in &mut lines {
                l.close(STOPPED);
            }
            if lines.is_empty() {
                return;
            }
        }
        // a single serving connection retires at its age limit
        let rotate_at = match lines.as_slice() {
            [l] if !l.retire => l.connected_at.map(|t| t + cfg.rotate_after),
            _ => None,
        };
        // the retiring connection closes once its successor has delivered for a while
        let handover_at = match lines.as_slice() {
            [old, new] if old.retire && old.close.is_some() => {
                new.first_message_at.map(|t| t + cfg.overlap)
            }
            _ => None,
        };
        tokio::select! {
            Some((conn, ev)) = ev_rx.recv() => {
                let Some(i) = lines.iter().position(|l| l.conn == conn) else { continue };
                match ev {
                    LineEvent::Connected => lines[i].connected_at = Some(Instant::now()),
                    LineEvent::FirstMessage => lines[i].first_message_at = Some(Instant::now()),
                    LineEvent::Notice => {
                        lines[i].retire = true;
                        if lines.len() == 1 && start_at.is_none() {
                            start_at = Some(Instant::now());
                        }
                    }
                    LineEvent::Ended { end, lived } => {
                        lines.remove(i);
                        if lived >= cfg.backoff.stable_after {
                            backoff.reset();
                        }
                        let replace = match lines.as_slice() {
                            [] => true,
                            // the successor of a retiring connection failed: try again
                            [old] => old.retire,
                            _ => false,
                        };
                        if replace && matches!(end, End::Failed(_)) && start_at.is_none() {
                            start_at = Some(Instant::now() + backoff.next_delay());
                        }
                    }
                }
            }
            _ = sleep_until(start_at), if start_at.is_some() => {
                start_at = None;
                next_conn += 1;
                let (close_tx, close_rx) = oneshot::channel();
                lines.push(Line {
                    conn: next_conn,
                    close: Some(close_tx),
                    connected_at: None,
                    first_message_at: None,
                    retire: false,
                });
                tokio::spawn(line(next_conn, cfg.clone(), tx.clone(), ev_tx.clone(), close_rx));
            }
            _ = sleep_until(rotate_at), if rotate_at.is_some() => {
                lines[0].retire = true;
                start_at = Some(Instant::now());
            }
            _ = sleep_until(handover_at), if handover_at.is_some() => lines[0].close(HANDOVER),
            _ = stop.changed() => stopping = true,
        }
    }
}

/// One connection, from the attempt to its `disconnected` frame.
async fn line(
    conn: u64,
    cfg: WsConfig,
    tx: mpsc::Sender<OwnedFrame>,
    events: mpsc::UnboundedSender<(u64, LineEvent)>,
    mut close: oneshot::Receiver<String>,
) {
    let mut connected = None;
    let attempt = tokio::time::timeout(
        cfg.connect_timeout,
        tokio_tungstenite::connect_async(cfg.url.as_str()),
    );
    let end = tokio::select! {
        r = attempt => match r {
            Ok(Ok((ws, _))) => {
                if send(&tx, FrameKind::Connected, conn, cfg.url.as_bytes().to_vec()).await.is_err() {
                    End::Closed("frame receiver closed".into())
                } else {
                    connected = Some(Instant::now());
                    let _ = events.send((conn, LineEvent::Connected));
                    pump(ws, conn, &tx, &mut close, &cfg, &events).await
                }
            }
            Ok(Err(e)) => End::Failed(format!("connect failed: {e}")),
            Err(_) => End::Failed(format!("connect timed out after {:?}", cfg.connect_timeout)),
        },
        _ = &mut close => End::Closed("stopped while connecting".into()),
    };
    let reason = match &end {
        End::Failed(r) | End::Closed(r) => r.clone(),
    };
    let _ = send(&tx, FrameKind::Disconnected, conn, reason.into_bytes()).await;
    let lived = connected.map_or(Duration::ZERO, |t: Instant| t.elapsed());
    let _ = events.send((conn, LineEvent::Ended { end, lived }));
}

async fn pump<S>(
    mut ws: tokio_tungstenite::WebSocketStream<S>,
    conn: u64,
    tx: &mpsc::Sender<OwnedFrame>,
    close: &mut oneshot::Receiver<String>,
    cfg: &WsConfig,
    events: &mpsc::UnboundedSender<(u64, LineEvent)>,
) -> End
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let stale_after = cfg.stale_after;
    let mut last_msg = tokio::time::Instant::now();
    let mut probe = tokio::time::interval_at(last_msg + cfg.probe_every, cfg.probe_every);
    probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut first = true;
    loop {
        let next = tokio::select! {
            m = ws.next() => m,
            _ = tokio::time::sleep_until(last_msg + stale_after) => {
                return End::Failed(format!("stale: no message for {stale_after:?}"));
            }
            _ = probe.tick() => {
                let sent = now_ns().to_le_bytes().to_vec();
                if let Err(e) = ws.send(Message::Ping(sent.into())).await {
                    return End::Failed(format!("ping failed: {e}"));
                }
                continue;
            }
            reason = &mut *close => {
                let _ = ws.close(None).await;
                return End::Closed(reason.unwrap_or_else(|_| STOPPED.into()));
            }
        };
        last_msg = tokio::time::Instant::now();
        let msg = match next {
            None => return End::Failed("stream ended".into()),
            Some(Err(e)) => return End::Failed(format!("read error: {e}")),
            Some(Ok(m)) => m,
        };
        let (kind, payload) = match msg {
            Message::Text(t) => (FrameKind::Text, t.as_bytes().to_vec()),
            Message::Binary(b) => (FrameKind::Binary, b.to_vec()),
            // the answer to our own ping: 8 bytes of send time
            Message::Pong(p) if p.len() == 8 => (FrameKind::Rtt, p.to_vec()),
            Message::Close(Some(cf)) => {
                return End::Failed(format!(
                    "closed by server: {} {}",
                    u16::from(cf.code),
                    cf.reason
                ));
            }
            Message::Close(None) => return End::Failed("closed by server".into()),
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue, // pongs are sent by tungstenite
        };
        let notice = kind == FrameKind::Text && payload.starts_with(SHUTDOWN_NOTICE);
        if send(tx, kind, conn, payload).await.is_err() {
            return End::Closed("frame receiver closed".into());
        }
        if kind != FrameKind::Rtt && std::mem::take(&mut first) {
            let _ = events.send((conn, LineEvent::FirstMessage));
        }
        if notice {
            let _ = events.send((conn, LineEvent::Notice));
        }
    }
}

/// Stamp and queue a frame. The time is taken once room in the queue is reserved, so the
/// frames of all connections enter the queue in the order of their receive times.
async fn send(
    tx: &mpsc::Sender<OwnedFrame>,
    kind: FrameKind,
    aux: u64,
    payload: Vec<u8>,
) -> Result<(), ()> {
    let permit = tx.reserve().await.map_err(|_| ())?;
    permit.send(OwnedFrame {
        kind,
        recv_ts: now_ns(),
        aux,
        payload,
    });
    Ok(())
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
        /// Sends each message, then pauses. `NOW_US` in a text message becomes the time of
        /// sending in microseconds, as an exchange stamps its events.
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
                                let m = match m {
                                    Message::Text(t) if t.contains("NOW_US") => Message::text(
                                        t.replace("NOW_US", &(super::now_ns() / 1000).to_string()),
                                    ),
                                    m => m,
                                };
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
            probe_every: Duration::from_secs(3600),
            rotate_after: Duration::from_secs(3600),
            overlap: Duration::from_millis(50),
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
    async fn own_pings_come_back_as_round_trips_and_keep_a_quiet_link_alive() {
        // the server sends nothing but answers pings: without the probes the link would be
        // stale after 300 ms
        let url = server(vec![Script::SendSlowlyThenWait(vec![], Duration::ZERO)]).await;
        let mut cfg = fast(url);
        cfg.probe_every = Duration::from_millis(50);
        let frames = collect(cfg, 7).await;
        assert_eq!(frames[0].kind, FrameKind::Connected);
        let rtts: Vec<&OwnedFrame> = frames[1..].iter().collect();
        assert!(
            rtts.iter().all(|f| f.kind == FrameKind::Rtt && f.aux == 1),
            "{:?}",
            describe(&frames)
        );
        for f in rtts {
            let sent = i64::from_le_bytes(f.payload.as_slice().try_into().unwrap());
            let rtt = f.recv_ts - sent;
            assert!((0..100_000_000).contains(&rtt), "local round trip {rtt} ns");
        }
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

    /// Run a session and collect frames until `done` holds for the last one.
    async fn collect_until(cfg: WsConfig, done: impl Fn(&OwnedFrame) -> bool) -> Vec<OwnedFrame> {
        let (tx, mut rx) = mpsc::channel(256);
        let (stop_tx, stop_rx) = watch::channel(false);
        let task = tokio::spawn(run(cfg, tx, stop_rx));
        let mut out = Vec::new();
        while let Ok(Some(f)) = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
            let end = done(&f);
            out.push(f);
            if end {
                break;
            }
        }
        stop_tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
        out
    }

    fn texts(prefix: &str, n: usize) -> Vec<Message> {
        (1..=n)
            .map(|i| Message::text(format!("{prefix}{i}")))
            .collect()
    }

    fn handover_of(conn: u64) -> impl Fn(&OwnedFrame) -> bool {
        move |f| {
            f.kind == FrameKind::Disconnected && f.aux == conn && f.payload == HANDOVER.as_bytes()
        }
    }

    /// Index of the first frame described as `what`.
    fn at(d: &[String], what: &str) -> usize {
        d.iter()
            .position(|l| l.starts_with(what))
            .unwrap_or_else(|| panic!("no {what} in {d:?}"))
    }

    #[tokio::test]
    async fn server_shutdown_notice_brings_a_successor_before_the_old_connection_goes() {
        let notice = r#"{"stream":"!serverShutdown","data":{"e":"serverShutdown","E":1,"T":2}}"#;
        let mut first = vec![Message::text(notice)];
        first.extend(texts("a", 30));
        let url = server(vec![
            Script::SendSlowlyThenWait(first, Duration::from_millis(10)),
            Script::SendSlowlyThenWait(texts("b", 30), Duration::from_millis(10)),
        ])
        .await;
        let mut cfg = fast(url.clone());
        cfg.backoff.initial = Duration::from_secs(30); // a backoff pause would time the test out
        cfg.backoff.max = Duration::from_secs(30);
        let frames = collect_until(cfg, handover_of(1)).await;
        let d = describe(&frames);
        assert_eq!(d[1], format!("text 1 {notice}"));
        let (connected2, first_b, handover) = (
            at(&d, &format!("connected 2 {url}")),
            at(&d, "text 2 b1"),
            d.len() - 1,
        );
        assert!(connected2 > 1 && first_b > connected2);
        assert!(
            d[connected2..handover]
                .iter()
                .any(|l| l.starts_with("text 1 a")),
            "the old connection kept delivering after its successor connected: {d:?}"
        );
        assert!(
            !d[..handover].iter().any(|l| l.starts_with("disconnected")),
            "{d:?}"
        );
        let overlap = frames[handover].recv_ts - frames[first_b].recv_ts;
        assert!(
            overlap >= 50_000_000,
            "closed {overlap} ns after the successor delivered"
        );
    }

    #[tokio::test]
    async fn a_connection_is_replaced_at_its_age_limit_without_a_break() {
        let url = server(vec![
            Script::SendSlowlyThenWait(texts("a", 40), Duration::from_millis(10)),
            Script::SendSlowlyThenWait(texts("b", 40), Duration::from_millis(10)),
        ])
        .await;
        let mut cfg = fast(url);
        cfg.rotate_after = Duration::from_millis(120);
        cfg.stale_after = Duration::from_secs(5);
        let frames = collect_until(cfg, handover_of(1)).await;
        let d = describe(&frames);
        let (c1, c2) = (at(&d, "connected 1"), at(&d, "connected 2"));
        let age = frames[c2].recv_ts - frames[c1].recv_ts;
        assert!(age >= 120_000_000, "replaced after {age} ns");
        assert!(
            !d[..d.len() - 1]
                .iter()
                .any(|l| l.starts_with("disconnected")),
            "{d:?}"
        );
    }

    #[tokio::test]
    async fn a_failed_successor_is_tried_again_while_the_old_connection_serves() {
        let notice = r#"{"stream":"!serverShutdown","data":{"e":"serverShutdown","E":1,"T":2}}"#;
        let mut first = vec![Message::text(notice)];
        first.extend(texts("a", 40));
        let url = server(vec![
            Script::SendSlowlyThenWait(first, Duration::from_millis(10)),
            Script::SendThenClose(vec![], "busy"),
            Script::SendSlowlyThenWait(texts("c", 40), Duration::from_millis(10)),
        ])
        .await;
        let frames = collect_until(fast(url), handover_of(1)).await;
        let d = describe(&frames);
        let failed = at(&d, "disconnected 2 closed by server: 1001 busy");
        let c3 = at(&d, "connected 3");
        assert!(failed < c3);
        assert!(
            !d[..d.len() - 1]
                .iter()
                .any(|l| l.starts_with("disconnected 1")),
            "connection 1 served until 3 took over: {d:?}"
        );
    }

    #[tokio::test]
    async fn a_notice_and_an_immediate_close_still_bring_a_successor_without_backoff() {
        let notice = r#"{"stream":"!serverShutdown","data":{"e":"serverShutdown","E":1,"T":2}}"#;
        let url = server(vec![
            Script::SendThenClose(vec![Message::text(notice)], "going away"),
            Script::SendThenClose(vec![Message::text("after")], "bye"),
        ])
        .await;
        let mut cfg = fast(url);
        cfg.backoff.initial = Duration::from_secs(30);
        cfg.backoff.max = Duration::from_secs(30);
        let d = describe(&collect(cfg, 5).await);
        assert_eq!(d[1], format!("text 1 {notice}"));
        assert!(d.contains(&"disconnected 1 closed by server: 1001 going away".to_owned()));
        assert!(d.iter().any(|l| l.starts_with("connected 2")));
        assert!(d.contains(&"text 2 after".to_owned()), "{d:?}");
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
