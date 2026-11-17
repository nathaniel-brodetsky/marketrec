//! `marketrec listen`: connect for a while and count what arrives. A quick live check of the
//! WebSocket path that records nothing.

use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};

use super::ws::{self, WsConfig};
use crate::frames::FrameKind;

#[derive(Debug, Clone, Default)]
pub struct ListenSummary {
    pub url: String,
    pub elapsed: Duration,
    pub connections: u64,
    /// Connection number and reason, in order.
    pub disconnects: Vec<(u64, String)>,
    /// Messages per stream kind, for example `depth@100ms`, `bookTicker`, `trade`.
    pub by_stream: BTreeMap<String, u64>,
    pub binary: u64,
    pub bytes: u64,
    pub first_message_after: Option<Duration>,
}

/// `{"stream":"btcusdt@depth@100ms",...` -> `depth@100ms`; `!serverShutdown` stays as is.
fn stream_kind(payload: &[u8]) -> String {
    const PREFIX: &[u8] = br#"{"stream":""#;
    let Some(rest) = payload.strip_prefix(PREFIX) else {
        return "other".into();
    };
    let name = rest
        .iter()
        .position(|&b| b == b'"')
        .map_or(rest, |end| &rest[..end]);
    let name = String::from_utf8_lossy(name);
    name.split_once('@')
        .map_or(name.to_string(), |(_, kind)| kind.to_owned())
}

pub async fn listen(cfg: WsConfig, duration: Duration) -> ListenSummary {
    let mut summary = ListenSummary {
        url: cfg.url.clone(),
        ..ListenSummary::default()
    };
    let (tx, mut rx) = mpsc::channel(4096);
    let (stop_tx, stop_rx) = watch::channel(false);
    let started = Instant::now();
    let session = tokio::spawn(ws::run(cfg, tx, stop_rx));
    let deadline = tokio::time::sleep(duration);
    tokio::pin!(deadline);
    let mut stopping = false;
    loop {
        let frame = tokio::select! {
            f = rx.recv() => f,
            _ = &mut deadline, if !stopping => {
                stopping = true;
                summary.elapsed = started.elapsed();
                let _ = stop_tx.send(true);
                continue;
            }
        };
        let Some(f) = frame else { break };
        match f.kind {
            FrameKind::Connected => summary.connections += 1,
            FrameKind::Disconnected => summary
                .disconnects
                .push((f.aux, String::from_utf8_lossy(&f.payload).into_owned())),
            FrameKind::Text | FrameKind::Binary => {
                summary
                    .first_message_after
                    .get_or_insert_with(|| started.elapsed());
                summary.bytes += f.payload.len() as u64;
                if f.kind == FrameKind::Binary {
                    summary.binary += 1;
                } else {
                    *summary
                        .by_stream
                        .entry(stream_kind(&f.payload))
                        .or_default() += 1;
                }
            }
            FrameKind::Snapshot | FrameKind::Overflow | FrameKind::Rtt => {}
        }
    }
    let _ = session.await;
    summary
}

impl fmt::Display for ListenSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let secs = self.elapsed.as_secs_f64().max(1e-9);
        writeln!(f, "url           {}", self.url)?;
        let first = self
            .first_message_after
            .map_or("no messages".to_owned(), |d| {
                format!("first message after {:.3} s", d.as_secs_f64())
            });
        writeln!(
            f,
            "listened      {secs:.1} s, {} connection(s), {first}",
            self.connections
        )?;
        let streams: Vec<String> = self
            .by_stream
            .iter()
            .map(|(k, n)| format!("{k} {n} ({:.1}/s)", *n as f64 / secs))
            .collect();
        writeln!(
            f,
            "messages      {}",
            if streams.is_empty() {
                "none".to_owned()
            } else {
                streams.join(", ")
            }
        )?;
        if self.binary > 0 {
            writeln!(f, "binary        {}", self.binary)?;
        }
        writeln!(
            f,
            "bytes         {} ({:.1} KB/s)",
            self.bytes,
            self.bytes as f64 / secs / 1000.0
        )?;
        // the same reason repeated (a failing connect loop) is shown once with a count
        let mut reasons: Vec<(&str, usize)> = Vec::new();
        for (_, r) in &self.disconnects {
            match reasons.iter_mut().find(|(seen, _)| seen == r) {
                Some((_, n)) => *n += 1,
                None => reasons.push((r, 1)),
            }
        }
        let link: Vec<String> = reasons
            .iter()
            .map(|(r, n)| {
                if *n == 1 {
                    (*r).to_owned()
                } else {
                    format!("{n} x {r}")
                }
            })
            .collect();
        write!(
            f,
            "disconnects   {}",
            if link.is_empty() {
                "none".to_owned()
            } else {
                link.join("; ")
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use tokio_tungstenite::tungstenite::Message;

    use super::*;
    use crate::net::ws::tests::{Script, fast, server};

    #[test]
    fn stream_kinds() {
        assert_eq!(
            stream_kind(br#"{"stream":"btcusdt@depth@100ms","data":{}}"#),
            "depth@100ms"
        );
        assert_eq!(
            stream_kind(br#"{"stream":"btcusdt@bookTicker","data":{}}"#),
            "bookTicker"
        );
        assert_eq!(
            stream_kind(br#"{"stream":"!serverShutdown","data":{}}"#),
            "!serverShutdown"
        );
        assert_eq!(stream_kind(b"{}"), "other");
        assert_eq!(stream_kind(br#"{"stream":"unterminated"#), "unterminated");
    }

    #[tokio::test]
    async fn counts_messages_per_stream() {
        let msgs = vec![
            Message::text(r#"{"stream":"btcusdt@depth@100ms","data":{}}"#),
            Message::text(r#"{"stream":"btcusdt@bookTicker","data":{}}"#),
            Message::text(r#"{"stream":"btcusdt@bookTicker","data":{}}"#),
            Message::text(r#"{"stream":"btcusdt@trade","data":{}}"#),
        ];
        let url = server(vec![Script::SendThenClose(msgs, "bye"), Script::Silent]).await;
        let mut cfg = fast(url);
        cfg.stale_after = Duration::from_secs(5);
        let s = listen(cfg, Duration::from_millis(400)).await;
        assert_eq!(s.connections, 2);
        assert_eq!(s.by_stream.get("bookTicker"), Some(&2));
        assert_eq!(s.by_stream.get("depth@100ms"), Some(&1));
        assert_eq!(s.by_stream.get("trade"), Some(&1));
        assert_eq!(s.disconnects.len(), 2, "the server's close and our stop");
        assert_eq!(s.disconnects[1].1, "stopped");
        let text = s.to_string();
        assert!(text.contains("2 connection(s)"), "{text}");
        assert!(text.contains("bookTicker 2 ("), "{text}");
    }
}
