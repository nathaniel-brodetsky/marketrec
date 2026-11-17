//! Snapshot requests from the pipeline, answered with REST depth snapshots, paced to stay
//! far below Binance's request-weight limit.
//!
//! Every answer becomes a `snapshot` frame whose `aux` is the request ID, so the pipeline
//! (live or on replay) matches it to its request. A failed request is answered too, with a
//! small JSON error body: the pipeline cannot decode it, reports it and asks again. Requests
//! that pile up while waiting are merged: only the newest is fetched, because the pipeline
//! ignores answers to older ones.
//!
//! Pacing: at least `min_interval` between fetches; longer after HTTP 429 or 418 (the
//! server's `Retry-After`) and when the IP has used more than 80% of its weight for the
//! minute.

use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};

use crate::frames::{FrameKind, OwnedFrame};
use crate::net::rest::{Rest, RestError};
use crate::net::ws::now_ns;

#[derive(Debug, Clone, Copy)]
pub struct FetchConfig {
    /// Levels per side; weight 250 for 1001-5000.
    pub limit: u32,
    pub min_interval: Duration,
    /// Weight budget per minute; above 80% of it the fetcher waits.
    pub weight_limit_1m: u32,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            limit: 5000,
            min_interval: Duration::from_secs(2),
            weight_limit_1m: 6000,
        }
    }
}

pub async fn run(
    rest: Rest,
    symbol: String,
    cfg: FetchConfig,
    mut requests: mpsc::UnboundedReceiver<u64>,
    frames: mpsc::Sender<OwnedFrame>,
    mut stop: watch::Receiver<bool>,
) {
    let mut next_allowed = Instant::now();
    loop {
        let mut id = tokio::select! {
            r = requests.recv() => match r { Some(id) => id, None => return },
            _ = stop.changed() => return,
        };
        let wait = next_allowed.saturating_duration_since(Instant::now());
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = stop.changed() => return,
        }
        while let Ok(newer) = requests.try_recv() {
            id = id.max(newer);
        }
        let result = tokio::select! {
            r = rest.depth(&symbol, cfg.limit) => r,
            _ = stop.changed() => return,
        };
        let (payload, pause) = match result {
            Ok(f) => {
                let busy = f
                    .used_weight_1m
                    .is_some_and(|w| u64::from(w) * 5 > u64::from(cfg.weight_limit_1m) * 4);
                (
                    f.body,
                    if busy {
                        Duration::from_secs(15)
                    } else {
                        Duration::ZERO
                    },
                )
            }
            Err(e) => {
                let retry = match &e {
                    RestError::Status {
                        retry_after: Some(s),
                        ..
                    } => Duration::from_secs(*s),
                    _ => Duration::ZERO,
                };
                (error_body(&e), retry)
            }
        };
        next_allowed = Instant::now() + cfg.min_interval.max(pause);
        let frame = OwnedFrame {
            kind: FrameKind::Snapshot,
            recv_ts: now_ns(),
            aux: id,
            payload,
        };
        if frames.send(frame).await.is_err() {
            return;
        }
    }
}

fn error_body(e: &RestError) -> Vec<u8> {
    let (status, retry_after) = match e {
        RestError::Status {
            code, retry_after, ..
        } => (Some(*code), *retry_after),
        _ => (None, None),
    };
    serde_json::json!({ "error": e.to_string(), "status": status, "retry_after": retry_after })
        .to_string()
        .into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::rest::tests::mock_http_routes;

    async fn start(
        routes: Vec<crate::net::rest::tests::Route>,
        cfg: FetchConfig,
    ) -> (
        mpsc::UnboundedSender<u64>,
        mpsc::Receiver<OwnedFrame>,
        watch::Sender<bool>,
        std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        let m = mock_http_routes(routes, Duration::ZERO).await;
        let rest = Rest::new(&m.base, Duration::from_secs(2)).unwrap();
        let (req_tx, req_rx) = mpsc::unbounded_channel();
        let (frame_tx, frame_rx) = mpsc::channel(16);
        let (stop_tx, stop_rx) = watch::channel(false);
        tokio::spawn(run(rest, "BTCUSDT".into(), cfg, req_rx, frame_tx, stop_rx));
        (req_tx, frame_rx, stop_tx, m.requests)
    }

    const DEPTH: &str = r#"{"lastUpdateId":1000,"bids":[],"asks":[]}"#;

    #[tokio::test]
    async fn answers_with_the_body_and_merges_waiting_requests() {
        let cfg = FetchConfig {
            limit: 5000,
            min_interval: Duration::from_millis(200),
            weight_limit_1m: 6000,
        };
        let (req, mut frames, _stop, seen) = start(
            vec![(
                "/api/v3/depth",
                "200 OK",
                "x-mbx-used-weight-1m: 250\r\n",
                DEPTH,
            )],
            cfg,
        )
        .await;
        req.send(1).unwrap();
        let first = frames.recv().await.unwrap();
        assert_eq!(
            (first.kind, first.aux, first.payload.as_slice()),
            (FrameKind::Snapshot, 1, DEPTH.as_bytes())
        );
        let t = Instant::now();
        for id in 2..=4 {
            req.send(id).unwrap();
        }
        let second = frames.recv().await.unwrap();
        assert_eq!(
            second.aux, 4,
            "requests 2 and 3 are stale by the time the pause ends"
        );
        assert!(
            t.elapsed() >= Duration::from_millis(150),
            "waited for the minimum interval"
        );
        assert_eq!(seen.lock().unwrap().len(), 2);
        assert_eq!(
            seen.lock().unwrap()[0],
            "GET /api/v3/depth?symbol=BTCUSDT&limit=5000 HTTP/1.1"
        );
    }

    #[tokio::test]
    async fn rate_limit_answer_is_an_error_body_and_respects_retry_after() {
        let cfg = FetchConfig {
            limit: 100,
            min_interval: Duration::from_millis(10),
            weight_limit_1m: 6000,
        };
        let routes = vec![(
            "/api/v3/depth",
            "429 Too Many Requests",
            "Retry-After: 1\r\n",
            r#"{"code":-1003,"msg":"Too many requests."}"#,
        )];
        let (req, mut frames, _stop, _) = start(routes, cfg).await;
        req.send(1).unwrap();
        let f = frames.recv().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&f.payload).unwrap();
        assert_eq!(
            (body["status"].as_u64(), body["retry_after"].as_u64()),
            (Some(429), Some(1))
        );
        assert!(body["error"].as_str().unwrap().contains("-1003"));
        let t = Instant::now();
        req.send(2).unwrap();
        frames.recv().await.unwrap();
        assert!(
            t.elapsed() >= Duration::from_millis(900),
            "Retry-After of 1 s was honoured: {:?}",
            t.elapsed()
        );
    }

    #[tokio::test]
    async fn heavy_weight_use_slows_down() {
        let cfg = FetchConfig {
            limit: 5000,
            min_interval: Duration::from_millis(10),
            weight_limit_1m: 6000,
        };
        let (req, mut frames, stop, _) = start(
            vec![(
                "/api/v3/depth",
                "200 OK",
                "x-mbx-used-weight-1m: 5000\r\n",
                DEPTH,
            )],
            cfg,
        )
        .await;
        req.send(1).unwrap();
        frames.recv().await.unwrap();
        req.send(2).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(500), frames.recv())
                .await
                .is_err(),
            "must wait after using 83% of the weight"
        );
        stop.send(true).unwrap();
    }

    #[tokio::test]
    async fn stops_on_request() {
        let (req, mut frames, stop, _) = start(
            vec![("/api/v3/depth", "200 OK", "", DEPTH)],
            FetchConfig::default(),
        )
        .await;
        stop.send(true).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = req.send(1);
        assert!(
            frames.recv().await.is_none(),
            "the fetcher ended and dropped its sender"
        );
    }
}
