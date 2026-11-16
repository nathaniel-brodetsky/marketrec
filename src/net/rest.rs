//! Binance Spot REST: symbol rules and depth snapshots.
//!
//! Public market data needs no key. The default base URL is `https://data-api.binance.vision`,
//! which Binance documents for market-data-only clients; `https://api.binance.com` serves
//! the same endpoints.
//!
//! Limits (per IP, see the Binance REST documentation): `exchangeInfo` weighs 20, a depth
//! snapshot with `limit` 1001-5000 weighs 250, the budget is 6000 per minute. HTTP 429 and
//! 418 carry `Retry-After` in seconds; the error returned here keeps it so the caller can
//! back off. The weight used so far is read from `X-MBX-USED-WEIGHT-1M`.

use std::fmt;
use std::time::Duration;

use serde::Deserialize;

pub const DEFAULT_BASE: &str = "https://data-api.binance.vision";

/// Trading rules of one symbol, as needed to read its prices and quantities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolInfo {
    pub symbol: String,
    /// `TRADING`, `HALT`, `BREAK`, ...
    pub status: String,
    /// `PRICE_FILTER.tickSize`, for example "0.01000000".
    pub tick_size: String,
    /// `LOT_SIZE.stepSize`, for example "0.00001000".
    pub step_size: String,
}

/// A response body with the request weight the IP has used in the current minute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fetched {
    pub body: Vec<u8>,
    pub used_weight_1m: Option<u32>,
}

#[derive(Debug)]
pub enum RestError {
    /// Connection, TLS or timeout: no HTTP response.
    Transport(reqwest::Error),
    /// The server answered with a non-success status. `retry_after` is in seconds.
    Status {
        code: u16,
        retry_after: Option<u64>,
        body: String,
    },
    /// A success response that does not have the expected shape.
    Unexpected(String),
}

impl RestError {
    /// The server asks to slow down (429) or has banned the IP for a while (418).
    pub fn is_rate_limit(&self) -> bool {
        matches!(
            self,
            RestError::Status {
                code: 418 | 429,
                ..
            }
        )
    }
}

impl fmt::Display for RestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RestError::Transport(e) => {
                // reqwest keeps the useful part (refused, DNS, TLS, timeout) in the source chain
                write!(f, "request failed: {e}")?;
                let mut source = std::error::Error::source(e);
                while let Some(cause) = source {
                    write!(f, ": {cause}")?;
                    source = cause.source();
                }
                Ok(())
            }
            RestError::Status {
                code,
                retry_after: Some(s),
                body,
            } => write!(f, "HTTP {code}, retry after {s} s: {body}"),
            RestError::Status {
                code,
                retry_after: None,
                body,
            } => write!(f, "HTTP {code}: {body}"),
            RestError::Unexpected(msg) => write!(f, "unexpected response: {msg}"),
        }
    }
}

impl std::error::Error for RestError {}

impl From<reqwest::Error> for RestError {
    fn from(e: reqwest::Error) -> Self {
        RestError::Transport(e)
    }
}

pub struct Rest {
    http: reqwest::Client,
    base: String,
}

impl Rest {
    pub fn new(base: &str, timeout: Duration) -> Result<Self, RestError> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .user_agent(concat!("marketrec/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self {
            http,
            base: base.trim_end_matches('/').to_owned(),
        })
    }

    /// `GET /api/v3/exchangeInfo?symbol=...` (weight 20).
    pub async fn symbol_info(&self, symbol: &str) -> Result<SymbolInfo, RestError> {
        let fetched = self
            .get(&format!("/api/v3/exchangeInfo?symbol={symbol}"))
            .await?;
        parse_symbol_info(&fetched.body, symbol)
    }

    /// `GET /api/v3/depth?symbol=...&limit=...` (weight 250 for 1001-5000 levels). The body is
    /// returned as received: it is recorded verbatim and decoded by the pipeline.
    pub async fn depth(&self, symbol: &str, limit: u32) -> Result<Fetched, RestError> {
        self.get(&format!("/api/v3/depth?symbol={symbol}&limit={limit}"))
            .await
    }

    async fn get(&self, path_and_query: &str) -> Result<Fetched, RestError> {
        let resp = self
            .http
            .get(format!("{}{path_and_query}", self.base))
            .send()
            .await?;
        let status = resp.status();
        let header = |name: &str| {
            resp.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
        };
        let used_weight_1m = header("x-mbx-used-weight-1m").and_then(|v| v.parse().ok());
        let retry_after = header("retry-after").and_then(|v| v.parse().ok());
        let body = resp.bytes().await?.to_vec();
        if !status.is_success() {
            let body = String::from_utf8_lossy(&body[..body.len().min(500)]).into_owned();
            return Err(RestError::Status {
                code: status.as_u16(),
                retry_after,
                body,
            });
        }
        Ok(Fetched {
            body,
            used_weight_1m,
        })
    }
}

#[derive(Deserialize)]
struct ExchangeInfo {
    symbols: Vec<SymbolEntry>,
}

#[derive(Deserialize)]
struct SymbolEntry {
    symbol: String,
    status: String,
    filters: Vec<Filter>,
}

#[derive(Deserialize)]
struct Filter {
    #[serde(rename = "filterType")]
    filter_type: String,
    #[serde(rename = "tickSize")]
    tick_size: Option<String>,
    #[serde(rename = "stepSize")]
    step_size: Option<String>,
}

fn parse_symbol_info(body: &[u8], symbol: &str) -> Result<SymbolInfo, RestError> {
    let info: ExchangeInfo =
        serde_json::from_slice(body).map_err(|e| RestError::Unexpected(e.to_string()))?;
    let entry = info
        .symbols
        .into_iter()
        .find(|s| s.symbol == symbol)
        .ok_or_else(|| RestError::Unexpected(format!("{symbol} is not in the response")))?;
    let filter = |kind: &str, pick: fn(&Filter) -> &Option<String>| {
        entry
            .filters
            .iter()
            .find(|f| f.filter_type == kind)
            .and_then(|f| pick(f).clone())
            .ok_or_else(|| RestError::Unexpected(format!("{symbol} has no {kind} filter")))
    };
    let tick_size = filter("PRICE_FILTER", |f| &f.tick_size)?;
    let step_size = filter("LOT_SIZE", |f| &f.step_size)?;
    Ok(SymbolInfo {
        symbol: entry.symbol.clone(),
        status: entry.status.clone(),
        tick_size,
        step_size,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// A local HTTP server: the first route whose prefix matches the request path answers.
    /// Records the request lines.
    pub(crate) struct MockHttp {
        pub base: String,
        pub requests: Arc<Mutex<Vec<String>>>,
    }

    /// (path prefix, status line, extra headers, body)
    pub(crate) type Route = (&'static str, &'static str, &'static str, &'static str);

    pub(crate) async fn mock_http_routes(routes: Vec<Route>, delay: Duration) -> MockHttp {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let routes = Arc::new(routes);
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let (seen, routes) = (seen.clone(), routes.clone());
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let mut n = 0;
                    while !buf[..n].windows(4).any(|w| w == b"\r\n\r\n") {
                        match sock.read(&mut buf[n..]).await {
                            Ok(0) | Err(_) => return,
                            Ok(k) => n += k,
                        }
                    }
                    let line = String::from_utf8_lossy(&buf[..n])
                        .lines()
                        .next()
                        .unwrap_or_default()
                        .to_owned();
                    let path = line.split(' ').nth(1).unwrap_or_default().to_owned();
                    seen.lock().unwrap().push(line);
                    tokio::time::sleep(delay).await;
                    let (status, headers, body) = routes
                        .iter()
                        .find(|r| path.starts_with(r.0))
                        .map_or(("404 Not Found", "", ""), |r| (r.1, r.2, r.3));
                    let resp = format!(
                        "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n{headers}\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        MockHttp { base, requests }
    }

    pub(crate) async fn mock_http(
        status: &'static str,
        headers: &'static str,
        body: &'static str,
        delay: Duration,
    ) -> MockHttp {
        mock_http_routes(vec![("/", status, headers, body)], delay).await
    }

    /// The mock server on its own thread, for code that runs its own runtime. Returns the base URL.
    pub(crate) fn mock_http_thread(routes: Vec<Route>) -> String {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let m = mock_http_routes(routes, Duration::ZERO).await;
                tx.send(m.base).unwrap();
                std::future::pending::<()>().await;
            });
        });
        rx.recv().unwrap()
    }

    pub(crate) const EXCHANGE_INFO: &str = r#"{"timezone":"UTC","serverTime":1795219200000,"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":6000}],"exchangeFilters":[],"symbols":[{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","baseAssetPrecision":8,"quoteAsset":"USDT","quotePrecision":8,"quoteAssetPrecision":8,"orderTypes":["LIMIT","MARKET"],"icebergAllowed":true,"filters":[{"filterType":"PRICE_FILTER","minPrice":"0.01000000","maxPrice":"1000000.00000000","tickSize":"0.01000000"},{"filterType":"LOT_SIZE","minQty":"0.00001000","maxQty":"9000.00000000","stepSize":"0.00001000"},{"filterType":"ICEBERG_PARTS","limit":10},{"filterType":"NOTIONAL","minNotional":"5.00000000","applyMinToMarket":true,"maxNotional":"9000000.00000000","applyMaxToMarket":false,"avgPriceMins":5}],"permissions":[],"permissionSets":[["SPOT","MARGIN"]],"defaultSelfTradePreventionMode":"EXPIRE_MAKER","allowedSelfTradePreventionModes":["NONE"]}]}"#;

    fn rest(base: &str) -> Rest {
        Rest::new(base, Duration::from_millis(500)).unwrap()
    }

    #[tokio::test]
    async fn symbol_info() {
        let m = mock_http(
            "200 OK",
            "x-mbx-used-weight-1m: 20\r\n",
            EXCHANGE_INFO,
            Duration::ZERO,
        )
        .await;
        let info = rest(&m.base).symbol_info("BTCUSDT").await.unwrap();
        assert_eq!(
            info,
            SymbolInfo {
                symbol: "BTCUSDT".into(),
                status: "TRADING".into(),
                tick_size: "0.01000000".into(),
                step_size: "0.00001000".into()
            }
        );
        assert_eq!(
            m.requests.lock().unwrap()[0],
            "GET /api/v3/exchangeInfo?symbol=BTCUSDT HTTP/1.1"
        );
    }

    #[test]
    fn missing_filter_or_symbol_is_an_error() {
        let no_lot = EXCHANGE_INFO.replace("\"LOT_SIZE\"", "\"OTHER\"");
        assert!(
            matches!(parse_symbol_info(no_lot.as_bytes(), "BTCUSDT"), Err(RestError::Unexpected(m)) if m.contains("LOT_SIZE"))
        );
        assert!(matches!(
            parse_symbol_info(EXCHANGE_INFO.as_bytes(), "ETHUSDT"),
            Err(RestError::Unexpected(_))
        ));
        assert!(matches!(
            parse_symbol_info(b"<html>", "BTCUSDT"),
            Err(RestError::Unexpected(_))
        ));
    }

    #[tokio::test]
    async fn depth_body_is_returned_verbatim_with_the_weight() {
        let body = r#"{"lastUpdateId":1027024,"bids":[["4.00000000","431.00000000"]],"asks":[["4.00000200","12.00000000"]]}"#;
        let m = mock_http(
            "200 OK",
            "X-MBX-USED-WEIGHT-1M: 270\r\n",
            body,
            Duration::ZERO,
        )
        .await;
        let f = rest(&m.base).depth("BTCUSDT", 5000).await.unwrap();
        assert_eq!(f.body, body.as_bytes());
        assert_eq!(f.used_weight_1m, Some(270));
        assert_eq!(
            m.requests.lock().unwrap()[0],
            "GET /api/v3/depth?symbol=BTCUSDT&limit=5000 HTTP/1.1"
        );
    }

    #[tokio::test]
    async fn rate_limit_keeps_retry_after() {
        let m = mock_http(
            "429 Too Many Requests",
            "Retry-After: 7\r\n",
            r#"{"code":-1003,"msg":"Too many requests."}"#,
            Duration::ZERO,
        )
        .await;
        let err = rest(&m.base).depth("BTCUSDT", 5000).await.unwrap_err();
        assert!(err.is_rate_limit());
        assert!(
            matches!(&err, RestError::Status { code: 429, retry_after: Some(7), body } if body.contains("-1003"))
        );
    }

    #[tokio::test]
    async fn invalid_symbol_is_reported_with_the_exchange_message() {
        let m = mock_http(
            "400 Bad Request",
            "",
            r#"{"code":-1121,"msg":"Invalid symbol."}"#,
            Duration::ZERO,
        )
        .await;
        let err = rest(&m.base).symbol_info("NOPE").await.unwrap_err();
        assert!(!err.is_rate_limit());
        assert_eq!(
            err.to_string(),
            r#"HTTP 400: {"code":-1121,"msg":"Invalid symbol."}"#
        );
    }

    #[tokio::test]
    async fn slow_server_times_out() {
        let m = mock_http("200 OK", "", "{}", Duration::from_secs(5)).await;
        let started = std::time::Instant::now();
        let err = rest(&m.base).depth("BTCUSDT", 5).await.unwrap_err();
        assert!(matches!(err, RestError::Transport(_)));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn closed_port_is_a_transport_error_that_says_why() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let err = rest(&base).depth("BTCUSDT", 5).await.unwrap_err();
        assert!(matches!(err, RestError::Transport(_)));
        assert!(err.to_string().to_lowercase().contains("refused"), "{err}");
    }
}
