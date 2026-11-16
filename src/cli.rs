//! Command line interface. Every command is declared here; implementations arrive stage by stage.

use std::fmt;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Write};
use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "marketrec",
    version,
    about = "Market-data recorder for Binance Spot"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum Command {
    /// Record one symbol from Binance into the data directory.
    Record {
        /// Symbol as Binance names it, for example BTCUSDT.
        #[arg(long)]
        symbol: String,
        /// Which feed to record.
        #[arg(long, value_enum, default_value_t = Protocol::Json)]
        protocol: Protocol,
        /// Root of the data directory.
        #[arg(long, default_value = "data")]
        out: PathBuf,
    },
    /// Feed recorded frames through the same hot path as the live feed, without network,
    /// and print a summary. Accepts binary frames files and their JSON Lines form.
    Replay {
        /// File with recorded frames.
        #[arg(long)]
        input: PathBuf,
        /// Also print every quality event with its receive time.
        #[arg(long)]
        events: bool,
    },
    /// Check a recorded day: continuity, row counts against the manifest, book rebuild.
    Verify {
        /// Directory of one recorded day, for example data/binance/BTCUSDT/2026-11-21.
        #[arg(long)]
        day: PathBuf,
    },
    /// Run performance experiments on a recorded day.
    Bench {
        /// Directory of one recorded day.
        #[arg(long)]
        day: PathBuf,
    },
    /// Show the trading rules Binance publishes for a symbol. Uses the network.
    Info {
        #[arg(long)]
        symbol: String,
        #[arg(long, default_value = crate::net::rest::DEFAULT_BASE)]
        rest_url: String,
    },
    /// Fetch a depth snapshot and print its best levels. Uses the network.
    Snapshot {
        #[arg(long)]
        symbol: String,
        /// Levels to print on each side.
        #[arg(long, default_value_t = 5)]
        levels: usize,
        /// Levels to request on each side (Binance weight: 250 for 1001-5000).
        #[arg(long, default_value_t = 5000)]
        limit: u32,
        #[arg(long, default_value = crate::net::rest::DEFAULT_BASE)]
        rest_url: String,
    },
    /// Convert frames files between the binary form and readable JSON Lines.
    Frames {
        #[command(subcommand)]
        action: FramesAction,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum FramesAction {
    /// Binary frames to JSON Lines.
    Dump {
        #[arg(long)]
        input: PathBuf,
        /// Output file; standard output if omitted.
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// JSON Lines to binary frames. The output must not exist yet.
    Pack {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Protocol {
    /// Text frames from stream.binance.com.
    Json,
    /// Binary SBE frames from stream-sbe.binance.com.
    Sbe,
    /// Both feeds at once, cross-checked by `verify`.
    Both,
}

#[derive(Debug)]
pub enum CliError {
    /// The command is declared but not built yet.
    NotImplemented(&'static str),
    Io(io::Error),
    Text(crate::frames::text::TextError),
    Replay(crate::replay::ReplayError),
    Rest(crate::net::rest::RestError),
    Decode(crate::json::DecodeError),
    /// The input contains a damaged record; everything before it was processed.
    Damaged,
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::NotImplemented(cmd) => write!(f, "`{cmd}` is not implemented yet"),
            CliError::Io(e) => write!(f, "{e}"),
            CliError::Text(e) => write!(f, "{e}"),
            CliError::Replay(e) => write!(f, "{e}"),
            CliError::Rest(e) => write!(f, "{e}"),
            CliError::Decode(e) => write!(f, "{e}"),
            CliError::Damaged => write!(f, "the input contains a damaged record"),
        }
    }
}

impl std::error::Error for CliError {}

impl From<io::Error> for CliError {
    fn from(e: io::Error) -> Self {
        CliError::Io(e)
    }
}

impl From<crate::frames::text::TextError> for CliError {
    fn from(e: crate::frames::text::TextError) -> Self {
        CliError::Text(e)
    }
}

/// Run one command. Progress and summaries go to standard error.
pub fn run(command: Command) -> Result<(), CliError> {
    match command {
        Command::Record { .. } => Err(CliError::NotImplemented("record")),
        Command::Replay { input, events } => run_replay(&input, events),
        Command::Verify { .. } => Err(CliError::NotImplemented("verify")),
        Command::Bench { .. } => Err(CliError::NotImplemented("bench")),
        Command::Frames { action } => run_frames(action),
        Command::Info { symbol, rest_url } => {
            let info = block_on(rest(&rest_url)?.symbol_info(&symbol))?;
            println!(
                "{} {}: tick size {}, step size {}",
                info.symbol, info.status, info.tick_size, info.step_size
            );
            Ok(())
        }
        Command::Snapshot {
            symbol,
            levels,
            limit,
            rest_url,
        } => {
            print!("{}", snapshot_report(&rest_url, &symbol, limit, levels)?);
            Ok(())
        }
    }
}

fn block_on<T>(
    fut: impl std::future::Future<Output = Result<T, crate::net::rest::RestError>>,
) -> Result<T, CliError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(fut).map_err(CliError::Rest)
}

fn rest(base: &str) -> Result<crate::net::rest::Rest, CliError> {
    crate::net::rest::Rest::new(base, std::time::Duration::from_secs(10)).map_err(CliError::Rest)
}

/// Rules of the symbol, then a snapshot decoded with them: checks the whole read path on
/// live data.
fn snapshot_report(
    rest_url: &str,
    symbol: &str,
    limit: u32,
    levels: usize,
) -> Result<String, CliError> {
    use std::fmt::Write as _;

    use crate::decimal::Grid;
    use crate::json::{JsonDecoder, TimeUnit};
    use crate::replay::decimal;

    let client = rest(rest_url)?;
    let info = block_on(client.symbol_info(symbol))?;
    let started = std::time::Instant::now();
    let fetched = block_on(client.depth(symbol, limit))?;
    let elapsed = started.elapsed();
    let grid = |s: &str| {
        Grid::parse(s.as_bytes()).map_err(|err| {
            CliError::Decode(crate::json::DecodeError::Number {
                field: "filter",
                err,
            })
        })
    };
    let (price, qty) = (grid(&info.tick_size)?, grid(&info.step_size)?);
    let snap = JsonDecoder::new(symbol, price, qty, TimeUnit::Milli)
        .decode_snapshot(&fetched.body)
        .map_err(CliError::Decode)?;
    let mut out = String::new();
    let weight = fetched
        .used_weight_1m
        .map_or("unknown".to_owned(), |w| w.to_string());
    let _ = writeln!(
        out,
        "{} {}: tick size {}, step size {}",
        info.symbol, info.status, info.tick_size, info.step_size
    );
    let _ = writeln!(
        out,
        "snapshot at update {}: {} bids, {} asks, fetched in {} ms, IP weight used this minute {weight}",
        snap.last_update_id,
        snap.bids.len(),
        snap.asks.len(),
        elapsed.as_millis()
    );
    let n = levels.min(snap.asks.len());
    for l in snap.asks[..n].iter().rev() {
        let _ = writeln!(
            out,
            "  ask {:>20}  {:>16}",
            decimal(l.price, price),
            decimal(l.qty, qty)
        );
    }
    for l in snap.bids.iter().take(levels) {
        let _ = writeln!(
            out,
            "  bid {:>20}  {:>16}",
            decimal(l.price, price),
            decimal(l.qty, qty)
        );
    }
    Ok(out)
}

fn run_replay(input: &std::path::Path, events: bool) -> Result<(), CliError> {
    let report = crate::replay::replay_file(input).map_err(CliError::Replay)?;
    let mut out = io::stdout().lock();
    if events {
        for (ts, e) in &report.events {
            writeln!(out, "{ts} {e}")?;
        }
        writeln!(out)?;
    }
    writeln!(out, "{report}")?;
    match report.ending {
        crate::replay::Ending::Corrupt { .. } => Err(CliError::Damaged),
        _ => Ok(()),
    }
}

fn run_frames(action: FramesAction) -> Result<(), CliError> {
    use crate::frames::text;
    match action {
        FramesAction::Dump { input, output } => {
            let src = BufReader::new(File::open(&input)?);
            let n = match output {
                Some(path) => {
                    let mut out = BufWriter::new(File::create_new(&path)?);
                    let n = text::dump(src, &mut out)?;
                    out.flush()?;
                    n
                }
                None => {
                    let mut out = BufWriter::new(io::stdout().lock());
                    let n = text::dump(src, &mut out)?;
                    out.flush()?;
                    n
                }
            };
            eprintln!("{n} records");
        }
        FramesAction::Pack { input, output } => {
            let n = text::pack_file(BufReader::new(File::open(&input)?), &output)?;
            eprintln!("{n} records written to {}", output.display());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn parse(args: &[&str]) -> Command {
        Cli::try_parse_from(args)
            .expect("arguments should parse")
            .command
    }

    #[test]
    fn cli_definition_is_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn record_defaults() {
        assert_eq!(
            parse(&["marketrec", "record", "--symbol", "BTCUSDT"]),
            Command::Record {
                symbol: "BTCUSDT".into(),
                protocol: Protocol::Json,
                out: PathBuf::from("data"),
            }
        );
    }

    #[test]
    fn record_with_both_protocols() {
        let cmd = parse(&[
            "marketrec",
            "record",
            "--symbol",
            "ETHUSDT",
            "--protocol",
            "both",
            "--out",
            "/tmp/d",
        ]);
        assert_eq!(
            cmd,
            Command::Record {
                symbol: "ETHUSDT".into(),
                protocol: Protocol::Both,
                out: PathBuf::from("/tmp/d"),
            }
        );
    }

    #[test]
    fn record_requires_symbol() {
        assert!(Cli::try_parse_from(["marketrec", "record"]).is_err());
    }

    #[test]
    fn unknown_protocol_is_rejected() {
        assert!(
            Cli::try_parse_from([
                "marketrec",
                "record",
                "--symbol",
                "BTCUSDT",
                "--protocol",
                "fix"
            ])
            .is_err()
        );
    }

    #[test]
    fn stubs_say_what_is_missing() {
        let err = run(parse(&["marketrec", "verify", "--day", "x"])).unwrap_err();
        assert_eq!(err.to_string(), "`verify` is not implemented yet");
    }

    #[test]
    fn snapshot_command_reads_rules_then_decodes_the_snapshot_with_them() {
        use crate::net::rest::tests::{EXCHANGE_INFO, mock_http_thread};
        let depth = r#"{"lastUpdateId":77,"bids":[["97123.45000000","0.25000000"],["97123.44000000","0.50000000"]],"asks":[["97123.46000000","0.30000000"],["97123.47000000","0.80000000"]]}"#;
        let base = mock_http_thread(vec![
            ("/api/v3/exchangeInfo", "200 OK", "", EXCHANGE_INFO),
            (
                "/api/v3/depth",
                "200 OK",
                "x-mbx-used-weight-1m: 270\r\n",
                depth,
            ),
        ]);
        let out = snapshot_report(&base, "BTCUSDT", 5000, 1).unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(
            lines[0],
            "BTCUSDT TRADING: tick size 0.01000000, step size 0.00001000"
        );
        assert!(lines[1].starts_with("snapshot at update 77: 2 bids, 2 asks, fetched in "));
        assert!(lines[1].ends_with("IP weight used this minute 270"));
        assert_eq!(lines[2], "  ask       97123.46000000        0.30000000");
        assert_eq!(lines[3], "  bid       97123.45000000        0.25000000");
        assert_eq!(lines.len(), 4);
        // an off-grid price in the snapshot is caught by the decoder
        let bad = mock_http_thread(vec![
            ("/api/v3/exchangeInfo", "200 OK", "", EXCHANGE_INFO),
            (
                "/api/v3/depth",
                "200 OK",
                "",
                r#"{"lastUpdateId":1,"bids":[["97123.455","1"]],"asks":[]}"#,
            ),
        ]);
        assert!(matches!(
            snapshot_report(&bad, "BTCUSDT", 5, 1),
            Err(CliError::Decode(_))
        ));
    }

    #[test]
    fn frames_pack_then_dump() {
        let dir = tempfile::tempdir().unwrap();
        let text = dir.path().join("in.jsonl");
        let bin = dir.path().join("in.frames");
        let back = dir.path().join("back.jsonl");
        let content = concat!(
            r#"{"header":{"symbol":"BTCUSDT","price_step":"0.01000000","qty_step":"0.00001000","time_unit":"milli","recorder":"t"}}"#,
            "\n",
            r#"{"kind":"text","recv_ts":1,"aux":1,"payload_json":{}}"#,
            "\n",
        );
        std::fs::write(&text, content).unwrap();
        let path = |p: &std::path::Path| p.to_str().unwrap().to_owned();
        run(parse(&[
            "marketrec",
            "frames",
            "pack",
            "--input",
            &path(&text),
            "--output",
            &path(&bin),
        ]))
        .unwrap();
        run(parse(&[
            "marketrec",
            "frames",
            "dump",
            "--input",
            &path(&bin),
            "--output",
            &path(&back),
        ]))
        .unwrap();
        assert_eq!(std::fs::read_to_string(&back).unwrap(), content);
        // outputs are never overwritten
        assert!(
            run(parse(&[
                "marketrec",
                "frames",
                "pack",
                "--input",
                &path(&text),
                "--output",
                &path(&bin)
            ]))
            .is_err()
        );
        assert!(
            run(parse(&[
                "marketrec",
                "frames",
                "dump",
                "--input",
                &path(&bin),
                "--output",
                &path(&back)
            ]))
            .is_err()
        );
    }
}
