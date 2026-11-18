//! Command line interface. Every command is declared here; implementations arrive stage by stage.

use std::fmt;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Write};
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

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
    /// Record one symbol from Binance into the data directory until Ctrl+C.
    Record(RecordArgs),
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
    /// Build the Parquet tables from frames files, as the recorder does live. Use it for
    /// recordings made before a schema change, or to check the live tables.
    Convert {
        /// A frames file, or a directory searched for `*.frames` and `*.jsonl` files.
        #[arg(long)]
        input: PathBuf,
        /// Root of a data directory without tables for this symbol yet.
        #[arg(long)]
        out: PathBuf,
    },
    /// Check a recorded day: manifest and files, continuity of the update stream, and a
    /// rebuild of every table from the frames. Exits with 1 if a check fails.
    Verify {
        /// Directory of one recorded day, for example data/binance/BTCUSDT/2026-11-21.
        #[arg(long)]
        day: PathBuf,
        /// Skip the rebuild from frames.
        #[arg(long)]
        quick: bool,
        /// Directory for the rebuilt tables (removed afterwards); the system's temporary
        /// directory if omitted. The rebuild needs about as much space as two days of tables.
        #[arg(long)]
        scratch: Option<PathBuf>,
    },
    /// Derive a recorded day's tables again from its frames, in place, then verify it: for
    /// a day a crash left incomplete or that older rules derived. Frames are not touched.
    Rebuild {
        /// Directory of one recorded day.
        #[arg(long)]
        day: PathBuf,
        /// Allow today's date (UTC); only when no recorder writes to it.
        #[arg(long)]
        today: bool,
    },
    /// Measure the decode path on a recorded day: the JSON decoder, the whole pipeline with
    /// each book, and the book updates alone. Use a release build.
    Bench {
        /// Directory of one recorded day; its frames/ are read.
        #[arg(long)]
        day: PathBuf,
        /// Frames to load at most, from the start of the day.
        #[arg(long, default_value_t = 2_000_000)]
        max_frames: usize,
        /// Runs of each measurement; the best counts.
        #[arg(long, default_value_t = 3)]
        repeat: usize,
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
    /// Connect to the live stream for a while and count what arrives. Records nothing.
    Listen {
        #[arg(long)]
        symbol: String,
        #[arg(long, default_value_t = 10)]
        seconds: u64,
        #[arg(long, default_value = crate::net::ws::DEFAULT_BASE)]
        ws_url: String,
    },
    /// Bring in a day recorded by someone else, as frames, and derive its tables.
    Import {
        #[command(subcommand)]
        source: ImportSource,
    },
    /// Convert frames files between the binary form and readable JSON Lines.
    Frames {
        #[command(subcommand)]
        action: FramesAction,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum ImportSource {
    /// A day of Tardis.dev's raw Binance Spot recording: depth updates, best bid and ask,
    /// trades and its depth snapshots. The first day of each month needs no key; for other
    /// days the key is read from the environment variable TARDIS_API_KEY.
    Tardis {
        /// UTC date, for example 2026-11-01.
        #[arg(long)]
        date: String,
        #[arg(long, default_value = "BTCUSDT")]
        symbol: String,
        /// Root of the data directory.
        #[arg(long, default_value = "data")]
        out: PathBuf,
        /// Tick size, for example 0.01000000; with --qty-step. Taken from Binance's current
        /// rules if both are left out.
        #[arg(long, requires = "qty_step")]
        price_step: Option<String>,
        /// Step size, for example 0.00001000; with --price-step.
        #[arg(long, requires = "price_step")]
        qty_step: Option<String>,
        #[arg(long, default_value = crate::tardis::DEFAULT_BASE)]
        tardis_url: String,
        #[arg(long, default_value = crate::net::rest::DEFAULT_BASE)]
        rest_url: String,
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
    /// Compress finished frames files to `.frames.zst`, as the recorder does: each copy is
    /// read back and compared before the original is removed.
    Compress {
        /// A frames file, or a directory searched for `*.frames` files.
        #[arg(long)]
        input: PathBuf,
    },
}

#[derive(Debug, Args, PartialEq, Eq)]
pub struct RecordArgs {
    /// Symbol as Binance names it, for example BTCUSDT.
    #[arg(long)]
    pub symbol: String,
    /// Which feed to record.
    #[arg(long, value_enum, default_value_t = Protocol::Json)]
    pub protocol: Protocol,
    /// Root of the data directory.
    #[arg(long, default_value = "data")]
    pub out: PathBuf,
    /// Stop after this many seconds instead of waiting for Ctrl+C.
    #[arg(long)]
    pub seconds: Option<u64>,
    /// Levels per side in each REST snapshot (weight 250 for 1001-5000, 50 for 501-1000).
    #[arg(long, default_value_t = 5000)]
    pub depth_limit: u32,
    /// Seconds between status lines.
    #[arg(long, default_value_t = 10)]
    pub status_every: u64,
    #[arg(long, default_value = crate::net::rest::DEFAULT_BASE)]
    pub rest_url: String,
    #[arg(long, default_value = crate::net::ws::DEFAULT_BASE)]
    pub ws_url: String,
    /// Keep finished hourly frames files uncompressed.
    #[arg(long)]
    pub no_compress: bool,
    /// Refuse to start, and stop cleanly, below this many GB free on the data disk. Exits
    /// with 3 in that case, which a service manager can treat as "do not restart".
    #[arg(long, default_value_t = 2)]
    pub min_free_gb: u64,
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
    Record(crate::record::RecordError),
    Decode(crate::json::DecodeError),
    Convert(crate::tables::convert::ConvertError),
    Bench(crate::bench::BenchError),
    Rebuild(crate::rebuild::RebuildError),
    Import(crate::tardis::ImportError),
    /// The input contains a damaged record; everything before it was processed.
    Damaged,
    /// `verify` found a failing check; the report says which.
    VerifyFailed,
    /// Not enough free space on the data disk to record. Exit code 3.
    LowDisk(String),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::NotImplemented(cmd) => write!(f, "`{cmd}` is not implemented yet"),
            CliError::Io(e) => write!(f, "{e}"),
            CliError::Text(e) => write!(f, "{e}"),
            CliError::Replay(e) => write!(f, "{e}"),
            CliError::Rest(e) => write!(f, "{e}"),
            CliError::Record(e) => write!(f, "{e}"),
            CliError::Decode(e) => write!(f, "{e}"),
            CliError::Convert(e) => write!(f, "{e}"),
            CliError::Bench(e) => write!(f, "{e}"),
            CliError::Rebuild(e) => write!(f, "{e}"),
            CliError::Import(e) => write!(f, "{e}"),
            CliError::Damaged => write!(f, "the input contains a damaged record"),
            CliError::VerifyFailed => write!(f, "verification failed"),
            CliError::LowDisk(m) => write!(f, "{m}"),
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
        Command::Record(args) => run_record(args),
        Command::Replay { input, events } => run_replay(&input, events),
        Command::Convert { input, out } => {
            let summary =
                crate::tables::convert::convert(&input, &out, &crate::record::recorder_id())
                    .map_err(CliError::Convert)?;
            println!("{summary}");
            if summary.damaged().next().is_some() {
                return Err(CliError::Damaged);
            }
            Ok(())
        }
        Command::Verify {
            day,
            quick,
            scratch,
        } => {
            let report =
                crate::verify::verify_day(&day, &crate::verify::VerifyOptions { quick, scratch });
            println!("{report}");
            if report.outcome() == crate::verify::Outcome::Fail {
                return Err(CliError::VerifyFailed);
            }
            Ok(())
        }
        Command::Rebuild { day, today } => {
            let r = crate::rebuild::rebuild_day(&day, today).map_err(CliError::Rebuild)?;
            println!("{r}");
            if r.verify.outcome() == crate::verify::Outcome::Fail {
                return Err(CliError::VerifyFailed);
            }
            Ok(())
        }
        Command::Bench {
            day,
            max_frames,
            repeat,
        } => {
            let opts = crate::bench::BenchOptions { max_frames, repeat };
            let report = crate::bench::bench_day(&day, &opts).map_err(CliError::Bench)?;
            println!("{report}");
            Ok(())
        }
        Command::Frames { action } => run_frames(action),
        Command::Import {
            source:
                ImportSource::Tardis {
                    date,
                    symbol,
                    out,
                    price_step,
                    qty_step,
                    tardis_url,
                    rest_url,
                },
        } => {
            let steps = match (price_step, qty_step) {
                (Some(p), Some(q)) => (p, q),
                _ => {
                    let info = block_on(rest(&rest_url)?.symbol_info(&symbol))?;
                    eprintln!(
                        "steps         {} and {}, Binance's current rules for {symbol}",
                        info.tick_size, info.step_size
                    );
                    (info.tick_size, info.step_size)
                }
            };
            let key = std::env::var("TARDIS_API_KEY").ok();
            let r = crate::tardis::tardis_day(
                &tardis_url,
                key,
                crate::tardis::Retry::default(),
                &date,
                &symbol,
                &out,
                steps,
                false,
            )
            .map_err(CliError::Import)?;
            println!("{r}");
            if r.rebuilt.verify.outcome() == crate::verify::Outcome::Fail {
                return Err(CliError::VerifyFailed);
            }
            Ok(())
        }
        Command::Info { symbol, rest_url } => {
            let info = block_on(rest(&rest_url)?.symbol_info(&symbol))?;
            println!(
                "{} {}: tick size {}, step size {}",
                info.symbol, info.status, info.tick_size, info.step_size
            );
            Ok(())
        }
        Command::Listen {
            symbol,
            seconds,
            ws_url,
        } => {
            let url = crate::net::ws::stream_url(&ws_url, &symbol, crate::json::TimeUnit::Micro);
            let cfg = crate::net::ws::WsConfig::new(url);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let summary = rt.block_on(crate::net::listen::listen(
                cfg,
                std::time::Duration::from_secs(seconds),
            ));
            println!("{summary}");
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

fn run_record(args: RecordArgs) -> Result<(), CliError> {
    use crate::record::{Progress, RecordConfig, record};
    use crate::time::Utc;

    if args.protocol != Protocol::Json {
        return Err(CliError::NotImplemented("record --protocol sbe|both"));
    }
    let mut cfg = RecordConfig::new(&args.symbol, args.out);
    cfg.duration = args.seconds.map(std::time::Duration::from_secs);
    cfg.fetch.limit = args.depth_limit;
    cfg.status_every = std::time::Duration::from_secs(args.status_every.max(1));
    cfg.rest_base = args.rest_url;
    cfg.ws_base = args.ws_url;
    if args.no_compress {
        cfg.compress_level = None;
    }
    cfg.min_free_bytes = args.min_free_gb.saturating_mul(1_000_000_000);
    eprintln!("recording {}, stop with Ctrl+C", cfg.symbol);
    let summary = record(cfg, |p| match p {
        Progress::Status(s) => eprintln!("{s}"),
        Progress::Quality(ts, e) => {
            let t = Utc::from_ns(ts);
            eprintln!(
                "{} {}.{:03} {e}",
                t.date(),
                t.time().replace('-', ":"),
                ts.rem_euclid(1_000_000_000) / 1_000_000
            );
        }
        Progress::Fatal(m) => eprintln!("FATAL: {m}"),
        Progress::Stopping(m) => eprintln!("STOPPING: {m}"),
    })
    .map_err(|e| match e {
        e @ crate::record::RecordError::LowDisk { .. } => CliError::LowDisk(e.to_string()),
        e => CliError::Record(e),
    })?;
    print!("{summary}");
    if let Some(first) = summary.files.first() {
        println!(
            "check it with: marketrec replay --input {}",
            first.display()
        );
    }
    match summary.low_disk {
        Some(why) => Err(CliError::LowDisk(why)),
        None => Ok(()),
    }
}

impl CliError {
    /// The process exit code: 3 for a full disk, which a service manager should not answer
    /// with a restart; 1 for everything else.
    pub fn exit_code(&self) -> u8 {
        match self {
            CliError::LowDisk(_) => 3,
            _ => 1,
        }
    }
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
            let src = crate::frames::zst::open(&input)?;
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
        FramesAction::Compress { input } => {
            let files: Vec<PathBuf> = crate::tables::convert::input_files(&input)?
                .into_iter()
                .filter(|p| p.extension().is_some_and(|e| e == "frames"))
                .collect();
            let (mut raw, mut zst, mut failed) = (0u64, 0u64, 0usize);
            for f in &files {
                match crate::frames::zst::compress_file(f, crate::frames::zst::LEVEL) {
                    Ok(c) => {
                        raw += c.raw_bytes;
                        zst += c.compressed_bytes;
                        println!("{}", c.path.display());
                    }
                    Err(e) => {
                        failed += 1;
                        eprintln!("{}: {e}", f.display());
                    }
                }
            }
            eprintln!(
                "compressed {} of {} files: {:.1} MB to {:.1} MB",
                files.len() - failed,
                files.len(),
                raw as f64 / 1e6,
                zst as f64 / 1e6
            );
            if failed > 0 {
                return Err(CliError::Io(io::Error::other(format!(
                    "{failed} files not compressed"
                ))));
            }
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
        let Command::Record(a) = parse(&["marketrec", "record", "--symbol", "BTCUSDT"]) else {
            panic!("not record")
        };
        assert_eq!(
            (a.symbol.as_str(), a.protocol, a.out.as_path()),
            ("BTCUSDT", Protocol::Json, std::path::Path::new("data"))
        );
        assert_eq!((a.seconds, a.depth_limit, a.status_every), (None, 5000, 10));
        assert_eq!(a.rest_url, crate::net::rest::DEFAULT_BASE);
        assert_eq!(a.ws_url, crate::net::ws::DEFAULT_BASE);
    }

    #[test]
    fn record_with_options() {
        let cmd = parse(&[
            "marketrec",
            "record",
            "--symbol",
            "ETHUSDT",
            "--protocol",
            "both",
            "--out",
            "/tmp/d",
            "--seconds",
            "60",
            "--depth-limit",
            "1000",
        ]);
        let Command::Record(a) = cmd else {
            panic!("not record")
        };
        assert_eq!(
            (a.protocol, a.seconds, a.depth_limit),
            (Protocol::Both, Some(60), 1000)
        );
        assert_eq!(a.out, PathBuf::from("/tmp/d"));
        assert!(
            matches!(run(Command::Record(a)), Err(CliError::NotImplemented(_))),
            "SBE comes later"
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
    fn frames_compress_compresses_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/gap.jsonl");
        let bin = dir.path().join("00-00-00.frames");
        run(Command::Frames {
            action: FramesAction::Pack {
                input: src,
                output: bin.clone(),
            },
        })
        .unwrap();
        let before = crate::replay::replay_file(&bin).unwrap();
        run(parse(&[
            "marketrec",
            "frames",
            "compress",
            "--input",
            dir.path().to_str().unwrap(),
        ]))
        .unwrap();
        assert!(!bin.exists());
        let after = crate::replay::replay_file(&dir.path().join("00-00-00.frames.zst")).unwrap();
        assert_eq!(after.stats, before.stats);
        assert_eq!(after.events, before.events);
    }

    #[test]
    fn convert_takes_input_and_output() {
        assert_eq!(
            parse(&["marketrec", "convert", "--input", "a", "--out", "b"]),
            Command::Convert {
                input: "a".into(),
                out: "b".into()
            }
        );
        assert!(Cli::try_parse_from(["marketrec", "convert", "--input", "a"]).is_err());
    }

    #[test]
    fn bench_defaults_and_a_missing_day() {
        let cmd = parse(&["marketrec", "bench", "--day", "/nonexistent/2026-11-21"]);
        assert!(matches!(
            cmd,
            Command::Bench {
                max_frames: 2_000_000,
                repeat: 3,
                ..
            }
        ));
        let err = run(cmd).unwrap_err();
        assert!(err.to_string().contains("no frames files"), "{err}");
    }

    #[test]
    fn verify_of_a_missing_day_fails() {
        let cmd = parse(&[
            "marketrec",
            "verify",
            "--day",
            "/nonexistent/2026-11-21",
            "--quick",
        ]);
        assert!(matches!(run(cmd), Err(CliError::VerifyFailed)));
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
