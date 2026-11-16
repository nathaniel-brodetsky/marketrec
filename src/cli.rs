//! Command line interface. Every command is declared here; implementations arrive stage by stage.

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
    /// Feed recorded frames through the same hot path as the live feed, without network.
    Replay {
        /// File with recorded frames.
        #[arg(long)]
        input: PathBuf,
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

/// Error returned by a command that is declared but not built yet.
#[derive(Debug, PartialEq, Eq)]
pub struct NotImplemented(pub &'static str);

impl std::fmt::Display for NotImplemented {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "`{}` is not implemented yet", self.0)
    }
}

impl std::error::Error for NotImplemented {}

/// Run one command.
pub fn run(command: Command) -> Result<(), NotImplemented> {
    match command {
        Command::Record { .. } => Err(NotImplemented("record")),
        Command::Replay { .. } => Err(NotImplemented("replay")),
        Command::Verify { .. } => Err(NotImplemented("verify")),
        Command::Bench { .. } => Err(NotImplemented("bench")),
    }
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
        let err = run(parse(&["marketrec", "replay", "--input", "x.frames"])).unwrap_err();
        assert_eq!(err.to_string(), "`replay` is not implemented yet");
    }
}
