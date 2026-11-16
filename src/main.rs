use std::process::ExitCode;

use clap::Parser;
use marketrec::cli::{Cli, run};

/// Exit codes: 0 success, 1 failure, 2 invalid arguments (reported by clap).
fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli.command) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}
