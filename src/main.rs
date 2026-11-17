use std::process::ExitCode;

use clap::Parser;
use marketrec::cli::{Cli, run};

/// Counts allocations per thread, for `bench` (see `marketrec::alloc_count`).
#[global_allocator]
static ALLOCATOR: marketrec::alloc_count::Counting = marketrec::alloc_count::Counting;

/// Exit codes: 0 success, 1 failure, 2 invalid arguments (reported by clap), 3 not enough
/// free disk space to record.
fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli.command) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::from(err.exit_code())
        }
    }
}
