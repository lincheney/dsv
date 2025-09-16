mod base;
mod subcommands;
mod column_slicer;
mod writer;
mod python;
mod utils;
use std::io::IsTerminal;
use std::process::ExitCode;
use anyhow::Result;
use clap::{Parser, CommandFactory};
use base::{Processor};
use subcommands::{Cli};
use std::sync::Mutex;

pub static CONTROL_C_HANDLERS: Mutex<Vec<fn()>> = Mutex::new(vec![]);

fn run_cleanup() {
    for func in CONTROL_C_HANDLERS.lock().unwrap().iter() {
        func();
    }
}

fn main() -> Result<ExitCode> {
    let cli = Cli::parse();

    ctrlc::set_handler(|| {
        run_cleanup();
        std::process::exit(130);
    })?;

    let result = subcommands::run(cli.command, cli.opts, |base, receiver| {
        if std::io::stdin().is_terminal() {
            Cli::command().print_help()?;
            Ok(ExitCode::SUCCESS)
        } else {
            // run as if cat
            base::DefaultProcessor{}.run(base, receiver)
        }
    });
    run_cleanup();
    result
}
