mod base;
mod subcommands;
mod column_slicer;
mod writer;
mod python;
use std::io::IsTerminal;
use std::process::ExitCode;
use anyhow::Result;
use clap::{Parser, CommandFactory};
use base::{Processor};
use subcommands::{Cli};

fn main() -> Result<ExitCode> {
    let is_tty = std::io::stdout().is_terminal();
    let cli = Cli::parse();

    subcommands::run(cli.command, cli.opts, is_tty, |base, receiver| {
        if std::io::stdin().is_terminal() {
            Cli::command().print_help()?;
            Ok(ExitCode::SUCCESS)
        } else {
            // run as if cat
            base::DefaultProcessor{}.run(base, receiver)
        }
    })
}
