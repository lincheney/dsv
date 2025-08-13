mod base;
mod subcommands;
mod column_slicer;
mod writer;
use std::io::IsTerminal;
use std::process::*;
use anyhow::Result;
use clap::{Parser, CommandFactory};
use base::{Processor};
use subcommands::{cat, Cli};

fn main() -> Result<ExitCode> {
    let is_tty = std::io::stdout().is_terminal();
    let cli = Cli::parse();

    subcommands::run(cli.command, cli.opts, is_tty, |opts| {
        if std::io::stdin().is_terminal() {
            Cli::command().print_help()?;
            Ok(ExitCode::SUCCESS)
        } else {
            // run as if cat
            cat::Handler::new(std::default::Default::default()).run(opts, is_tty)
        }
    })
}
