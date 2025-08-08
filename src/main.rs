mod base;
mod subcommands;
mod column_slicer;
use std::io::IsTerminal;
use std::process::*;
use anyhow::Result;
use clap::{Parser, CommandFactory};
use base::Processor;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<subcommands::Command>,
    #[command(flatten)]
    opts: base::BaseOptions,
}

fn main() -> Result<ExitCode> {
    let mut cli = Cli::parse();
    cli.opts.post_process();

    subcommands::run(cli.command, cli.opts, |opts| {
        if std::io::stdin().is_terminal() {
            Cli::command().print_help()?;
            Ok(ExitCode::SUCCESS)
        } else {
            // run as if cat
            subcommands::cat::Handler::run(opts, std::default::Default::default())
        }
    })
}
