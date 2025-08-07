mod base;
mod head;
mod cat;
mod tail;
mod cut;
mod tocsv;
mod column_slicer;
use std::io::IsTerminal;
use anyhow::Result;
use base::Processor;
use clap::{Parser, Subcommand, CommandFactory};

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
    #[command(flatten)]
    opts: base::BaseOptions,
}

#[derive(Subcommand)]
enum Commands {
    Head(head::Opts),
    Cat(cat::Opts),
    Tail(tail::Opts),
    Cut(cut::Opts),
    Tocsv(tocsv::Opts),
}

fn main() -> Result<std::process::ExitCode> {
    let mut cli = Cli::parse();
    cli.opts.post_process();

    match cli.command {
        Some(Commands::Head(opts)) => head::Handler::run(cli.opts, opts),
        Some(Commands::Cat(opts)) => cat::Handler::run(cli.opts, opts),
        Some(Commands::Tail(opts)) => tail::Handler::run(cli.opts, opts),
        Some(Commands::Cut(opts)) => cut::Handler::run(cli.opts, opts),
        Some(Commands::Tocsv(opts)) => tocsv::Handler::run(cli.opts, opts),
        None => {
            if std::io::stdin().is_terminal() {
                Cli::command().print_help()?;
            } else {
                // run as if cat
                cat::Handler::run(cli.opts, std::default::Default::default());
            }
        },
    }
    Ok(std::process::ExitCode::SUCCESS)
}
