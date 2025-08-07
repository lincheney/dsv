mod base;
mod head;
mod cat;
mod tail;
use base::Processor;
use clap::{Parser, Subcommand};

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
}

fn main() {
    let mut cli = Cli::parse();
    cli.opts.post_process();

    match cli.command {
        Some(Commands::Head(opts)) => head::Handler::run(cli.opts, opts),
        Some(Commands::Cat(opts)) => cat::Handler::run(cli.opts, opts),
        Some(Commands::Tail(opts)) => tail::Handler::run(cli.opts, opts),
        _ => todo!(),
    }
}
