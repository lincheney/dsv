mod base;
mod head;
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
}

fn main() {
    let mut cli = Cli::parse();
    cli.opts.post_process();

    let mut handler = match cli.command {
        Some(Commands::Head(opts)) => head::Handler::new(opts),
        _ => todo!(),
    };
    handler.process_opts(&mut cli.opts);
    handler.process_file(std::io::stdin(), cli.opts, true);
}
