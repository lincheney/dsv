mod base;
mod column_slicer;
use std::io::IsTerminal;
use std::process::*;
use anyhow::Result;
use base::Processor;
use clap::{Parser, Subcommand, CommandFactory};

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    #[command(flatten)]
    opts: base::BaseOptions,
}

macro_rules! add_subcommands {
    ($($name:ident,)*) => (
        $(
            mod $name;
        )*

        #[derive(Subcommand)]
        enum Command {
            $(
                #[allow(non_camel_case_types)]
                $name($name::Opts),
            )*
        }

        fn run_subcommand<F: Fn(base::BaseOptions) -> Result<ExitCode>>(
            subcommand: Option<Command>,
            cli_opts: base::BaseOptions,
            default: F,
        ) -> Result<ExitCode> {
            match subcommand {
                $(
                    Some(Command::$name(opts)) => $name::Handler::run(cli_opts, opts),
                )*
                None => default(cli_opts),
            }
        }
    )
}

add_subcommands!(
    head,
    cat,
    tail,
    cut,
    tocsv,
);

fn main() -> Result<ExitCode> {
    let mut cli = Cli::parse();
    cli.opts.post_process();

    run_subcommand(cli.command, cli.opts, |opts| {
        if std::io::stdin().is_terminal() {
            Cli::command().print_help()?;
            Ok(ExitCode::SUCCESS)
        } else {
            // run as if cat
            cat::Handler::run(opts, std::default::Default::default())
        }
    })
}
