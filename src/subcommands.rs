use std::process::*;
use anyhow::Result;
use crate::base::{Processor, BaseOptions};
use clap::{Subcommand};

macro_rules! add_subcommands {
    ($($name:ident,)*) => (
        $(
            pub mod $name;
        )*

        #[derive(Subcommand)]
        pub enum Command {
            $(
                #[allow(non_camel_case_types)]
                $name($name::Opts),
            )*
        }

        pub fn run<F: Fn(BaseOptions) -> Result<ExitCode>>(
            subcommand: Option<Command>,
            cli_opts: BaseOptions,
            is_tty: bool,
            default: F,
        ) -> Result<ExitCode> {
            match subcommand {
                $(
                    Some(Command::$name(opts)) => $name::Handler::run(cli_opts, opts, is_tty),
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
    grep,
    replace,
    flip,
);
