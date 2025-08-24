use std::process::*;
use anyhow::Result;
use crate::base::{Base, Processor, BaseOptions, Message};
use clap::{Subcommand, Parser};

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
    #[command(flatten)]
    pub opts: BaseOptions,
}

macro_rules! add_subcommands {
    ($($name:ident,)*) => {
        $(
            pub mod $name;
        )*
        pub mod _pipeline;

        #[derive(Subcommand)]
        #[allow(non_camel_case_types)]
        pub enum Command {
            $(
                $name($name::Opts),
            )*
            #[command(name = "!")]
            _pipeline(_pipeline::Opts),
        }

        pub fn run<F: Fn(BaseOptions) -> Result<ExitCode>>(
            subcommand: Option<Command>,
            cli_opts: BaseOptions,
            is_tty: bool,
            default: F,
        ) -> Result<ExitCode> {
            match subcommand {
                $(
                    Some(Command::$name(opts)) => $name::Handler::new(opts)?.run(cli_opts, is_tty),
                )*
                Some(Command::_pipeline(opts)) => _pipeline::Handler::new(opts)?.run(cli_opts, is_tty),
                None => default(cli_opts),
            }
        }

        #[allow(non_camel_case_types)]
        enum Subcommands {
            $(
                $name($name::Handler),
            )*
        }

        impl Subcommands {
            pub fn from_args(args: &[String]) -> Result<(Self, BaseOptions)> {
                match args[0].as_str() {
                    $(
                        stringify!($name) => {

                            #[derive(Parser)]
                            struct Cli {
                                #[command(flatten)]
                                opts: $name::Opts,
                                #[command(flatten)]
                                cli_opts: BaseOptions,
                            }

                            let cli = Cli::parse_from(args);
                            let handler = $name::Handler::new(cli.opts)?;
                            Ok((Self::$name(handler), cli.cli_opts))
                        },
                    )*
                    _ => {
                        let arg0 = std::env::args().next().unwrap_or("dsv".into());
                        Cli::parse_from(std::iter::once(&arg0).chain(args));
                        unreachable!();
                    },
                }
            }

            pub fn forward_messages(self, base: &mut Base, receiver: std::sync::mpsc::Receiver<Message>) -> Result<()> {
                match self {
                    $( Self::$name(handler) => handler.forward_messages(base, receiver), )*
                }
            }

            pub fn process_opts(&mut self, opts: &mut BaseOptions, is_tty: bool) {
                match self {
                    $( Self::$name(handler) => handler.process_opts(opts, is_tty), )*
                }
            }

        }

    };
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
    totsv,
    sqlite,
    page,
    tac,
    pretty,
    sort,
    uniq,
    join,
    tojson,
    fromjson,
    tomarkdown,
    frommarkdown,
    set_header,
    paste,
    fromhtml,
    pipe,
    exec,
    exec_filter,
    exec_groupby,
);
