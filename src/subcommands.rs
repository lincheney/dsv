use std::process::*;
use std::sync::mpsc::{self, Sender, Receiver};
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

        pub fn run<F: Fn(&mut Base, Receiver<Message>) -> Result<ExitCode>>(
            subcommand: Option<Command>,
            mut cli_opts: BaseOptions,
            is_tty: bool,
            default: F,
        ) -> Result<ExitCode> {
            std::thread::scope(|scope| {
                let (sender, receiver) = mpsc::channel();
                cli_opts.post_process(is_tty);
                let mut base = Base::new(cli_opts.clone(), sender, scope);
                match subcommand {
                    $(
                        Some(Command::$name(opts)) => $name::Handler::new(opts, &mut base, is_tty)?.run(&mut base, receiver),
                    )*
                    Some(Command::_pipeline(opts)) => _pipeline::Handler::new(opts, &mut base, is_tty)?.run(&mut base, receiver),
                    None => default(&mut base, receiver),
                }
            })
        }

        #[allow(non_camel_case_types)]
        enum Subcommands {
            $(
                $name($name::Handler),
            )*
        }

        impl Subcommands {
            pub fn from_args<'a, 'b>(
                args: &[String],
                sender: Sender<Message>,
                scope: &'a std::thread::Scope<'a, 'b>,
                is_tty: bool,
        ) -> Result<(Self, Base<'a, 'b>)> {

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

                            let mut cli = Cli::parse_from(args);
                            cli.cli_opts.post_process(is_tty);
                            let mut base = Base::new(cli.cli_opts, sender, scope);
                            let handler = $name::Handler::new(cli.opts, &mut base, is_tty)?;
                            Ok((Self::$name(handler), base))
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
    reshape_long,
    reshape_wide,
    summary,
);
