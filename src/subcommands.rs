use std::process::ExitCode;
use std::sync::mpsc::{self, Sender, Receiver};
use anyhow::Result;
use crate::base::{Base, Processor, BaseOptions, Message};
use clap::{Subcommand, Parser, CommandFactory};

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
            pub fn from_args<'a, 'b, 'c, I: Iterator<Item=&'c str>>(
                args: I,
                sender: Sender<Message>,
                scope: &'a std::thread::Scope<'a, 'b>,
                is_tty: bool,
                writer: bool,
        ) -> Result<(Self, Base<'a, 'b>, Option<Box<dyn Send + FnOnce(Receiver<Message>) -> Result<()>>>)> {

                const ARG0: &str = env!("CARGO_PKG_NAME");
                let mut cli = Cli::parse_from(std::iter::once(ARG0).chain(args));
                cli.opts.post_process(is_tty);
                let mut base = Base::new(cli.opts, sender, scope);
                let (handler, writer) = match cli.command {
                    $(
                        Some(Command::$name(opts)) => {
                            let h = Self::$name($name::Handler::new(opts, &mut base, is_tty)?);
                            let w = if writer {
                                let cli_opts = base.opts.clone();
                                Some(Box::new(|r| $name::Handler::make_writer(cli_opts).run(r)) as _)
                            } else {
                                None
                            };
                            (h, w)
                        },
                    )*
                    Some(Command::_pipeline(_)) | None => {
                        Cli::command().print_help()?;
                        unreachable!();
                    },
                };

                Ok((handler, base, writer))
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
    cat,
    cut,
    flip,
    fromhtml,
    fromjson,
    frommarkdown,
    grep,
    head,
    join,
    page,
    paste,
    pipe,
    pretty,
    py,
    py_filter,
    py_groupby,
    replace,
    reshape_long,
    reshape_wide,
    set_header,
    sort,
    sqlite,
    summary,
    tac,
    tail,
    tocsv,
    tojson,
    tomarkdown,
    totsv,
    uniq,
    xargs,
);
