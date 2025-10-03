use std::ffi::{OsStr};
use std::process::ExitCode;
use std::sync::mpsc::{self, Sender, Receiver};
use anyhow::Result;
use crate::base::{Base, Processor, BaseOptions, Message, Callbacks};
use clap::{Subcommand, Parser, CommandFactory};
use std::io::{BufRead};

#[derive(Parser)]
#[command(version, about, long_about = None, disable_help_subcommand = true)]
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
            default: F,
        ) -> Result<ExitCode> {
            std::thread::scope(|scope| {
                let (sender, receiver) = mpsc::channel();
                cli_opts.post_process(None);
                let mut base = Base::new(cli_opts.clone(), sender, scope);
                match subcommand {
                    $(
                        Some(Command::$name(opts)) => $name::Handler::new(opts, &mut base)?.run(&mut base, receiver),
                    )*
                    Some(Command::_pipeline(opts)) => {
                        let args = std::env::args_os().collect();
                        _pipeline::Handler::new(opts, &mut base, args)?.run(&mut base, receiver)
                    },
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
            pub fn from_args<'a, 'b, 'c, I: Iterator<Item=&'c OsStr>>(
                args: I,
                sender: Sender<Message>,
                scope: &'a std::thread::Scope<'a, 'b>,
                is_stdout_tty: bool,
            ) -> Result<(Self, Base<'a, 'b>)> {

                const ARG0: &str = env!("CARGO_PKG_NAME");
                let mut cli = Cli::parse_from(std::iter::once(ARG0.as_ref()).chain(args));
                cli.opts.post_process(Some(is_stdout_tty));
                let mut base = Base::new(cli.opts, sender, scope);
                let handler = match cli.command {
                    $(
                        Some(Command::$name(opts)) => Self::$name($name::Handler::new(opts, &mut base)?),
                    )*
                    Some(Command::_pipeline(_)) | None => {
                        Cli::command().print_help()?;
                        return crate::utils::Break.to_err()
                    },
                };

                Ok((handler, base))
            }

            pub fn register_cleanup(&self) {
                match self {
                    $( Self::$name(handler) => handler.register_cleanup(), )*
                }
            }

            pub fn forward_messages(self, base: &mut Base, receiver: Receiver<Message>) -> Result<ExitCode> {
                match self {
                    $( Self::$name(handler) => handler.forward_messages(base, receiver), )*
                }
            }

            pub fn process_file<R: BufRead>(self, file: R, base: &mut Base, do_callbacks: Callbacks) -> Result<ExitCode> {
                match self {
                    $( Self::$name(handler) => handler.process_file(file, base, do_callbacks), )*
                }
            }

            pub fn run(self, base: &mut Base, receiver: Receiver<Message>) -> Result<ExitCode> {
                match self {
                    $( Self::$name(handler) => handler.run(base, receiver), )*
                }
            }

            pub fn spawn_writer(&self, base: &mut Base, receiver: Receiver<Message>) {
                match self {
                    $(
                    Self::$name(handler) => {
                        let mut writer = handler.make_writer(base.opts.clone());
                        base.scope.spawn(move || {
                            writer.run(receiver)
                        });
                    },
                    )*
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
