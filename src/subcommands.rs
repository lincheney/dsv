use std::process::*;
use anyhow::Result;
use bstr::BString;
use crate::base::{Base, Processor, BaseOptions, Hook, BaseHook};
use crate::writer::Writer;
use clap::{Subcommand, Parser};

macro_rules! run {
    (tomarkdown, $opts:expr, $($args:expr),*) => {{
        run!(tomarkdown, $opts, tomarkdown::MarkdownWriter; $($args),*)
    }};
    ($name:ident, $opts:expr, $($args:expr),*) => {{
        run!($name, $opts, crate::writer::BaseWriter; $($args),*)
    }};
    ($name:ident, $opts:expr, $writer:path; $($args:expr),*) => {{
        let mut handler = $name::Handler::new($opts);
        <$name::Handler as Processor<BaseHook, $writer>>::run(&mut handler, $($args),*)
    }};
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

        pub fn run<F: Fn(BaseHook, BaseOptions) -> Result<ExitCode>>(
            subcommand: Option<Command>,
            cli_opts: BaseOptions,
            is_tty: bool,
            default: F,
        ) -> Result<ExitCode> {
            let hook = BaseHook{};
            match subcommand {
                $(
                    Some(Command::$name(opts)) => run!($name, opts, hook, cli_opts, is_tty),
                )*
                Some(Command::_pipeline(opts)) => run!(_pipeline, opts, hook, cli_opts, is_tty),
                None => default(BaseHook{}, cli_opts),
            }
        }

        #[allow(non_camel_case_types)]
        enum Subcommands {
            $(
                $name($name::Handler),
            )*
        }

        impl Subcommands {
            pub fn from_args(args: &[String]) -> (Self, BaseOptions) {
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
                            let handler = $name::Handler::new(cli.opts);
                            (Self::$name(handler), cli.cli_opts)
                        },
                    )*
                    _ => todo!(),
                }
            }

            pub fn on_row<H: Hook<W>, W: Writer>(&mut self, base: &mut Base<H, W>, row: Vec<BString>) -> bool {
                match self {
                    $(
                        Self::$name(handler) => handler.on_row(base, row),
                    )*
                }
            }

            pub fn on_header<H: Hook<W>, W: Writer>(&mut self, base: &mut Base<H, W>, header: Vec<BString>) -> bool {
                match self {
                    $(
                        Self::$name(handler) => handler.on_header(base, header),
                    )*
                }
            }

            pub fn on_eof<H: Hook<W>, W: Writer>(&mut self, base: &mut Base<H, W>) {
                match self {
                    $(
                        Self::$name(handler) => handler.on_eof(base),
                    )*
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
);
