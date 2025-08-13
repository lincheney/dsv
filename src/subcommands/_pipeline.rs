use std::sync::mpsc;
use crate::base::*;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(about = "pipe multiple commands together")]
pub struct Opts {
    #[command(subcommand)]
    args: Args,
}

#[derive(Subcommand, Clone)]
enum Args {
    #[command(external_subcommand)]
    Args(Vec<String>),
}

pub struct Handler {
    args: Vec<String>,
    is_tty: bool,
}

impl Handler {
    pub fn new(opts: Opts) -> Self {
        let Args::Args(args) = opts.args;
        Self {
            args,
            is_tty: false,
        }
    }
}

impl Processor for Handler {

    fn process_opts(&mut self, opts: &mut BaseOptions, is_tty: bool) {
        self._process_opts(opts, is_tty);
        self.is_tty = is_tty;
    }

    fn on_start(&mut self, base: &mut Base) -> bool {
        let mut new_base;
        let args = std::mem::take(&mut self.args);
        let mut copied_opts = false;
        for arg in args.rsplit(|a| a == "!") {
            let (sender, receiver) = mpsc::channel();
            let (mut handler, mut cli_opts) = super::Subcommands::from_args(arg);
            handler.process_opts(&mut cli_opts, self.is_tty);
            new_base = Base::new(cli_opts, base.sender.clone(), base.scope);
            base.sender = sender;

            // take opts from the last handler?
            if !copied_opts {
                base.opts = new_base.opts.clone();
                copied_opts = true;
            }

            new_base.scope.spawn(move || {
                handler.forward_messages(&mut new_base, receiver)
            });
        }
        false
    }
}
