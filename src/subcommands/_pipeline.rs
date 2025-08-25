use anyhow::Result;
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
}

impl Handler {
    pub fn new(opts: Opts, base: &mut Base, is_tty: bool) -> Result<Self> {
        let Args::Args(args) = opts.args;

        let (opts_sender, opts_receiver) = mpsc::channel();
        for (i, arg) in args.rsplit(|a| a == "!").enumerate() {
            let new_sender = base.sender.clone();
            let receiver;
            (base.sender, receiver) = mpsc::channel();

            let last = i == 0;
            let opts_sender = opts_sender.clone();
            let arg = arg.to_owned();
            let scope = base.scope;
            scope.spawn(move || {
                let (handler, mut base) = super::Subcommands::from_args(&arg, new_sender, scope, is_tty)?;
                // take opts from the last handler?
                if last {
                    opts_sender.send(base.opts.clone()).unwrap();
                }
                handler.forward_messages(&mut base, receiver)
            });
        }

        base.opts = opts_receiver.recv().unwrap();

        Ok(Self {})
    }
}

impl Processor for Handler {}
