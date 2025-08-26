use anyhow::{Result, Context};
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
    err_receiver: mpsc::Receiver<Result<()>>,
}

impl Handler {
    pub fn new(opts: Opts, base: &mut Base, is_tty: bool) -> Result<Self> {
        let Args::Args(args) = opts.args;

        let (err_sender, err_receiver) = mpsc::channel();
        let (opts_sender, opts_receiver) = mpsc::channel();
        for (i, arg) in args.rsplit(|a| a == "!").enumerate() {
            let new_sender = base.sender.clone();
            let receiver;
            (base.sender, receiver) = mpsc::channel();

            let last = i == 0;
            let opts_sender = opts_sender.clone();
            let arg = arg.to_owned();
            let scope = base.scope;
            let err_sender = err_sender.clone();
            scope.spawn(move || {
                let result = (|| {
                    let (handler, mut base) = super::Subcommands::from_args(&arg, new_sender, scope, is_tty)?;
                    // take opts from the last handler?
                    if last {
                        opts_sender.send(base.opts.clone()).unwrap();
                    }
                    handler.forward_messages(&mut base, receiver)
                })();
                err_sender.send(result).unwrap();
            });
        }

        base.opts = opts_receiver.recv().unwrap();

        Ok(Self {
            err_receiver
        })
    }
}

impl Processor for Handler {
    fn on_eof(self, base: &mut Base) -> Result<bool> {
        base.on_eof()?;

        let mut result = Ok(());
        for err in self.err_receiver.iter() {
            if result.is_ok() {
                result = result.and(err);
            } else if let Err(e) = err {
                result = result.context(e);
            }
        }
        result?;
        Ok(false)
    }
}
