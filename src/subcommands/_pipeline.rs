use anyhow::{Result};
use std::sync::mpsc::{self, Receiver};
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

type WriterFn = Box<dyn Send + FnOnce(Receiver<Message>) -> Result<()>>;

struct Payload {
    writer: WriterFn,
    opts: BaseOptions,
}

pub struct Handler {
    err_receiver: Receiver<Result<()>>,
    writer: Option<WriterFn>,
}

impl Handler {
    pub fn new(opts: Opts, base: &mut Base, is_tty: bool) -> Result<Self> {
        let Args::Args(args) = opts.args;

        let (payload_sender, payload_receiver) = mpsc::channel();
        let (err_sender, err_receiver) = mpsc::channel();
        for (i, arg) in args.rsplit(|a| a == "!").enumerate() {
            let new_sender = base.sender.clone();
            let receiver;
            (base.sender, receiver) = mpsc::channel();

            let arg = arg.to_owned();
            let scope = base.scope;
            let err_sender = err_sender.clone();

            if i == 0 {
                // last
                let payload_sender = payload_sender.clone();
                scope.spawn(move || {
                    let result = (|| {
                        let args = arg.iter().map(|x| x.as_ref());
                        let (handler, mut base, writer) = super::Subcommands::from_args(args, new_sender, scope, is_tty, true)?;
                        // take opts from the last handler?
                        payload_sender.send(Payload {
                            writer: writer.unwrap(),
                            opts: base.opts.clone(),
                        }).unwrap();
                        handler.forward_messages(&mut base, receiver)
                    })();
                    err_sender.send(result).unwrap();
                });
            } else {
                scope.spawn(move || {
                    let result = (|| {
                        let args = arg.iter().map(|x| x.as_ref());
                        let (handler, mut base, _) = super::Subcommands::from_args(args, new_sender, scope, false, false)?;
                        handler.forward_messages(&mut base, receiver)
                    })();
                    err_sender.send(result).unwrap();
                });
            }
        }

        let payload = payload_receiver.recv().unwrap();
        base.opts = BaseOptions{
            ifs: base.opts.ifs.clone(),
            tsv: base.opts.tsv,
            csv: base.opts.csv,
            ssv: base.opts.ssv,
            plain_ifs: base.opts.plain_ifs,
            ..payload.opts
        };

        Ok(Self {
            err_receiver,
            writer: Some(payload.writer),
        })
    }
}

impl Processor for Handler {

    fn run(mut self, base: &mut Base, receiver: Receiver<Message>) -> Result<std::process::ExitCode> {
        let writer = self.writer.take().unwrap();
        base.scope.spawn(move || {
            writer(receiver)
        });
        self.process_file(std::io::stdin().lock(), base, Callbacks::all())
    }

    fn on_eof(self, base: &mut Base) -> Result<bool> {
        base.on_eof()?;
        crate::utils::chain_errors(self.err_receiver)?;
        Ok(false)
    }
}
