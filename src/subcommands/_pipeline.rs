use anyhow::{Result};
use std::sync::mpsc::{self, Receiver, Sender};
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
    err_receiver: Receiver<Result<()>>,
    writer_sender: Sender<Receiver<Message>>,
}

impl Handler {
    pub fn new(opts: Opts, base: &mut Base) -> Result<Self> {
        let Args::Args(args) = opts.args;

        let (writer_sender, writer_receiver) = mpsc::channel();
        let (err_sender, err_receiver) = mpsc::channel();

        let mut handlers = vec![];
        for arg in args.rsplit(|a| a == "!") {
            let new_sender = base.sender.clone();
            let receiver;
            (base.sender, receiver) = mpsc::channel();

            let arg = arg.iter().map(|x| x.as_ref());
            let (handler, base) = super::Subcommands::from_args(arg, new_sender, base.scope)?;
            handlers.push((handler, base, receiver));
        }

        let mut handlers = handlers.into_iter();

        let base_opts;
        // first is actually last in pipeline
        {
            let (handler, mut base, receiver) = handlers.next().unwrap();
            // take opts from the last handler?
            base_opts = base.opts.clone();
            let err_sender = err_sender.clone();
            base.scope.spawn(move || {
                handler.spawn_writer(&mut base, writer_receiver);
                let result = handler.forward_messages(&mut base, receiver);
                err_sender.send(result).unwrap();
            });
        }

        for (handler, mut base, receiver) in handlers {
            let err_sender = err_sender.clone();
            base.scope.spawn(move || {
                let result = handler.forward_messages(&mut base, receiver);
                err_sender.send(result).unwrap();
            });
        }

        base.opts = BaseOptions{
            ifs: base.opts.ifs.clone(),
            tsv: base.opts.tsv,
            csv: base.opts.csv,
            ssv: base.opts.ssv,
            plain_ifs: base.opts.plain_ifs,
            ..base_opts
        };

        Ok(Self {
            err_receiver,
            writer_sender,
        })
    }
}

impl Processor for Handler {

    fn run(self, base: &mut Base, receiver: Receiver<Message>) -> Result<std::process::ExitCode> {
        self.writer_sender.send(receiver).unwrap();
        self.process_file(std::io::stdin().lock(), base, Callbacks::all())
    }

    fn on_eof(self, base: &mut Base) -> Result<bool> {
        base.on_eof()?;
        crate::utils::chain_errors(self.err_receiver)?;
        Ok(false)
    }
}
