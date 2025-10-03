use std::ffi::{OsString};
use crate::utils::Break;
use std::process::ExitCode;
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

pub struct Handler {
    err_receiver: Option<Receiver<Result<ExitCode>>>,
    flags: Vec<OsString>,
    args: Vec<String>,
}

impl Handler {
    pub fn new(opts: Opts, _base: &mut Base, mut cli_args: Vec<OsString>) -> Result<Self> {
        let Args::Args(args) = opts.args;

        drop(cli_args.drain(cli_args.len() - args.len() .. ));
        cli_args.pop().unwrap();
        cli_args.remove(0);

        Ok(Self {
            err_receiver: None,
            flags: cli_args,
            args,
        })
    }
}

impl Processor for Handler {

    fn run(mut self, base: &mut Base, receiver: Receiver<Message>) -> Result<ExitCode> {
        let (err_sender, err_receiver) = mpsc::channel();
        self.err_receiver = Some(err_receiver);

        let mut handlers = vec![];
        for (i, arg) in self.args.rsplit(|a| a == "!").enumerate() {
            let new_sender = base.sender.clone();
            let receiver;
            (base.sender, receiver) = mpsc::channel();

            let arg = self.flags.iter().map(|f| f.as_ref()).chain(arg.iter().map(|x| x.as_ref()));
            let sub = super::Subcommands::from_args(
                arg,
                new_sender,
                base.scope,
                base.opts.is_stdout_tty && i == 0,
            );
            if let Ok((handler, base)) = sub {
                handler.register_cleanup();
                handlers.push((handler, base, receiver));
            } else if Break::is_break(sub)? {
                return Ok(ExitCode::SUCCESS)
            }
        }

        // first is actually last in pipeline
        let first = handlers.pop().unwrap();
        let mut handlers = handlers.into_iter();

        if let Some(last) = handlers.next() {

            let (handler, mut base, recv) = last;
            // last gets to create the writer
            handler.spawn_writer(&mut base, receiver);
            // take opts from the last handler?
            {
                let err_sender = err_sender.clone();
                base.scope.spawn(move || {
                    let result = handler.forward_messages(&mut base, recv);
                    err_sender.send(result).unwrap();
                });
            }

            // spawn all the other handlers
            for (handler, mut base, recv) in handlers {
                let err_sender = err_sender.clone();
                base.scope.spawn(move || {
                    let result = handler.forward_messages(&mut base, recv);
                    err_sender.send(result).unwrap();
                });
            }

            // first handler gets to read from stdin
            let (handler, mut base, _) = first;
            let result = handler.process_file(std::io::stdin().lock(), &mut base, Callbacks::all());
            err_sender.send(result).unwrap();

        } else {
            // exactly 1 handler, just run it
            let (handler, mut base, _) = first;
            let result = handler.run(&mut base, receiver);
            err_sender.send(result).unwrap();
        }
        drop(err_sender);

        self.on_eof_detailed(base)
    }

    fn on_eof(self, base: &mut Base) -> Result<bool> {
        let mut success = base.on_eof()?;
        crate::utils::chain_errors(
            self.err_receiver.unwrap().iter()
                .inspect(|code|
                    if let Ok(code) = code && *code != ExitCode::SUCCESS {
                        success = false;
                    }
                )
        )?;
        Ok(success)
    }
}
