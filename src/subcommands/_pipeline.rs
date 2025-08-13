use std::sync::mpsc::{self, Sender};
use anyhow::Result;
use bstr::BString;
use crate::base::*;
use crate::writer::{Writer, BaseWriter};
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

enum Message {
    Row(Vec<BString>),
    Header(Vec<BString>),
    Eof,
}

pub struct Handler {
    args: Vec<String>,
    sender: Option<Sender<Message>>
}

impl Handler {
    pub fn new(opts: Opts) -> Self {
        let args = match opts.args {
            Args::Args(args) => args,
        };
        Self {
            args,
            sender: None,
        }
    }
}

impl<H: Hook<W>, W: crate::writer::Writer> Processor<H, W> for Handler {
    fn process_file<R: std::io::BufRead>(
        &mut self,
        file: R,
        base: &mut Base<H, W>,
        do_callbacks: Callbacks,
    ) -> Result<std::process::ExitCode> {

        let args = std::mem::take(&mut self.args);
        std::thread::scope(|scope| {
            let mut prev_sender = None;

            for arg in args.rsplit(|a| a == "!") {
                let (sender, receiver) = mpsc::channel();

                scope.spawn(move || {
                    let (mut handler, cli_opts) = super::Subcommands::from_args(arg);
                    let hooks = PipeHook {
                        sender: prev_sender,
                        inner: crate::base::BaseHook{},
                    };
                    let mut base: Base<PipeHook, BaseWriter> = Base::new(cli_opts, hooks);
                    for msg in receiver.iter() {
                        match msg {
                            Message::Row(row) => if handler.on_row(&mut base, row) { break },
                            Message::Header(header) => if handler.on_header(&mut base, header) { break },
                            Message::Eof => { handler.on_eof(&mut base); },
                        };
                    }
                });

                prev_sender = Some(sender);
            }

            self.sender = prev_sender;
            let result = self._process_file(file, base, do_callbacks);
            self.sender.take();
            result
        })
    }

    fn on_header(&mut self, base: &mut Base<H, W>, header: Vec<BString>) -> bool {
        if let Some(sender) = &mut self.sender {
            sender.send(Message::Header(header)).unwrap();
            false
        } else {
            base.on_header(header)
        }
    }

    fn on_row(&mut self, base: &mut Base<H, W>, row: Vec<BString>) -> bool {
        if let Some(sender) = &mut self.sender {
            sender.send(Message::Row(row)).unwrap();
            false
        } else {
            base.on_row(row)
        }
    }

    fn on_eof(&mut self, base: &mut Base<H, W>) {
        if let Some(sender) = &mut self.sender {
            sender.send(Message::Eof).unwrap();
        } else {
            base.on_eof()
        }
    }
}

struct PipeHook {
    sender: Option<Sender<Message>>,
    inner: crate::base::BaseHook,
}

impl<W: Writer> Hook<W> for PipeHook {
    fn on_eof(&mut self, base: &mut BaseInner, opts: &BaseOptions, writer: &mut W) {
        if let Some(sender) = &mut self.sender {
            sender.send(Message::Eof).unwrap();
        } else {
            self.inner.on_eof(base, opts, writer)
        }
    }
    fn on_separator(&mut self, base: &mut BaseInner, opts: &BaseOptions, writer: &mut W) -> bool {
        if self.sender.is_some() {
            // do nothing
            false
        } else {
            self.inner.on_separator(base, opts, writer)
        }
    }
    fn _on_row(&mut self, base: &mut BaseInner, opts: &BaseOptions, row: Vec<BString>, is_header: bool, writer: &mut W) -> bool {
        if let Some(sender) = &mut self.sender {
            if is_header {
                sender.send(Message::Header(row)).unwrap();
            } else {
                sender.send(Message::Row(row)).unwrap();
            }
            false
        } else {
            self.inner._on_row(base, opts, row, is_header, writer)
        }
    }
}
