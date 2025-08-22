use anyhow::{Result, Context};
use std::sync::mpsc::{self, Sender, Receiver};
use crate::base;
use bstr::BString;
use clap::{Parser};

#[derive(Parser)]
#[command(about = "concatenate files by column")]
pub struct Opts {
    #[arg(help = "other files to concatenate to stdin")]
    files: Vec<String>,
}

pub struct Handler {
    opts: Opts,
    receivers: Vec<Receiver<Vec<BString>>>,
    err_receivers: Vec<Receiver<Result<()>>>,
    row_sizes: Vec<usize>,
}

impl Handler {
    pub fn new(opts: Opts) -> Self {
        Self {
            opts,
            receivers: vec![],
            err_receivers: vec![],
            row_sizes: vec![],
        }
    }
}

impl base::Processor for Handler {

    fn on_start(&mut self, base: &mut base::Base) -> Result<bool> {
        let files = std::mem::take(&mut self.opts.files);
        for file in files {
            let (sender, receiver) = mpsc::channel();
            let (err_sender, err_receiver) = mpsc::channel();
            self.receivers.push(receiver);
            self.err_receivers.push(err_receiver);
            let mut base = base.clone();
            base.scope.spawn(move || {
                let result = (|| {
                    let file = std::fs::File::open(file)?;
                    let file = std::io::BufReader::new(file);
                    Child{ sender }.process_file(file, &mut base, base::Callbacks::ON_HEADER | base::Callbacks::ON_ROW)?;
                    Ok(())
                })();
                err_sender.send(result).unwrap();
            });
        }
        Ok(false)
    }

    fn on_header(&mut self, base: &mut base::Base, header: Vec<BString>) -> Result<bool> {
        base.on_header(self.paste_row(header))
    }

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        base.on_row(self.paste_row(row))
    }

    fn on_eof(&mut self, base: &mut base::Base) -> Result<bool> {
        let mut result = Ok(());
        for r in self.err_receivers.iter() {
            match r.recv().unwrap() {
                Ok(_) => (),
                e if result.is_ok() => { result = e; },
                Err(e) => { result = result.context(e); },
            }
        }
        result?;

        base.on_eof()
    }
}

impl Handler {
    fn paste_row(&mut self, mut row: Vec<BString>) -> Vec<BString> {
        let mut make_row_sizes = self.row_sizes.is_empty().then(Vec::new);

        // grab a row from each receiver
        for (r, &size) in self.receivers.iter().zip(self.row_sizes.iter().chain(std::iter::repeat(&0))) {
            if let Ok(mut r) = r.recv() {
                if let Some(row_sizes) = make_row_sizes.as_mut() {
                    row_sizes.push(r.len());
                }
                row.append(&mut r);
            } else {
                // pad rows that are missing
                row.extend(std::iter::repeat_n(b"".into(), size));
            }
        }
        row
    }
}

struct Child {
    sender: Sender<Vec<BString>>,
}

impl base::Processor for Child {
    fn on_header(&mut self, _base: &mut base::Base, header: Vec<BString>) -> Result<bool> {
        Ok(self.sender.send(header).is_err())
    }
    fn on_row(&mut self, _base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        Ok(self.sender.send(row).is_err())
    }
}
