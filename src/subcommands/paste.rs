use anyhow::{Result};
use std::sync::mpsc::{self, Sender, Receiver};
use crate::base::*;
use bstr::BString;
use clap::{Parser};

#[derive(Parser)]
#[command(about = "concatenate files by column")]
pub struct Opts {
    #[arg(help = "other files to concatenate to stdin")]
    files: Vec<String>,
}

pub struct Handler {
    receivers: Vec<Receiver<Vec<BString>>>,
    err_receivers: Vec<Receiver<Result<()>>>,
    row_sizes: Vec<usize>,
}

impl Handler {
    pub fn new(opts: Opts, base: &mut Base) -> Result<Self> {

        let mut receivers = vec![];
        let mut err_receivers = vec![];
        for file in opts.files {
            let (sender, receiver) = mpsc::channel();
            let (err_sender, err_receiver) = mpsc::channel();
            receivers.push(receiver);
            err_receivers.push(err_receiver);
            let mut base = base.clone();
            base.scope.spawn(move || {
                let result = (|| {
                    let file = std::fs::File::open(file)?;
                    let file = std::io::BufReader::new(file);
                    Child{ sender }.process_file(file, &mut base, Callbacks::ON_HEADER | Callbacks::ON_ROW)?;
                    Ok(())
                })();
                err_sender.send(result).unwrap();
            });
        }

        Ok(Self {
            receivers,
            err_receivers,
            row_sizes: vec![],
        })
    }
}

impl Processor for Handler {
    fn on_header(&mut self, base: &mut Base, header: Vec<BString>) -> Result<bool> {
        base.on_header(self.paste_row(header))
    }

    fn on_row(&mut self, base: &mut Base, row: Vec<BString>) -> Result<bool> {
        base.on_row(self.paste_row(row))
    }

    fn on_eof(self, base: &mut Base) -> Result<bool> {
        crate::utils::chain_errors(self.err_receivers.iter().map(|r| r.recv().unwrap()))?;
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

impl Processor for Child {
    fn on_header(&mut self, _base: &mut Base, header: Vec<BString>) -> Result<bool> {
        Ok(self.sender.send(header).is_err())
    }
    fn on_row(&mut self, _base: &mut Base, row: Vec<BString>) -> Result<bool> {
        Ok(self.sender.send(row).is_err())
    }
}
