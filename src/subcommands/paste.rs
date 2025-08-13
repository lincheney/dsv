use std::sync::mpsc::{self, Sender, Receiver};
use crate::base;
use bstr::BString;
use anyhow::Result;
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
    row_sizes: Vec<usize>,
}

impl Handler {
    pub fn new(opts: Opts) -> Self {
        Self {
            opts,
            receivers: vec![],
            row_sizes: vec![],
        }
    }
}

impl<H: base::Hook<W>, W: crate::writer::Writer> base::Processor<H, W> for Handler {

    fn process_file<R: std::io::Read>(
        &mut self,
        file: R,
        base: &mut base::Base<H, W>,
        do_callbacks: base::Callbacks,
    ) -> Result<std::process::ExitCode> {

        std::thread::scope(|scope| {
            let files = std::mem::take(&mut self.opts.files);
            for file in files {
                let (sender, receiver) = mpsc::channel();
                self.receivers.push(receiver);
                let opts = base.opts.clone();

                scope.spawn(move || {
                    let mut base: base::Base<_, crate::writer::BaseWriter> = base::Base::new(opts, base::BaseHook{});
                    let file = std::fs::File::open(file).unwrap();
                    let file = std::io::BufReader::new(file);
                    let _ = Child{ sender }.process_file(file, &mut base, base::Callbacks::all());
                });
            }
            self._process_file(file, base, do_callbacks)
        })
    }

    fn on_header(&mut self, base: &mut base::Base<H, W>, header: Vec<BString>) -> bool {
        base.on_header(self.paste_row(header))
    }

    fn on_row(&mut self, base: &mut base::Base<H, W>, row: Vec<BString>) -> bool {
        base.on_row(self.paste_row(row))
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

impl<H: base::Hook<W>, W: crate::writer::Writer> base::Processor<H, W> for Child {
    fn on_header(&mut self, _base: &mut base::Base<H, W>, header: Vec<BString>) -> bool {
        self.sender.send(header).unwrap();
        false
    }
    fn on_row(&mut self, _base: &mut base::Base<H, W>, row: Vec<BString>) -> bool {
        self.sender.send(row).unwrap();
        false
    }
}
