use anyhow::Result;
use crate::base;
use bstr::BString;
use clap::{Parser, ArgAction};

#[derive(Parser, Default, Clone)]
#[command(about = "concatenate files by row")]
pub struct Opts {
    #[arg(short = 'n', long, action = ArgAction::SetTrue, help = "number all output lines")]
    number: bool,
    #[arg(help = "other files to concatenate to stdin")]
    files: Vec<String>,
}

pub struct Handler {
    opts: Opts,
    row_count: usize,
    got_data: bool,
}

impl Handler {
    pub fn new(opts: Opts, _base: &mut base::Base) -> Result<Self> {
        Ok(Self {
            row_count: 0,
            opts,
            got_data: false,
        })
    }
}

impl base::Processor for Handler {

    fn on_header(&mut self, base: &mut base::Base, mut header: Vec<BString>) -> Result<bool> {
        self.got_data = true;
        if self.opts.number {
            header.insert(0, b"n".into());
        }
        base.on_header(header)
    }

    fn on_row(&mut self, base: &mut base::Base, mut row: Vec<BString>) -> Result<bool> {
        self.got_data = true;
        if self.opts.number {
            self.row_count += 1;
            row.insert(0, format!("{}", self.row_count).into());
        }
        base.on_row(row)
    }

    fn on_eof(mut self, base: &mut base::Base) -> Result<bool> {
        for file in &std::mem::take(&mut self.opts.files) {
            match std::fs::File::open(file) {
                Ok(file) => {
                    let file = std::io::BufReader::new(file);
                    Child{inner: &mut self}.process_file(file, base, base::Callbacks::all() - base::Callbacks::ON_EOF)?;
                },
                Err(e) => {
                    base.write_raw_stderr(format!("{e}: {file}\n").into(), false);
                }
            }
        }
        base.on_eof()
    }

}

struct Child<'a> {
    inner: &'a mut Handler,
}

impl base::Processor for Child<'_> {
    fn on_ofs(&mut self, base: &mut base::Base, ofs: base::Ofs) -> bool {
        !self.inner.got_data && base.on_ofs(ofs)
    }
    fn on_header(&mut self, base: &mut base::Base, header: Vec<BString>) -> Result<bool> {
        Ok(!self.inner.got_data && self.inner.on_header(base, header)?)
    }
    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        self.inner.got_data = true;
        self.inner.on_row(base, row)
    }
}
