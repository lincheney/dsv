use anyhow::Result;
use crate::base;
use bstr::{BString};
use clap::{Parser};

#[derive(Parser)]
#[command(about = "prints each column on a separate line")]
pub struct Opts {
    #[arg(short = 'n', long, value_name = "NUM", help = "print the first NUM lines")]
    lines: Option<usize>,
    #[arg(long, value_enum, default_value_t = base::AutoChoices::Auto, help = "show a separator between the rows")]
    row_sep: base::AutoChoices,
}

pub struct Handler {
    opts: Opts,
    count: usize,
    header: Option<Vec<BString>>,
}

impl Handler {
    pub fn new(mut opts: Opts, base: &mut base::Base, is_tty: bool) -> Result<Self> {
        opts.row_sep = opts.row_sep.resolve(is_tty);
        if base.opts.ofs.is_none() {
            base.opts.pretty = true;
        }

        Ok(Self{
            opts,
            count: 0,
            header: None,
        })
    }
}

impl base::Processor for Handler {
    fn on_header(&mut self, base: &mut base::Base, header: Vec<BString>) -> Result<bool> {
        self.header = Some(header);
        base.on_header(vec![b"row".into(), b"column".into(), b"key".into(), b"value".into()])
    }

    fn on_row(&mut self, base: &mut base::Base, mut row: Vec<BString>) -> Result<bool> {
        if self.count == 0 {
            // first row
            if self.header.is_none() && base.on_header(vec![b"row".into(), b"column".into(), b"value".into()])? {
                return Ok(true)
            }

        } else if self.opts.row_sep == base::AutoChoices::Always && base.on_separator() {
            return Ok(true)
        }

        self.count += 1;

        for (i, value) in row.drain(..).enumerate() {
            let mut row = vec![format!("{}", self.count).into(), format!("{}", i+1).into()];
            if let Some(header) = &self.header {
                if let Some(h) = header.get(i) {
                    row.push(format!("{h}").into());
                } else {
                    row.push(b"".into());
                }
            }
            row.push(value);
            if base.on_row(row)? {
                return Ok(false)
            }
        }

        Ok(self.opts.lines.is_some_and(|lines| self.count >= lines))
    }
}
