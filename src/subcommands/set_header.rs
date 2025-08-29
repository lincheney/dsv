use anyhow::Result;
use crate::base;
use bstr::{BString};
use clap::{Parser, ArgAction};
use crate::column_slicer::ColumnSlicer;

#[derive(Parser)]
#[command(about = "set the header labels")]
pub struct Opts {
    #[arg(help = "new header names")]
    fields: Vec<String>,
    #[arg(long, action = ArgAction::SetTrue, help = "drop all other header names")]
    only: bool,
    #[arg(num_args = 2, action = ArgAction::Append, long, value_names = ["A", "B"], help = "rename field A to B")]
    rename: Vec<String>,
    #[arg(long, action = ArgAction::SetTrue, help = "automatically name the headers, only useful if there is no input")]
    auto: bool,
}

pub struct Handler {
    opts: Opts,
    got_header: bool,
}

impl Handler {
    pub fn new(opts: Opts, _base: &mut base::Base, _is_tty: bool) -> Result<Self> {
        Ok(Self {
            opts,
            got_header: false,
        })
    }
}

impl base::Processor for Handler {

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        if !self.got_header {
            let header = if self.opts.auto {
                (0..row.len()).map(|i| format!("col{i}").into()).collect()
                // header = [self.opts.auto % (i+1) for i in range(len(row))]
            } else {
                vec![]
            };
            if base.on_header(header)? {
                return Ok(true)
            }
        }

        base.on_row(row)
    }

    fn on_header(&mut self, base: &mut base::Base, mut header: Vec<BString>) -> Result<bool> {
        self.got_header = true;

        let mut column_slicer = ColumnSlicer::new(&[], false);
        column_slicer.make_header_map(&header);
        for [old, new] in self.opts.rename.as_chunks::<2>().0 {
            if let Some(i) = column_slicer.get_single_field_index(old) {
                if let Some(h) = header.get_mut(i) {
                    *h = new.as_str().into();
                } else {
                    header.resize(i, b"".into());
                    header.push(new.as_str().into());
                }
            }
        }

        if !self.opts.fields.is_empty() {
            let new = self.opts.fields.iter().cloned().map(|h| h.into());
            if self.opts.only {
                header.clear();
                header.extend(new);
            } else {
                header.splice(0..new.len(), new);
            }
        }

        if header.is_empty() {
            Ok(false)
        } else {
            base.on_header(header)
        }
    }

}
