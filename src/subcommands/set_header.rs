use crate::base;
use bstr::{BString};
use clap::{Parser, ArgAction};

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
    pub fn new(opts: Opts) -> Self {
        Self {
            opts,
            got_header: false,
        }
    }
}

impl base::Processor for Handler {

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> bool {
        if !self.got_header {
            let header = if self.opts.auto {
                (0..row.len()).map(|i| format!("col{i}").into()).collect()
                // header = [self.opts.auto % (i+1) for i in range(len(row))]
            } else {
                vec![]
            };
            if base.on_header(header) {
                return true
            }
        }

        base.on_row(row)
    }

    fn on_header(&mut self, base: &mut base::Base, mut header: Vec<BString>) -> bool {
        self.got_header = true;

        for [old, new] in self.opts.rename.as_chunks::<2>().0.iter() {
            let i = if let Ok(i) = old.parse::<usize>() {
                i - 1
            } else if let Some(i) = header.iter().position(|h| h == old.as_str()) {
                i
            } else {
                continue
            };

            if let Some(h) = header.get_mut(i) {
                *h = new.as_str().into();
            } else {
                header.resize(i, b"".into());
                header.push(new.as_str().into());
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
            false
        } else {
            base.on_header(header)
        }
    }

}
