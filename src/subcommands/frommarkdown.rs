use anyhow::Result;
use crate::base;
use once_cell::sync::Lazy;
use regex::bytes::Regex;
use bstr::{BString, BStr, ByteSlice};
use clap::Parser;

static SEPARATOR: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s*:?-+:?\s*").unwrap());
static ESCAPE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\\(.)").unwrap());
static TABLE_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"(\\.|[^|])*").unwrap());

#[derive(Parser)]
#[command(about = "convert from markdown table")]
pub struct Opts {
}

pub struct Handler {
    just_got_header: bool,
}

impl Handler {
    pub fn new(_opts: Opts, base: &mut base::Base) -> Result<Self> {
        base.opts.no_header = false;
        base.opts.header = Some(true);
        base.opts.irs = Some("\n".into());
        Ok(Self {
            just_got_header: false,
        })
    }
}

impl base::Processor for Handler {
    fn on_header(&mut self, base: &mut base::Base, header: Vec<BString>) -> Result<()> {
        self.just_got_header = true;
        base.on_header(header)
    }

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> Result<()> {
        if self.just_got_header {
            self.just_got_header = false;
            if row.iter().all(|r| SEPARATOR.is_match(r)) {
                return Ok(())
            }
        }

        base.on_row(row)
    }

    fn parse_line(&self, _base: &mut base::Base, line: &BStr, mut row: Vec<BString>, _quote: u8) -> (Vec<BString>, bool) {
        row.clear();

        let mut line = TABLE_REGEX.find_iter(line).map(|m| m.as_bytes());
        // first column should be empty
        if line.next().is_none_or(|col| col.trim().len() != col.len()) {
            // print('invalid markdown table row:', line, file=sys.stderr)
            return (row, true)
        }

        let line = line.map(|col| ESCAPE.replace_all(col.trim(), b"$1").into_owned().into());
        row.extend(line);

        // last column should be empty
        if row.pop().is_none_or(|col| col.trim().len() != col.len()) || row.is_empty() {
            // print('invalid markdown table row:', line, file=sys.stderr)
            return (row, true)
        }

        (row, false)
    }

}
