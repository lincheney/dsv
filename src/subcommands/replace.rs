use anyhow::Result;
use crate::base;
use super::grep;
use bstr::{BString};
use clap::{Parser};

#[derive(Parser)]
#[command(about = "print lines that match patterns")]
pub struct Opts {
    #[arg(required_unless_present_any = ["regexp", "file"], help = "pattern to search for")]
    pattern: Option<String>,
    #[arg(required_unless_present_all = ["regexp", "pattern"], help = "replaces every match with the given text")]
    replace: Option<String>,
    #[command(flatten)]
    common: grep::CommonOpts,
}

pub struct Handler {
    inner: super::grep::Handler,
}

impl Handler {
    pub fn new(mut opts: Opts, base: &mut base::Base, is_tty: bool) -> Result<Self> {
        let mut grep_opts = grep::Opts::default();
        // can't make pattern optional and replace required
        // so if replace is missing, the value is actually in optional
        grep_opts.replace = Some(opts.replace.or_else(|| opts.pattern.take()).unwrap());
        grep_opts.patterns = opts.pattern.into_iter().collect();
        grep_opts.common = opts.common;
        grep_opts.passthru = true;
        Ok(Self{
            inner: grep::Handler::new(grep_opts, base, is_tty)?,
        })
    }
}

impl base::Processor for Handler {
    fn on_header(&mut self, base: &mut base::Base, header: Vec<BString>) -> Result<bool> {
        self.inner.on_header(base, header)
    }

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        self.inner.on_row(base, row)
    }

    fn on_eof(self, base: &mut base::Base) -> Result<bool> {
        self.inner.on_eof(base)
    }
}
