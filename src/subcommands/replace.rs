use crate::base;
use super::grep;
use bstr::{BString};
use clap::{Parser};

#[derive(Parser)]
#[command(about = "print lines that match patterns")]
pub struct Opts {
    #[arg(help = "pattern to search for")]
    patterns: String,
    #[arg(help = "replaces every match with the given text")]
    replace: String,
    #[command(flatten)]
    common: grep::CommonOpts,
}

pub struct Handler {
    inner: super::grep::Handler,
}

impl base::Processor<Opts> for Handler {
    fn new(opts: Opts) -> Self {
        let mut grep_opts = grep::Opts::default();
        grep_opts.patterns = vec![opts.patterns];
        grep_opts.replace = Some(opts.replace);
        grep_opts.common = opts.common;
        Self{
            inner: grep::Handler::new(grep_opts),
        }
    }

    fn process_opts(&mut self, opts: &mut base::BaseOptions) {
        self.inner.process_opts(opts)
    }

    fn on_header(&mut self, base: &mut base::Base, header: Vec<BString>) -> bool {
        self.inner.on_header(base, header)
    }

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> bool {
        self.inner.on_row(base, row)
    }
}
