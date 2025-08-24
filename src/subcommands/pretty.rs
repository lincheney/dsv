use anyhow::Result;
use crate::base;
use clap::{Parser};

#[derive(Parser)]
#[command(about = "pretty prints the file")]
pub struct Opts {
}

pub struct Handler {
}

impl Handler {
    pub fn new(_: Opts) -> Result<Self> {
        Ok(Self {})
    }
}

impl base::Processor for Handler {
    fn process_opts(&mut self, opts: &mut base::BaseOptions, is_tty: bool) {
        self._process_opts(opts, is_tty);
        opts.pretty = true;
    }

}
