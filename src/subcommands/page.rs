use anyhow::Result;
use crate::base;
use clap::{Parser};

#[derive(Parser)]
#[command(about = "view the file in a pager")]
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
        opts.page = true;
    }

}
