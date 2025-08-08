use crate::base;
use clap::{Parser};

#[derive(Parser)]
#[command(about = "view the file in a pager")]
pub struct Opts {
}

pub struct Handler {
}

impl Handler {
    pub fn new(_: Opts) -> Self {
        Self {}
    }
}

impl base::Processor for Handler {
    fn process_opts(&mut self, opts: &mut base::BaseOptions, _is_tty: bool) {
        opts.page = true;
    }

}
