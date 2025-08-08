use crate::base;
use clap::{Parser};

#[derive(Parser)]
#[command(about = "pretty prints the file")]
pub struct Opts {
}

pub struct Handler {
}

impl base::Processor<Opts> for Handler {
    fn new(_: Opts) -> Self {
        Self {}
    }

    fn process_opts(&mut self, opts: &mut base::BaseOptions, _is_tty: bool) {
        opts.pretty = true;
    }

}
