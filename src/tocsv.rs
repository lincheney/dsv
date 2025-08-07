use crate::base;
use clap::{Parser};

#[derive(Parser)]
#[command(about = "convert to csv")]
pub struct Opts {
}

pub struct Handler {
}

impl base::Processor<Opts> for Handler {
    fn new(_: Opts) -> Self {
        Self {}
    }

    fn process_opts(&mut self, opts: &mut base::BaseOptions) {
        opts.ofs = Some(",".into());
    }
}

