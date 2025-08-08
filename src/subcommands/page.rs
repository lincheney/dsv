use crate::base;
use super::grep;
use bstr::{BString};
use clap::{Parser};

#[derive(Parser)]
#[command(about = "view the file in a pager")]
pub struct Opts {
}

pub struct Handler {
}

impl base::Processor<Opts> for Handler {
    fn new(_: Opts) -> Self {
        Self {}
    }

    fn process_opts(&mut self, opts: &mut base::BaseOptions, _is_tty: bool) {
        opts.page = true;
    }

}
