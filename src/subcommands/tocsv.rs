use anyhow::Result;
use crate::base;
use clap::{Parser};

#[derive(Parser)]
#[command(about = "convert to csv")]
pub struct Opts {
}

pub struct Handler {
}

impl Handler {
    pub fn new(_: Opts, base: &mut base::Base, _is_tty: bool) -> Result<Self> {
        base.opts.ofs = Some(",".into());
        Ok(Self {})
    }
}

impl base::Processor for Handler {}
