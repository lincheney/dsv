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
    pub fn new(_: Opts, base: &mut base::Base, _is_tty: bool) -> Result<Self> {
        base.opts.pretty = true;
        Ok(Self {})
    }
}

impl base::Processor for Handler {}
