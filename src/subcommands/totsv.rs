use anyhow::Result;
use crate::base;
use clap::{Parser};

#[derive(Parser)]
#[command(about = "convert to tsv")]
pub struct Opts {
}

pub struct Handler {
}

impl Handler {
    pub fn new(_: Opts, base: &mut base::Base) -> Result<Self> {
        base.opts.ofs = Some("\t".into());
        Ok(Self {})
    }
}

impl base::Processor for Handler {}
