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
    pub fn new(_: Opts, base: &mut base::Base) -> Result<Self> {
        base.opts.page = true;
        Ok(Self {})
    }
}

impl base::Processor for Handler {}
