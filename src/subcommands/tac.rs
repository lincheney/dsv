use anyhow::Result;
use crate::base;
use bstr::{BString};
use clap::{Parser};

#[derive(Parser)]
#[command(about = "print the file in reverse")]
pub struct Opts {
}

pub struct Handler {
    rows: Vec<Vec<BString>>,
}

impl Handler {
    pub fn new(_: Opts, _base: &mut base::Base) -> Result<Self> {
        Ok(Self {
            rows: vec![],
        })
    }
}

impl base::Processor for Handler {
    fn on_row(&mut self, _base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        self.rows.push(row);
        Ok(false)
    }

    fn on_eof(self, base: &mut base::Base) -> Result<bool> {
        for row in self.rows.into_iter().rev() {
            if base.on_row(row)? {
                break
            }
        }
        base.on_eof()
    }

}

