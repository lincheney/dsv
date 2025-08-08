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
    pub fn new(_: Opts) -> Self {
        Self {
            rows: vec![],
        }
    }
}

impl base::Processor for Handler {
    fn on_row(&mut self, _base: &mut base::Base, row: Vec<BString>) -> bool {
        self.rows.push(row);
        false
    }

    fn on_eof(&mut self, base: &mut base::Base) {
        for row in self.rows.drain(..).rev() {
            if base.on_row(row) {
                break
            }
        }
        base.on_eof()
    }

}

