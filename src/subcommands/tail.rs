use anyhow::Result;
use crate::base;
use bstr::BString;
use clap::Parser;
use std::collections::VecDeque;

#[derive(Parser)]
#[command(about = "output the last lines")]
pub struct Opts {
    #[arg(short = 'n', long, default_value = "10", value_name = "NUM", help = "print the last NUM lines")]
    lines: String,
}

pub struct Handler {
    ring: Option<VecDeque<Vec<BString>>>,
    lines: usize,
    count: usize,
}

impl Handler {
    pub fn new(opts: Opts, _base: &mut base::Base) -> Result<Self> {
        let lines = opts.lines.parse::<usize>().unwrap();
        let ring = if opts.lines.starts_with('+') { None } else { Some(VecDeque::with_capacity(lines)) };

        Ok(Self {
            ring,
            lines,
            count: 0,
        })
    }
}

impl base::Processor for Handler {
    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> Result<()> {
        if let Some(ring) = self.ring.as_mut() {
            // Store the last n lines
            if ring.len() == self.lines {
                ring.pop_front();
            }
            ring.push_back(row);
            Ok(())
        } else {
            // Handle the case where -n has a plus sign
            if self.count < self.lines {
                self.count += 1;
                Ok(())
            } else {
                base.on_row(row)
            }
        }
    }

    fn on_eof(self, base: &mut base::Base) -> Result<bool> {
        if let Some(ring) = self.ring {
            for row in ring {
                base.on_row(row)?;
            }
        }
        base.on_eof()
    }
}
