use anyhow::Result;
use crate::base;
use bstr::BString;
use clap::Parser;

#[derive(Parser)]
#[command(about = "output the first lines")]
pub struct Opts {
    #[arg(short = 'n', long, allow_negative_numbers = true, default_value_t = 10, value_name = "NUM", help = "print the first NUM lines")]
    lines: isize,
}

pub struct Handler {
    ring: Option<std::collections::VecDeque<Vec<BString>>>,
    lines: usize,
    count: usize,
}

impl Handler {
    pub fn new(opts: Opts) -> Result<Self> {
        Ok(Self {
            ring: if opts.lines >= 0 { None } else { Some(std::collections::VecDeque::new()) },
            lines: opts.lines.unsigned_abs(),
            count: 0,
        })
    }
}


impl base::Processor for Handler {

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        if let Some(ring) = self.ring.as_mut() {
            // print except for last n lines
            if ring.len() >= self.lines && let Some(row) = ring.pop_front() && base.on_row(row)? {
                Ok(true)
            } else {
                ring.push_back(row);
                Ok(false)
            }

        } else if self.lines == 0 || base.on_row(row)? {
            Ok(true)
        } else {
            self.count += 1;
            Ok(self.count >= self.lines)
        }
    }
}

