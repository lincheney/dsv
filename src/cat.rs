use crate::base;
use bstr::BString;
use clap::{Parser, ArgAction};

#[derive(Parser, Default)]
#[command(about = "concatenate files by row")]
pub struct Opts {
    #[arg(short = 'n', long, action = ArgAction::SetTrue, help = "number all output lines")]
    number: bool,
    #[arg(help = "other files to concatenate to stdin")]
    files: Vec<String>,
}

pub struct Handler {
    opts: Opts,
    row_count: usize,
}

impl base::Processor<Opts> for Handler {

    fn new(opts: Opts) -> Self {
        Self {
            row_count: 0,
            opts,
        }
    }

    fn on_header(&mut self, base: &mut base::Base, mut row: Vec<BString>) -> bool {
        if self.opts.number {
            row.insert(0, b"n".into());
        }
        base.on_header(row)
    }

    fn on_row(&mut self, base: &mut base::Base, mut row: Vec<BString>) -> bool {
        if self.opts.number {
            self.row_count += 1;
            row.insert(0, format!("{}", self.row_count).into());
        }
        base.on_row(row)
    }

    fn on_eof(&mut self, base: &mut base::Base) {
        let files = std::mem::take(&mut self.opts.files);
        for file in &files {
            let file = std::fs::File::open(file).unwrap();
            let file = std::io::BufReader::new(file);
            self.process_file(file, base, base::Callbacks::ON_ROW);
        }
        base.on_eof();
    }

}
