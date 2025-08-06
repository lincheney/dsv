mod base;
use clap::Parser;

fn main() {
    let opts = base::BaseOptions::parse();
    let mut base = base::Processor::new(opts);
    base.process_file(std::io::stdin(), true);
}
