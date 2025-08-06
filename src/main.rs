mod base;
use clap::Parser;

fn main() {
    let opts = base::BaseOptions::parse();
    let mut base = base::Base::new(opts);
    base.process_file(std::io::stdin(), true);
}
