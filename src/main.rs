mod base;
use clap::Parser;

fn main() {
    let mut opts = base::BaseOptions::parse();
    opts.post_process();
    let mut base = base::Processor{};
    base.process_file(std::io::stdin(), opts, true);
}
