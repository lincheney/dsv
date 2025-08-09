use crate::base::{self, Processor, Callbacks};
use std::process::ExitCode;
use std::io::Read;
use clap::Parser;
use serde_json;

#[derive(Parser)]
#[command(about = "convert from json")]
pub struct Opts {
}

pub struct Handler {
}

impl Handler {
    pub fn new(_opts: Opts) -> Self {
        Self {}
    }
}

impl Handler {
    fn process_json<R: Read>(&mut self, file: R, base: &mut base::Base, do_callbacks: Callbacks) {
        let mut stream = serde_json::Deserializer::from_reader(file).into_iter::<serde_json::Map<_, _>>();

        let header_row = if let Some(header_row) = stream.next() { header_row } else { return };
        // TODO warn not panic
        let header_row = header_row.unwrap();
        let header: Vec<_> = header_row.keys().cloned().collect();
        if do_callbacks.contains(Callbacks::ON_HEADER) && self.on_header(base, header.iter().map(|x| x.clone().into()).collect()) {
            return
        }

        for row in std::iter::once(Ok(header_row)).chain(stream) {
            // TODO warn not panic
            let row = row.unwrap();
            if do_callbacks.contains(Callbacks::ON_EOF) {
                let values = header.iter().map(|k| {
                    row.get(k)
                        .map(|v| v.as_str().map(|s| s.to_owned()).unwrap_or_else(|| v.to_string()))
                        .unwrap_or_else(String::new)
                        .into()
                }).collect();
                if self.on_row(base, values) {
                    return
                }
            }
        }
    }
}

impl base::Processor for Handler {

    fn process_opts(&mut self, opts: &mut base::BaseOptions, _is_tty: bool) {
        // default to output with tab
        opts.ifs.get_or_insert_with(|| "\t".into());
        opts.ofs.get_or_insert_with(|| "\t".into());
    }

    fn process_file<R: Read>(&mut self, file: R, base: &mut base::Base, do_callbacks: Callbacks) -> anyhow::Result<ExitCode> {
        (base.ifs, base.ofs) = self.determine_delimiters(b"".into(), &base.opts);
        self.process_json(file, base, do_callbacks);
        if do_callbacks.contains(Callbacks::ON_EOF) {
            self.on_eof(base);
        }
        Ok(ExitCode::SUCCESS)
    }

}
