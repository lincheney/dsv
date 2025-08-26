use anyhow::Result;
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
    pub fn new(_opts: Opts, base: &mut base::Base, _is_tty: bool) -> Result<Self> {
        // default to output with tab
        base.opts.ifs.get_or_insert_with(|| "\t".into());
        base.opts.ofs.get_or_insert_with(|| "\t".into());
        Ok(Self {})
    }
}

impl Handler {
    fn process_json<R: Read>(&mut self, file: R, base: &mut base::Base, do_callbacks: Callbacks) -> Result<()> {
        let mut stream = serde_json::Deserializer::from_reader(file).into_iter::<serde_json::Map<_, _>>();

        let header_row = if let Some(header_row) = stream.next() { header_row? } else { return Ok(()) };
        let header: Vec<_> = header_row.keys().cloned().collect();
        if do_callbacks.contains(Callbacks::ON_HEADER) && self.on_header(base, header.iter().map(|x| x.clone().into()).collect())? {
            return Ok(())
        }

        for row in std::iter::once(Ok(header_row)).chain(stream) {
            let row = row?;
            if do_callbacks.contains(Callbacks::ON_EOF) {
                let values = header.iter().map(|k| {
                    row.get(k)
                        .map_or_else(String::new, |v| {
                            v.as_str().map_or_else(|| v.to_string(), |s| s.to_owned())
                        })
                        .into()
                }).collect();
                if self.on_row(base, values)? {
                    return Ok(())
                }
            }
        }
        Ok(())
    }
}

impl base::Processor for Handler {
    fn process_file<R: Read>(mut self, file: R, base: &mut base::Base, do_callbacks: Callbacks) -> anyhow::Result<ExitCode> {
        let ofs = self.determine_delimiters(b"".into(), &base.opts).1;
        if !base.on_ofs(ofs) {
            self.process_json(file, base, do_callbacks)?;
            if do_callbacks.contains(Callbacks::ON_EOF) {
                self.on_eof(base)?;
            }
        }
        Ok(ExitCode::SUCCESS)
    }

}
