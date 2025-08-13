use crate::base;
use bstr::BString;
use clap::Parser;
use serde_json;

#[derive(Parser)]
#[command(about = "convert to json")]
pub struct Opts {
}

pub struct Handler {
    header: Vec<String>,
}

impl Handler {
    pub fn new(_opts: Opts) -> Self {
        Self {
            header: vec![],
        }
    }
}

impl<H: base::Hook<W>, W: crate::writer::Writer> base::Processor<H, W> for Handler {

    fn on_header(&mut self, _base: &mut base::Base<H, W>, header: Vec<BString>) -> bool {
        self.header = header.iter().map(|h| h.to_string()).collect();
        false
    }

    fn on_row(&mut self, base: &mut base::Base<H, W>, row: Vec<BString>) -> bool {
        // default to numbered keys if header names run out
        let keys = self.header.iter().cloned().chain((self.header.len()..).map(|i| i.to_string()));
        let values = row.iter().map(|r| r.to_string().into());

        let output = keys.zip(values).collect();
        let output = serde_json::Value::Object(output);
        base.writer.write_raw_with(&base.opts, false, |file| Ok(serde_json::to_writer(file, &output)?));
        false
    }

}
