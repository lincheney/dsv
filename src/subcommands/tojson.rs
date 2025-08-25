use anyhow::Result;
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
    pub fn new(_opts: Opts, _base: &mut base::Base, _is_tty: bool) -> Result<Self> {
        Ok(Self {
            header: vec![],
        })
    }
}

impl base::Processor for Handler {

    fn on_header(&mut self, _base: &mut base::Base, header: Vec<BString>) -> Result<bool> {
        self.header = header.iter().map(|h| h.to_string()).collect();
        Ok(false)
    }

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        // default to numbered keys if header names run out
        let keys = self.header.iter().cloned().chain((self.header.len()..).map(|i| i.to_string()));
        let values = row.iter().map(|r| r.to_string().into());

        let output = keys.zip(values).collect();
        let output = serde_json::Value::Object(output);
        let output = serde_json::to_vec(&output).unwrap();
        base.write_raw(output.into());
        Ok(false)
    }

}
