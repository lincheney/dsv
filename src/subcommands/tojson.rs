use anyhow::Result;
use crate::base;
use crate::writer::{Writer, BaseWriter, WriterState};
use std::io::Write;
use bstr::{BString};
use clap::Parser;
use serde_json;

#[derive(Parser)]
#[command(about = "convert to json")]
pub struct Opts {
}

pub struct Handler {
}

impl Handler {
    pub fn new(_opts: Opts, base: &mut base::Base) -> Result<Self> {
        base.opts.numbered_columns = base::AutoChoices::Never;
        base.opts.drop_header = false;
        base.opts.quote_output = false;
        Ok(Self {})
    }
}

impl base::Processor<JsonWriter> for Handler { }

pub struct JsonWriter {
    inner: BaseWriter,
    header: Vec<String>,
}

impl Writer for JsonWriter {
    fn new(opts: &base::BaseOptions) -> Self {
        Self {
            inner: BaseWriter::new(opts),
            header: vec![],
        }
    }

    fn get_file(&mut self, opts: &base::BaseOptions, has_header: bool) -> Box<dyn Write> {
        self.inner.get_file(opts, has_header)
    }

    fn write_output(
        &mut self,
        state: &mut WriterState,
        row: Vec<BString>,
        _padding: Option<&Vec<usize>>,
        _is_header: bool,
        opts: &base::BaseOptions,
        _ofs: &base::Ofs,
    ) -> Result<()> {
        // default to numbered keys if header names run out
        let keys = self.header.iter().cloned().chain((self.header.len()..).map(|i| i.to_string()));
        let values = row.iter().map(|r| r.to_string().into());

        let output = keys.zip(values).collect();
        let output = serde_json::Value::Object(output);
        self.write_raw_with(state, opts, false, |mut file| {
            serde_json::to_writer(&mut file, &output)?;
            Ok(file)
        })
    }

    fn write_header(
        &mut self,
        _state: &mut WriterState,
        header: base::FormattedRow,
        _padding: Option<&Vec<usize>>,
        _opts: &base::BaseOptions,
        _ofs: &base::Ofs,
    ) -> Result<()> {
        self.header = header.0.iter().map(|h| h.to_string()).collect();
        Ok(())
    }

}
