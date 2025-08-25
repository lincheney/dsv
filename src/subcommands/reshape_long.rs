use anyhow::{Result};
use crate::base;
use bstr::{BString};
use clap::{Parser, ArgAction};

#[derive(Parser, Default)]
#[command(about = "reshape to long format")]
pub struct Opts {
    #[arg(short = 'k', long, action = ArgAction::Append, help = "reshape only these fields")]
    fields: Vec<String>,
    #[arg(short = 'r', long, action = ArgAction::SetTrue, help = "treat fields as regexes")]
    regex: bool,
    #[arg(long, action = ArgAction::SetTrue, help = "exclude, rather than include, field names")]
    complement: bool,
    #[arg(short = 'k', long, default_value = "key", help = "name of the key field")]
    key: String,
    #[arg(short = 'v', long, default_value = "value", help = "name of the value field")]
    value: String,
}

pub struct Handler {
    opts: Opts,
    header: Option<Vec<BString>>,
    column_slicer: crate::column_slicer::ColumnSlicer,
}

impl Handler {
    pub fn new(opts: Opts, _: &mut base::Base, _is_tty: bool) -> Result<Self> {
        let column_slicer = crate::column_slicer::ColumnSlicer::new(&opts.fields, opts.regex);
        Ok(Self{
            opts,
            column_slicer,
            header: None,
        })
    }
}

impl base::Processor for Handler {

    fn on_header(&mut self, base: &mut base::Base, header: Vec<BString>) -> Result<bool> {
        self.column_slicer.make_header_map(&header);
        self.header = Some(self.column_slicer.slice(&header, self.opts.complement, true));
        let mut header = self.column_slicer.slice(&header, !self.opts.complement, true);
        header.splice(0..0, [self.opts.key.clone().into(), self.opts.value.clone().into()]);
        base.on_header(header)
    }

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        let values = self.column_slicer.slice(&row, self.opts.complement, true);
        let mut default_keys = None;
        let keys = self.header.as_ref().unwrap_or_else(||
            default_keys.insert((0..row.len()).map(|i| format!("{i}").into()).collect())
        );

        for (k, v) in keys.iter().zip(values) {
            let mut row = row.clone();
            row.splice(0..0, [k.clone(), v]);
            if base.on_row(row)? {
                return Ok(true)
            }
        }
        Ok(false)
    }
}
