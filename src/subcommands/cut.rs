use crate::base;
use bstr::BString;
use clap::{Parser, ArgAction};

#[derive(Parser)]
#[command(about = "select columns")]
pub struct Opts {
    #[arg(required_unless_present = "old_style_fields", help = "select only these fields")]
    fields: Vec<String>,
    #[arg(short = 'f', long = "fields", value_name = "fields", help = "select only these fields")]
    old_style_fields: Vec<String>,
    #[arg(short = 'x', long, action = ArgAction::SetTrue, help = "exclude, rather than include, field names")]
    complement: bool,
    #[arg(short = 'r', long, action = ArgAction::SetTrue, help = "treat fields as regexes")]
    regex: bool,
}

pub struct Handler {
    complement: bool,
    column_slicer: crate::column_slicer::ColumnSlicer,
}

impl base::Processor<Opts> for Handler {
    fn new(mut opts: Opts) -> Self {
        opts.fields.extend(opts.old_style_fields.iter().flat_map(|x| x.split(",")).map(|x| x.into()));

        Self {
            complement: opts.complement,
            column_slicer: crate::column_slicer::ColumnSlicer::new(&opts.fields, opts.regex),
        }
    }

    fn on_header(&mut self, base: &mut base::Base, header: Vec<BString>) -> bool {
        self.column_slicer.make_header_map(&header);
        let header = self.column_slicer.slice(&header, self.complement, true);
        base.on_header(header)
    }

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> bool {
        let row = self.column_slicer.slice(&row, self.complement, true);
        base.on_row(row)
    }
}
