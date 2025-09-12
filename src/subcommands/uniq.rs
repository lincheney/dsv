use anyhow::Result;
use crate::base;
use bstr::{BString};
use std::collections::{HashMap};
use crate::column_slicer::ColumnSlicer;
use clap::{Parser, ArgAction};

#[derive(Parser)]
#[command(about = "omit repeated lines")]
pub struct Opts {
    #[arg(help = "sort based only on these fields")]
    fields: Vec<String>,
    #[arg(short = 'x', long, action = ArgAction::SetTrue, help = "exclude, rather than include, field names")]
    complement: bool,
    #[arg(long, action = ArgAction::SetTrue, help = "treat fields as regexes")]
    regex: bool,
    #[arg(short = 'c', long, action = ArgAction::SetTrue, help = "prefix lines by the number of occurrences")]
    count: bool,
    #[arg(short = 'C', long, help = "name of column to put the count in")]
    count_column: Option<String>,
    #[arg(long, action = ArgAction::SetTrue, help = "show all items, separating groups with an empty line")]
    group: bool,
    #[arg(long, conflicts_with_all = ["group"], help = "only print duplicate lines, one for each group")]
    repeated: bool,
    #[arg(long, conflicts_with_all = ["group"], help = "print all duplicate lines")]
    repeated_all: bool,
}

type Rope = Vec<BString>;
pub struct Handler {
    map: HashMap<Rope, (usize, Vec<Rope>)>,
    gather: bool,
    repeated: bool,
    print_early: bool,
    column_slicer: ColumnSlicer,
    opts: Opts,
}

impl Handler {
    pub fn new(mut opts: Opts, _base: &mut base::Base) -> Result<Self> {
        if opts.count {
            opts.count_column.get_or_insert_with(|| "count".into());
        }

        let gather = opts.group || opts.repeated_all;
        Ok(Self {
            map: HashMap::new(),
            gather,
            repeated: opts.repeated || opts.repeated_all,
            print_early: !gather && opts.count_column.is_none(),
            column_slicer: ColumnSlicer::new(&opts.fields, opts.regex),
            opts,
        })
    }
}

impl base::Processor for Handler {
    fn on_header(&mut self, base: &mut base::Base, mut header: Vec<BString>) -> Result<bool> {
        self.column_slicer.make_header_map(&header);
        if let Some(count_column) = &self.opts.count_column {
            header.insert(0, count_column.as_bytes().into());
        }
        base.on_header(header)
    }

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        let key = self.column_slicer.slice(&row, self.opts.complement, true);

        let entry = self.map.entry(key).or_insert((0, vec![]));
        entry.0 += 1;

        if self.print_early {
            if entry.0 == (if self.repeated { 2 } else { 1 }) {
                return base.on_row(row)
            }
        } else if self.gather || entry.1.is_empty() {
            entry.1.push(row);
        }

        Ok(false)
    }

    fn on_eof(self, base: &mut base::Base) -> Result<bool> {
        if !self.print_early {
            let mut first = true;
            'outer: for (_, (count, rows)) in self.map {
                if self.repeated && count < 2 {
                    continue
                }
                if self.opts.group && !first && base.on_separator() {
                    break
                }
                first = false;
                for mut row in rows {
                    if self.opts.count_column.is_some() {
                        row.insert(0, format!("{count}").into());
                    }
                    if base.on_row(row)? {
                        break 'outer
                    }
                }
            }
        }

        base.on_eof()
    }
}
