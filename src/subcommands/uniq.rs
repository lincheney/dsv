use crate::base;
use bstr::{BString};
use std::collections::{HashMap, hash_map::Entry};
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
    #[arg(short = 'C', long, overrides_with_all = ["count", "group"], help = "name of column to put the count in")]
    count_column: Option<String>,
    #[arg(long, action = ArgAction::SetTrue, overrides_with_all = ["count", "count_column"], help = "show all items, separating groups with an empty line")]
    group: bool,
}

pub struct Handler {
    opts: Opts,
    column_slicer: ColumnSlicer,
    groups: HashMap<Vec<BString>, (usize, Vec<Vec<BString>>)>,
}

impl base::Processor<Opts> for Handler {
    fn new(mut opts: Opts) -> Self {
        let column_slicer = ColumnSlicer::new(&opts.fields, opts.regex);
        if opts.count && opts.count_column.is_none() {
            opts.count_column = Some("count".into());
        }
        Self {
            opts,
            column_slicer,
            groups: HashMap::new(),
        }
    }

    fn on_header(&mut self, base: &mut base::Base, mut header: Vec<BString>) -> bool {
        self.column_slicer.make_header_map(&header);
        if let Some(count_column) = &self.opts.count_column {
            header.insert(0, count_column.as_bytes().into());
        }
        base.on_header(header)
    }

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> bool {
        let key = self.column_slicer.slice(&row, self.opts.complement, true);
        match self.groups.entry(key) {
            Entry::Occupied(mut entry) => {
                if self.opts.group {
                    entry.get_mut().1.push(row);
                } else if self.opts.count_column.is_some() {
                    entry.get_mut().0 += 1;
                }
            },
            Entry::Vacant(entry) => {
                if self.opts.group || self.opts.count_column.is_some() {
                    entry.insert((1, vec![row]));
                } else {
                    // don't need a count, don't need a group, print immediately
                    entry.insert((1, vec![]));
                    return base.on_row(row)
                }
            },
        }
        false
    }

    fn on_eof(&mut self, base: &mut base::Base) {
        if self.opts.group {
            let mut first = true;
            'outer: for (_count, rows) in self.groups.values_mut() {
                if !first && base.on_separator() {
                    break
                }
                first = false;
                for row in rows.drain(..) {
                    if base.on_row(row) {
                        break 'outer
                    }
                }
            }

        } else if self.opts.count_column.is_some() {
            for (count, rows) in self.groups.values_mut() {
                let mut row = rows.swap_remove(0);
                row.insert(0, format!("{count}").into());
                if base.on_row(row) {
                    break
                }
            }
        }
        base.on_eof()
    }
}
