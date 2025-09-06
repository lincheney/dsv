use anyhow::{Result};
use crate::base;
use std::collections::{HashSet, HashMap};
use crate::column_slicer::ColumnSlicer;
use bstr::{BString};
use clap::{Parser};

#[derive(Parser, Default)]
#[command(about = "reshape to wide format")]
pub struct Opts {
    #[arg(help = "value field (timevar/wide variable)")]
    value: String,
    #[arg(help = "fields to group by (idvar/long variable)")]
    fields: Vec<String>,
    #[arg(short = 'x', long, help = "exclude, rather than include, field names")]
    complement: bool,
    #[arg(long, help = "treat fields as regexes")]
    regex: bool,
}

struct Slicers {
    group: ColumnSlicer,
    long: ColumnSlicer,
    complement: bool,
}

pub struct Handler {
    group_header: Option<Vec<BString>>,
    wide_header: Option<Vec<BString>>,
    rows: Vec<Vec<BString>>,
    slicers: Slicers,
}

impl Handler {
    pub fn new(opts: Opts, _: &mut base::Base, _is_tty: bool) -> Result<Self> {
        Ok(Self{
            slicers: Slicers{
                group: ColumnSlicer::new(&opts.fields, opts.regex),
                long: ColumnSlicer::new([&opts.value], opts.regex),
                complement: opts.complement,
            },
            group_header: None,
            wide_header: None,
            rows: vec![],
        })
    }
}

impl Slicers {
    fn wide_indices(&self, len: usize) -> impl Iterator<Item=usize> {
        // things that are not the group or long value
        let long = self.long.indices(len, false).next();
        self.group.indices(len, !self.complement)
            .filter(move |&i| Some(i) != long)
    }
}

impl base::Processor for Handler {

    fn on_header(&mut self, _base: &mut base::Base, header: Vec<BString>) -> Result<bool> {
        self.slicers.group.make_header_map(&header);
        self.slicers.long.make_header_map(&header);
        self.group_header = Some(self.slicers.group.slice(&header, self.slicers.complement, true));
        self.wide_header = Some(self.slicers.wide_indices(header.len()).map(|i| header[i].clone()).collect());
        Ok(false)
    }

    fn on_row(&mut self, _base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        self.rows.push(row);
        Ok(false)
    }

    fn on_eof(self, base: &mut base::Base) -> Result<bool> {
        let empty = BString::new(vec![]);

        let mut long_values = HashSet::new();
        let mut groups = HashMap::new();
        for row in self.rows {
            // what if this row has no values?
            let long_value = if let Some(i) = self.slicers.long.indices(row.len(), false).next() {
                &row[i]
            } else {
                &empty
            };
            long_values.insert(long_value.clone());

            let key = self.slicers.group.slice(&row, self.slicers.complement, true);
            let group = groups.entry(key).or_insert_with(Vec::new);
            group.push((long_value.clone(), row));
        }
        let long_values: Vec<_> = long_values.into_iter().collect();

        if let Some((wide_header, group_header)) = self.wide_header.zip(self.group_header) {
            let new_headers = group_header.into_iter()
                .chain(
                    wide_header.iter()
                        .flat_map(|h| std::iter::repeat(h).zip(long_values.iter()))
                        .map(|(h, v)| format!("{h}_{v}").into())
                ).collect();
            if base.on_header(new_headers)? {
                return Ok(true)
            }
        }

        let long_value_map: HashMap<_, _> = long_values.iter().enumerate().map(|(i, v)| (v, i)).collect();
        for (key, group) in groups {
            let mut newrow = key;
            let num_columns = group.iter().map(|(_, row)| row.len()).max().unwrap();

            for i in self.slicers.wide_indices(num_columns) {
                let start = newrow.len();
                newrow.extend(std::iter::repeat_n(empty.clone(), long_values.len()));
                for (long_value, row) in &group {
                    let x = row.get(i).unwrap_or(&empty).clone();
                    newrow[start + long_value_map[long_value]] = x;
                }
            }
            if base.on_row(newrow)? {
                return Ok(true)
            }
        }

        base.on_eof()
    }
}

