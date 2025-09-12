use std::collections::{HashMap, HashSet};
use crate::column_slicer::ColumnSlicer;
use anyhow::{Result};
use crate::base;
use bstr::{BString};
use regex::bytes::Regex;
use clap::{Parser};

#[derive(Parser, Default)]
#[command(about = "reshape to long format")]
pub struct Opts {
    #[arg(default_value = "value", help = "value field (timevar/wide variable)")]
    value: String,
    #[arg(short = 'k', long, help = "reshape only these fields")]
    fields: Vec<String>,
    #[arg(short = 'r', long, help = "treat fields as regexes")]
    regex: bool,
    #[arg(long, help = "exclude, rather than include, field names")]
    complement: bool,
    #[arg(long, default_value = "^(.*?)_(.*)$", help = "regex to split wide columns")]
    format: String,
}

pub struct Handler {
    opts: Opts,
    wide_header: Option<HashMap<BString, usize>>,
    header_matches: Option<Vec<(BString, BString)>>,
    column_slicer: ColumnSlicer,
    format_slicer: ColumnSlicer,
    format_pattern: Regex,
}

impl Handler {
    pub fn new(opts: Opts, _: &mut base::Base) -> Result<Self> {
        Ok(Self{
            column_slicer: ColumnSlicer::new(&opts.fields, opts.regex),
            format_slicer: ColumnSlicer::new([&opts.format], true),
            format_pattern: Regex::new(&opts.format).unwrap(),
            opts,
            wide_header: None,
            header_matches: None,
        })
    }
}

impl base::Processor for Handler {

    fn on_header(&mut self, base: &mut base::Base, header: Vec<BString>) -> Result<bool> {
        self.column_slicer.make_header_map(&header);
        self.format_slicer.make_header_map(&header);

        let mut group_header = vec![];
        let mut wide_header = HashSet::new();
        let mut header_matches = vec![];
        for (i, h) in header.into_iter().enumerate() {
            if self.column_slicer.matches(i) && let Some(c) = self.format_pattern.captures(&h) {
                wide_header.insert(c.get(1).map_or(b"" as _, |m| m.as_bytes()).to_owned());
                header_matches.push((
                    c.get(1).or_else(|| c.name("key")).map_or(b"" as _, |m| m.as_bytes()).to_owned().into(),
                    c.get(2).or_else(|| c.name("value")).map_or(b"" as _, |m| m.as_bytes()).to_owned().into(),
                ));
            } else {
                group_header.push(h.clone());
            }
        }

        let wide_header: Vec<_> = wide_header.into_iter().map(|x| x.into()).collect();

        let mut header = group_header;
        header.push(std::mem::take(&mut self.opts.value).into());
        header.extend(wide_header.iter().cloned());

        let wide_header = wide_header.into_iter().enumerate().map(|(i, v)| (v, i)).collect();
        self.header_matches = Some(header_matches);
        self.wide_header = Some(wide_header);
        base.on_header(header)
    }

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        // not a lot we can do without headers ...
        if let Some((header_matches, wide_header)) = self.header_matches.as_ref().zip(self.wide_header.as_ref()) {

            let mut keys = vec![];
            let mut wide = vec![];
            for (i, col) in row.into_iter().enumerate() {
                if self.column_slicer.matches(i) && self.format_slicer.matches(i) {
                    wide.push(col);
                } else {
                    keys.push(col);
                }
            }

            let mut groups = HashMap::new();
            for ((h, lv), value) in header_matches.iter().zip(wide) {
                let group = groups.entry(lv).or_insert_with(|| vec![b"".into(); wide_header.len()]);
                group[wide_header[h]] = value;
            }

            for (lv, mut vals) in groups {
                let mut row = keys.clone();
                row.push(lv.clone());
                row.append(&mut vals);
                if base.on_row(row)? {
                    return Ok(true)
                }
            }
        }

        Ok(false)
    }
}
