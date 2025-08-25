use anyhow::{Result};
use crate::base;
use std::collections::{HashSet, HashMap, hash_map::Entry};
use bstr::{BString};
use clap::{Parser};

#[derive(Parser, Default)]
#[command(about = "reshape to wide format")]
pub struct Opts {
    #[arg(help = "key field")]
    key: String,
    #[arg(help = "value field")]
    value: String,
}

pub struct Handler {
    header: Option<Vec<BString>>,
    column_slicer: crate::column_slicer::ColumnSlicer,
    rows: Vec<Vec<BString>>,
}

impl Handler {
    pub fn new(opts: Opts, _: &mut base::Base, _is_tty: bool) -> Result<Self> {
        let column_slicer = crate::column_slicer::ColumnSlicer::new(&[opts.key, opts.value], false);
        Ok(Self{
            column_slicer,
            header: None,
            rows: vec![],
        })
    }
}

impl base::Processor for Handler {

    fn on_header(&mut self, _base: &mut base::Base, header: Vec<BString>) -> Result<bool> {
        self.column_slicer.make_header_map(&header);
        self.header = Some(self.column_slicer.slice(&header, true, true));
        Ok(false)
    }

    fn on_row(&mut self, _base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        self.rows.push(row);
        Ok(false)
    }

    fn on_eof(&mut self, base: &mut base::Base) -> Result<bool> {
        let header = std::mem::take(&mut self.header);
        let rows = std::mem::take(&mut self.rows);

        let mut seen = HashSet::new();
        for row in &rows {
            let i = self.column_slicer.indices(row.len(), false).next().unwrap();
            seen.insert(row[i].clone());
        }

        let new_headers: Vec<_> = seen.into_iter().chain(header.into_iter().flatten()).collect();
        if base.on_header(new_headers.clone())? {
            return Ok(true)
        }

        let new_headers: HashMap<_, _> = new_headers.iter().enumerate().map(|(i, h)| (h, i)).collect();

        let mut groups: HashMap<Vec<BString>, Vec<Option<BString>>> = HashMap::new();
        for mut row in rows.into_iter() {
            let mut indices = self.column_slicer.indices(row.len(), false);
            let group_key = self.column_slicer.slice(&row, true, true);
            let key = indices.next().unwrap();
            let value = indices.next().unwrap();

            let mut entry = match groups.entry(group_key) {
                Entry::Occupied(entry) => { entry },
                Entry::Vacant(entry) => { entry.insert_entry(vec![None; new_headers.len()]) },
            };
            let vec = entry.get_mut();
            vec[new_headers[&row[key]]] = Some(row.swap_remove(value));

            if vec.iter().all(|x| x.is_some()) {
                // this group is done
                let (key, value) = entry.remove_entry();
                let row = value.into_iter().map(|x| x.unwrap()).chain(key).collect();
                if base.on_row(row)? {
                    return Ok(true)
                }
            }
        }

        // print the remaining unmatched ones
        for (key, value) in groups.into_iter() {
            let row = value.into_iter().map(|x| x.unwrap_or(b"".into())).chain(key).collect();
            if base.on_row(row)? {
                return Ok(false)
            }
        }

        base.on_eof()
    }
}

