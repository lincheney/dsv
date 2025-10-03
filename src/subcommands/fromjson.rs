use crate::utils::Break;
use anyhow::{Result, Context};
use crate::base::{self, Processor, Callbacks};
use bstr::{BStr, BString};
use std::process::ExitCode;
use std::io::Read;
use clap::Parser;
use serde_json;
use indexmap::{IndexMap, IndexSet};

#[derive(Parser, Debug)]
#[command(about = "convert from json")]
pub struct Opts {
    #[arg(short = 'f', long, num_args = 0..=1, help = "flatten objects and arrays")]
    flatten: Option<Option<String>>,
    #[arg(short = 's', long, help = "determine header after reading all input")]
    slurp: bool,
}

pub struct Handler {
    flatten: Option<String>,
    slurp: bool,
}

impl Handler {
    pub fn new(opts: Opts, _base: &mut base::Base) -> Result<Self> {
        Ok(Self {
            flatten: opts.flatten.map(|f| f.unwrap_or(".".into())),
            slurp: opts.slurp,
        })
    }
}

fn vec_iter<T>(vec: Vec<T>) -> impl Iterator<Item=(String, T)> {
    vec.into_iter().enumerate().map(|(i, v)| (format!("{i}"), v))
}

fn flatten<T: AsRef<[u8]>, I: Iterator<Item=(T, serde_json::Value)>>(
    iter: I,
    sep: &BStr,
    parent_key: Option<&BStr>,
    result: &mut IndexMap<BString, serde_json::Value>,
) {

    for (k, v) in iter {
        let key: BString = if let Some(p) = parent_key {
            bstr::concat([p, sep, k.as_ref()]).into()
        } else {
            k.as_ref().into()
        };
        match v {
            serde_json::Value::Object(map) => flatten(map.into_iter(), sep, Some(key.as_ref()), result),
            serde_json::Value::Array(vec) => flatten(vec_iter(vec), sep, Some(key.as_ref()), result),
            v => { result.insert(key, v); },
        }
    }
}

fn flatten_to_hashmap(map: serde_json::Map<String, serde_json::Value>, sep: &BStr) -> IndexMap<BString, serde_json::Value> {
    let mut result = IndexMap::new();
    flatten(map.into_iter(), sep, None, &mut result);
    result
}

impl Handler {

    fn process_json_row(
        &mut self,
        base: &mut base::Base,
        header: &IndexSet<&BStr>,
        row: &IndexMap<BString, serde_json::Value>
    ) -> Result<()> {
        let values = header.iter().map(|k| {
            row.get(*k)
                .map_or_else(String::new, |v| {
                    v.as_str().map_or_else(|| v.to_string(), |s| s.to_owned())
                })
                .into()
        }).collect();
        self.on_row(base, values)
    }

    fn calc_header<'a, I: IntoIterator<Item=&'a IndexMap<BString, T>>, T: 'a>(rows: I) -> IndexSet<&'a BStr> {
        rows.into_iter().flat_map(|row| row.keys()).map(|col| col.as_ref()).collect()
    }

    fn process_json<R: Read>(&mut self, file: R, base: &mut base::Base, do_callbacks: Callbacks) -> Result<()> {
        let sep = self.flatten.take();
        let mut stream = serde_json::Deserializer::from_reader(file)
            .into_iter()
            .map(|row| {
                row.map(|row| {
                    if let Some(sep) = sep.as_ref() {
                        flatten_to_hashmap(row, sep.as_bytes().into())
                    } else {
                        row.into_iter().map(|(k, v)| (k.into(), v)).collect()
                    }
                }).context("invalid json")
            });

        let do_header = do_callbacks.contains(Callbacks::ON_HEADER);
        let do_row = do_callbacks.contains(Callbacks::ON_ROW);
        let first_row = stream.next().ok_or(Break)??;

        if self.slurp {
            let mut rows = vec![first_row];
            for row in stream {
                rows.push(row?);
            }
            let header = Self::calc_header(rows.iter());
            if do_header {
                self.on_header(base, header.iter().map(|x| (*x).to_owned()).collect())?;
            }
            if do_row {
                for row in &rows {
                    self.process_json_row(base, &header, row)?;
                }
            }

        } else {
            // get the header only from the first row
            let header = Self::calc_header([&first_row]);
            if do_header {
                self.on_header(base, header.iter().map(|x| (*x).to_owned()).collect())?;
            }
            if do_row {
                self.process_json_row(base, &header, &first_row)?;
            }
            for row in stream {
                let row = row?;
                if do_row {
                    self.process_json_row(base, &header, &row)?;
                }
            }
        }

        Ok(())
    }
}

impl base::Processor for Handler {
    fn process_file<R: Read>(mut self, file: R, base: &mut base::Base, do_callbacks: Callbacks) -> anyhow::Result<ExitCode> {
        let ofs = self.determine_delimiters(b"".into(), &base.opts).1;
        base.on_ofs(ofs)?;
        // silence the break
        Break::is_break(self.process_json(file, base, do_callbacks))?;
        if do_callbacks.contains(Callbacks::ON_EOF) {
            return self.on_eof_detailed(base)
        }
        Ok(ExitCode::SUCCESS)
    }

}
