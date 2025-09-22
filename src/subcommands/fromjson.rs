use anyhow::{Result, Context};
use crate::base::{self, Processor, Callbacks};
use bstr::{BStr, BString};
use std::collections::HashMap;
use std::process::ExitCode;
use std::io::Read;
use clap::Parser;
use serde_json;

#[derive(Parser, Debug)]
#[command(about = "convert from json")]
pub struct Opts {
    #[arg(short = 'f', long, num_args = 0..=1, help = "flatten objects and arrays")]
    flatten: Option<Option<String>>,
}

pub struct Handler {
    flatten: Option<String>,
}

impl Handler {
    pub fn new(opts: Opts, _base: &mut base::Base) -> Result<Self> {
        Ok(Self {
            flatten: opts.flatten.map(|f| f.unwrap_or(".".into())),
        })
    }
}

fn vec_iter<T>(vec: &[T]) -> impl Iterator<Item=(String, &T)> {
    vec.iter().enumerate().map(|(i, v)| (format!("{i}"), v))
}

fn flatten<'a, T: AsRef<[u8]>, I: Iterator<Item=(T, &'a serde_json::Value)>>(
    iter: I,
    sep: &BStr,
    parent_key: Option<&BStr>,
    result: &mut HashMap<BString, &'a serde_json::Value>,
) {

    for (k, v) in iter {
        let key: BString = if let Some(p) = parent_key {
            bstr::concat([p, sep, k.as_ref()]).into()
        } else {
            k.as_ref().into()
        };
        match v {
            serde_json::Value::Object(map) => flatten(map.iter(), sep, Some(key.as_ref()), result),
            serde_json::Value::Array(vec) => flatten(vec_iter(vec), sep, Some(key.as_ref()), result),
            v => { result.insert(key, v); },
        }
    }
}

fn flatten_to_hashmap<'a>(map: &'a serde_json::Map<String, serde_json::Value>, sep: &BStr) -> HashMap<BString, &'a serde_json::Value> {
    let mut result = HashMap::new();
    flatten(map.iter(), sep, None, &mut result);
    result
}

impl Handler {

    fn process_json_row<
        'a,
        K: 'static,
        I: Iterator<Item=&'a K>,
        F: Fn(&K) -> Option<&'a serde_json::Value>,
    >(
        &mut self,
        base: &mut base::Base,
        keys: I,
        get: F,
    ) -> Result<()> {
        let values = keys.map(|k| {
            get(k)
                .map_or_else(String::new, |v| {
                    v.as_str().map_or_else(|| v.to_string(), |s| s.to_owned())
                })
                .into()
        }).collect();
        self.on_row(base, values)
    }

    fn process_json<R: Read>(&mut self, file: R, base: &mut base::Base, do_callbacks: Callbacks) -> Result<()> {
        let mut stream = serde_json::Deserializer::from_reader(file).into_iter();

        let header_row = if let Some(header_row) = stream.next() {
            header_row.context("invalid json")?
        } else {
            return Ok(())
        };

        let do_header = do_callbacks.contains(Callbacks::ON_HEADER);
        let do_row = do_callbacks.contains(Callbacks::ON_ROW);

        if let Some(sep) = self.flatten.take() {
            let sep = sep.as_bytes().into();
            let first_row = flatten_to_hashmap(&header_row, sep);
            let header: Vec<_> = first_row.keys().cloned().collect();
            if do_header {
                self.on_header(base, header)?;
            }

            if do_row {
                self.process_json_row(base, first_row.keys(), |k| first_row.get(k).copied())?;
            }
            for row in stream {
                let row = row?;
                let row = flatten_to_hashmap(&row, sep);
                if do_row {
                    self.process_json_row(base, first_row.keys(), |k| row.get(k).copied())?;
                }
            }

        } else {
            let header: Vec<_> = header_row.keys().cloned().map(|x| x.into()).collect();
            if do_header {
                self.on_header(base, header)?;
            }
            if do_row {
                self.process_json_row(base, header_row.keys(), |k| header_row.get(k))?;
            }
            for row in stream {
                let row = row?;
                if do_row {
                    self.process_json_row(base, header_row.keys(), |k| row.get(k))?;
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
        self.process_json(file, base, do_callbacks)?;
        if do_callbacks.contains(Callbacks::ON_EOF) {
            self.on_eof(base)?;
        }
        Ok(ExitCode::SUCCESS)
    }

}
