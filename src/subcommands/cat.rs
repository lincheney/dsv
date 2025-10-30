use crate::io::{Reader};
use anyhow::Result;
use std::fs::File;
use std::io::BufReader;
use crate::base::{self, Processor};
use bstr::BString;
use clap::{Parser};

#[derive(Parser, Default, Clone)]
#[command(about = "concatenate files by row")]
pub struct Opts {
    #[arg(short = 'n', long, help = "number all output lines")]
    number: bool,
    #[arg(short = 's', long, help = "determine header after reading all input")]
    slurp: bool,
    #[arg(help = "other files to concatenate to stdin")]
    files: Vec<String>,
}

pub struct Handler {
    opts: Opts,
    row_count: usize,
    children: Option<Vec<Child>>,
    have_combined: bool,
}

impl Handler {
    pub fn new(opts: Opts, _base: &mut base::Base) -> Result<Self> {
        Ok(Self {
            row_count: 0,
            opts,
            children: None,
            have_combined: false,
        })
    }

    fn open_file(base: &mut base::Base, file: &String) -> Result<Option<BufReader<File>>> {
        match File::open(file) {
            Ok(file) => Ok(Some(BufReader::new(file))),
            Err(e) => {
                base.log(format!("{e}: {file}\n"))?;
                Ok(None)
            }
        }
    }

    fn get_children(&mut self, base: &mut base::Base) -> Result<&mut Vec<Child>> {
        let init = self.children.is_none();
        let children = self.children.get_or_insert_with(Vec::new);
        if init {
            for file in &std::mem::take(&mut self.opts.files) {
                if let Some(file) = Self::open_file(base, file)? {
                    let reader = Reader::new(file);
                    children.push(Child{
                        inner: ChildProcessor,
                        base: None,
                        reader,
                        prev_row: vec![],
                        first_row: true,
                        first_read: true,
                        mapping: vec![],
                        extra_row: None,
                    });
                }
            }
        }
        Ok(children)
    }

    fn make_combined_header(&mut self, base: &mut base::Base, row: Vec<BString>, is_header: bool) -> Result<Option<Vec<BString>>> {
        self.have_combined = true;
        let mut headers = if is_header {
            row.into_iter().map(Some).collect()
        } else {
            vec![None; row.len()]
        };

        let mut have_any_headers = is_header;
        // i got my header, get everyone elses
        for child in self.get_children(base)? {
            if let Some((row, is_header)) = child.process_one_row(base, base::Callbacks::None)? {
                if is_header {
                    have_any_headers = true;
                    for col in row {
                        if let Some(ix) = headers.iter().position(|h| h.as_ref() == Some(&col)) {
                            child.mapping.push(ix);
                        } else {
                            child.mapping.push(headers.len());
                            headers.push(Some(col));
                        }
                    }
                } else {
                    // this file has no headers
                    child.mapping.extend(headers.len() .. headers.len() + row.len());
                    headers.extend(row.iter().map(|_| None));
                    child.extra_row = Some(row);
                }
            }
        }

        if have_any_headers {
            Ok(Some(headers.into_iter().map(|x| x.unwrap_or(b"".into())).collect()))
        } else {
            self.opts.slurp = false;
            Ok(None)
        }
    }

    fn on_child_row(&mut self, base: &mut base::Base, child: &Child, row: Vec<BString>) -> Result<()> {
        if self.opts.slurp {
            let mut template = vec![b"".into(); child.mapping.iter().copied().max().unwrap_or(0) + 1];
            for (i, col) in child.mapping.iter().zip(row) {
                template[*i] = col;
            }
            self.on_row(base, template)
        } else {
            self.on_row(base, row)
        }
    }

}

impl Processor for Handler {

    fn on_header(&mut self, base: &mut base::Base, mut header: Vec<BString>) -> Result<()> {
        if !self.have_combined && self.opts.slurp {
            header = self.make_combined_header(base, header, true)?.unwrap();
        }

        if self.opts.number {
            header.insert(0, b"n".into());
        }
        base.on_header(header)
    }

    fn on_row(&mut self, base: &mut base::Base, mut row: Vec<BString>) -> Result<()> {
        if !self.have_combined && self.opts.slurp && let Some(header) = self.make_combined_header(base, row.clone(), false)? {
            self.on_header(base, header)?;
        }

        if self.opts.number {
            self.row_count += 1;
            row.insert(0, format!("{}", self.row_count).into());
        }
        base.on_row(row)
    }

    fn on_eof(mut self, base: &mut base::Base) -> Result<bool> {
        while !self.have_combined && self.opts.slurp && !self.opts.files.is_empty() {
            // we got no rows, process the next file
            let file = self.opts.files.remove(0);
            if let Some(file) = Self::open_file(base, &file)? {
                ChildProcessor.process_file(file, base, base::Callbacks::ON_HEADER | base::Callbacks::ON_ROW)?;
            }
        }

        self.get_children(base)?;
        for mut child in self.children.take().unwrap() {
            if let Some(row) = child.extra_row.take() {
                self.on_child_row(base, &child, row)?;
            }
            while let Some((row, is_header)) = child.process_one_row(base, base::Callbacks::None)? {
                if !is_header {
                    self.on_child_row(base, &child, row)?;
                }
            }
        }

        base.on_eof()
    }

}

struct Child {
    inner: ChildProcessor,
    base: Option<base::ScopelessBase>,
    reader: Reader<BufReader<File>>,
    prev_row: Vec<BString>,
    first_row: bool,
    first_read: bool,
    mapping: Vec<usize>,
    extra_row: Option<Vec<BString>>,
}

impl Child {
    fn process_one_row(&mut self, base: &mut base::Base, do_callbacks: base::Callbacks) -> Result<Option<(Vec<BString>, bool)>> {
        let prev_row = std::mem::take(&mut self.prev_row);

        // we need our own base as we might modify the opts
        let mut base = if let Some(inner) = self.base.take() {
            base::Base{ inner, scope: base.scope }
        } else {
            base.clone()
        };

        let result = self.inner.process_one_row(&mut self.reader, &mut base, do_callbacks, prev_row, self.first_row, self.first_read);
        self.base = Some(base.inner);
        self.first_row = false;
        self.first_read = false;

        if let Some((row, is_header, prev_row)) = result? {
            self.prev_row = prev_row;
            Ok(Some((row, is_header)))
        } else {
            Ok(None)
        }
    }
}

struct ChildProcessor;
impl Processor for ChildProcessor {}
