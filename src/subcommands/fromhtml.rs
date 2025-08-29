use anyhow::Result;
use crate::base;
use std::io::BufRead;
use std::process::ExitCode;
use std::collections::HashMap;
use bstr::{BString, ByteVec};
use clap::{Parser, ArgAction};
use quick_xml::{events::Event, reader::Reader, errors::SyntaxError};

#[derive(Parser, Default)]
#[command(about = "convert from html table")]
pub struct Opts {
    #[arg(long, action = ArgAction::SetTrue, help = "only allow valid table")]
    strict: bool,
    #[arg(long, action = ArgAction::SetTrue, help = "output the innerHTML of table cells, not the innerText")]
    inner_html: bool,
}

pub struct Handler {
    opts: Opts,
}

impl Handler {
    pub fn new(opts: Opts, _base: &mut base::Base, _is_tty: bool) -> Result<Self> {
        Ok(Self {
            opts,
        })
    }
}

type Rowspans = HashMap<usize, (usize, BString)>;

fn apply_rowspans(row: &mut Vec<BString>, rowspans: &Rowspans) {
    let mut i = row.len() + 1;
    while let Some((_, value)) = rowspans.get(&i) {
        row.push(value.clone());
        i += 1;
    }
}

fn decrement_rowspans(rowspans: &mut Rowspans) {
    let _ = rowspans.extract_if(|_, (span, _)| {
        *span = span.saturating_sub(1);
        *span == 0
    });
}

fn add_rowspan(rowspans: &mut Rowspans, column: usize, span: usize, value: BString) {
    rowspans.insert(column, (span, value));
}

impl base::Processor for Handler {

    fn process_file<R: BufRead>(mut self, file: R, base: &mut base::Base, do_callbacks: base::Callbacks) -> Result<ExitCode> {

        let ofs = self.determine_delimiters(b"".into(), &base.opts).1;
        if base.on_ofs(ofs) {
            return Ok(ExitCode::SUCCESS)
        }

        let mut state: Vec<BString> = vec![];
        let mut current_row: Vec<BString> = vec![];
        let mut got_header = false;
        let mut buffer = vec![];
        let mut rowspans = Rowspans::new();

        let mut reader = Reader::from_reader(file);
        let config = reader.config_mut();
        config.allow_dangling_amp = true;
        config.allow_unmatched_ends = true;
        config.check_end_names = false;
        config.expand_empty_elements = true;

        loop {
            buffer.clear();
            match reader.read_event_into(&mut buffer) {
                Ok(Event::Start(tag)) => {
                    if matches!(state.last().map(|x| x.as_slice()), Some(b"td" | b"th")) {
                        if self.opts.inner_html && let Some(last) = current_row.last_mut() {
                            last.push(b'<');
                            last.push_str(&*tag);
                            last.push(b'>');
                        }
                        continue
                    }

                    let name = tag.local_name();
                    let name = name.as_ref();
                    match (state.last().map(|x| x.as_slice()), name) {
                        (None, b"table" | b"thead" | b"tbody")
                        | (Some(b"table"), b"thead" | b"tbody" | b"tr")
                        | (Some(b"thead" | b"tbody"), b"tr")
                        | (Some(b"tr"), b"th" | b"td")
                        => {
                            state.push(name.into());
                            match name {
                                // good
                                b"tr" => {
                                    // new row
                                    current_row.clear();
                                    decrement_rowspans(&mut rowspans);
                                    apply_rowspans(&mut current_row, &rowspans);
                                },
                                b"td" | b"th" => {
                                    apply_rowspans(&mut current_row, &rowspans);
                                    // new column
                                    current_row.push(b"".into());
                                    for attr in tag.html_attributes().with_checks(false) {
                                        let attr = attr?;
                                        if attr.key.0 == b"rowspan" {
                                            if let Ok(Ok(span)) = std::str::from_utf8(&attr.value).map(str::parse::<usize>) && span > 0 {
                                                add_rowspan(&mut rowspans, current_row.len(), span, b"".into());
                                            } else {
                                                eprintln!("invalid rowspan {:?}", attr.value);
                                            }
                                        }
                                    }
                                },
                                _ => {
                                    // new table
                                    rowspans.clear();
                                }
                            }
                        },
                        _ => {
                            // bad
                            if self.opts.strict {
                                anyhow::bail!("invalid tags {:?}", state)
                            }
                        },
                    }

                },
                Ok(Event::End(tag)) => {
                    let name = tag.local_name();
                    let name = name.as_ref();
                    match state.last().map(|x| x.as_slice()) {
                        Some(x @ (b"th" | b"td")) if x != name && self.opts.inner_html => {
                            if let Some(last) = current_row.last_mut() {
                                last.push_str(b"</");
                                last.push_str(&*tag);
                                last.push(b'>');
                            }
                        },
                        _ => (),
                    }
                    let had_tr = state.iter().any(|x| x == b"tr");
                    let had_thead = state.iter().any(|x| x == b"thead");

                    if let Some(pos) = state.iter().rposition(|t| t == name) {
                        state.drain(pos..);
                    }

                    if had_tr && !state.iter().any(|x| x == b"tr") {
                        if had_thead && got_header {
                            eprintln!("got duplicate html table header");
                        } else if had_thead && do_callbacks.contains(base::Callbacks::ON_HEADER) {
                            self.on_header(base, current_row.clone())?;
                        } else if !had_thead && do_callbacks.contains(base::Callbacks::ON_HEADER) {
                            self.on_row(base, current_row.clone())?;
                        }
                        got_header = had_thead;
                    }
                },
                Ok(Event::Text(text)) => {
                    let row_len = current_row.len();
                    if matches!(state.last().map(|x| x.as_slice()), Some(b"td" | b"th")) && let Some(last) = current_row.last_mut() {
                        let text = text.into_inner();
                        let len = text.len();
                        last.push_str(text);
                        if let Some((_, value)) = rowspans.get_mut(&row_len) {
                            value.push_str(&last[last.len() - len ..]);
                        }
                    }
                },
                Ok(Event::Eof) => break,
                Err(quick_xml::errors::Error::Syntax(SyntaxError::UnclosedComment | SyntaxError::UnclosedPIOrXmlDecl | SyntaxError::UnclosedDoctype | SyntaxError::UnclosedCData | SyntaxError::UnclosedTag)) => (),

                Ok(_) => (),
                Err(e) => Err(e)?,
            }
        }
        if do_callbacks.contains(base::Callbacks::ON_EOF) {
            self.on_eof(base)?;
        }
        Ok(ExitCode::SUCCESS)
    }
}
