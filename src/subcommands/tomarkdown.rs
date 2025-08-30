use anyhow::Result;
use crate::base;
use crate::writer::{Writer, BaseWriter};
use once_cell::sync::Lazy;
use regex::bytes::Regex;
use bstr::{BString, BStr, ByteSlice};
use std::process::{Command};
use std::io::Write;
use clap::Parser;

static NEEDS_ESCAPE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[`|\\]").unwrap());

#[derive(Parser)]
#[command(about = "convert to markdown table")]
pub struct Opts {
}

pub struct Handler {
    got_header: bool,
    drop_header: bool,
}

impl Handler {
    pub fn new(_opts: Opts, base: &mut base::Base, _is_tty: bool) -> Result<Self> {
        base.opts.header_colour.get_or_insert_with(|| "\x1b[1m".into());
        base.opts.trailer = base::AutoChoices::Never;
        base.opts.numbered_columns = base::AutoChoices::Never;
        let drop_header = base.opts.drop_header;
        base.opts.drop_header = false;
        base.opts.pretty = true;

        Ok(Self {
            drop_header,
            got_header: false,
        })
    }
}

impl base::Processor<MarkdownWriter> for Handler {

    fn on_header(&mut self, base: &mut base::Base, mut header: Vec<BString>) -> Result<bool> {
        self.got_header = true;
        if self.drop_header {
            for h in &mut header {
                h.clear();
            }
        }
        base.on_header(header)
    }

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        if !self.got_header && self.on_header(base, (0..row.len()).map(|_| b"".into()).collect())? {
            Ok(false)
        } else {
            base.on_row(row)
        }
    }
}

pub struct MarkdownWriter {
    inner: BaseWriter,
    ofs: base::Ofs,
}

impl Writer for MarkdownWriter {
    fn new(opts: &base::BaseOptions) -> Self {
        Self {
            inner: BaseWriter::new(opts),
            ofs: base::Ofs::Plain(b"|".into()),
        }
    }

    fn get_ors(&self) -> &BStr { self.inner.get_ors() }
    fn get_rgb_map(&self) -> &Vec<BString> { self.inner.get_rgb_map() }
    fn get_rgb_map_mut(&mut self) -> &mut Vec<BString> { self.inner.get_rgb_map_mut() }

    fn get_file(&mut self, opts: &base::BaseOptions, has_header: bool) -> Box<dyn Write> {
        if opts.page {
            let mut command = Command::new("less");
            command.args(["-RX", "--header=2"]);
            self.inner.pipe_to(command)
        } else {
            self.inner.get_file(opts, has_header)
        }
    }

    fn format_columns(mut row: Vec<BString>, _ofs: &base::Ofs, _ors: &BStr, quote_output: bool) -> base::FormattedRow {
        if quote_output {
            for col in &mut row {
                // TODO what about newlines
                if let std::borrow::Cow::Owned(new) = NEEDS_ESCAPE.replace_all(col, b"\\$0") {
                    *col = new.into();
                }
                // add spaces before and after
                col.insert(0, b' ');
                col.push(b' ');
                if col.len() < 3 {
                    col.resize(3, b' ');
                }
            }
        }
        // fence on the left and right
        row.insert(0, b"".into());
        row.push(b"".into());
        base::FormattedRow(row)
    }

    fn format_row(
        &mut self,
        row: Vec<BString>,
        padding: Option<&Vec<usize>>,
        is_header: bool,
        opts: &base::BaseOptions,
        _ofs: &base::Ofs,
    ) -> BString {
        self.inner.format_row(row, padding, is_header, opts, &self.ofs)
    }

    fn write_header(
        &mut self,
        file: &mut Option<Box<dyn Write>>,
        header: base::FormattedRow,
        padding: Option<&Vec<usize>>,
        opts: &base::BaseOptions,
        ofs: &base::Ofs,
    ) -> Result<()> {
        // write the separator
        let sep: Vec<_> = if let Some(padding) = padding {
            padding.iter().chain(std::iter::repeat(&0))
                .zip(&header.0)
                .map(|(p, h)| {
                    let mut sep: BString = b"-".repeatn(p + base::no_ansi_colour_len(h.as_ref())).into();
                    if let Some(c) = sep.get_mut(0) {
                        *c = b' ';
                    }
                    if let Some(c) = sep.last_mut() {
                        *c = b' ';
                    }
                    sep
                })
                .collect()
        } else {
            [b"" as &[u8]].into_iter()
                .chain((0..header.0.len()).skip(2).map(|_| b"---" as _))
                .chain([b"" as _])
                .map(|x| x.into())
                .collect()
        };
        self.write_output(file, header.0, padding, true, opts, ofs)?;
        self.write_output(file, sep, None, false, opts, ofs)
    }

}
