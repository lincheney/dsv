use crate::base::*;
use std::io::{Write, BufWriter};
use bstr::{BStr, BString, ByteSlice, ByteVec};
use std::process::{Command, Stdio};
use anyhow::{Result, Context};
use colorutils_rs::Hsv;

fn get_rgb(i: usize, step: f32) -> BString {
    let hue = (step * i as f32) % 1.0;
    let hsv = Hsv{ h: hue * 360.0, s: 0.3, v: 1.0 };
    let rgb = hsv.to_rgb8();
    format!("\x1b[38;2;{};{};{}m", rgb.r, rgb.g, rgb.b).as_bytes().into()
}

pub fn format_columns<S: AsRef<BStr>>(mut row: Vec<BString>, ofs: &Ofs<S>, ors: &BStr, quote_output: bool) -> FormattedRow {
    if quote_output {
        // if pretty output, don't allow >1 space, no matter how long the ofs is
        let pretty_output = matches!(ofs, Ofs::Pretty);
        let ofs = ofs.as_bstr();

        for col in &mut row {
            if (pretty_output && col.is_empty()) || needs_quoting(col, ofs, ors) {
                let mut quoted_col = vec![];
                quoted_col.push(b'"');
                for (i, part) in col.split_str(b"\"").enumerate() {
                    if i != 0 {
                        quoted_col.extend_from_slice(b"\"\"");
                    }
                    quoted_col.extend_from_slice(part);
                }
                quoted_col.push(b'"');
                *col = quoted_col.into();
            }
        }
    }

    FormattedRow(row)
}

fn needs_quoting(value: &[u8], ofs: &[u8], ors: &[u8]) -> bool {
    value.contains(&b'"') || value.windows(ofs.len()).any(|window| window == ofs) || value.windows(ors.len()).any(|window| window == ors)
}

pub struct BaseWriter {
    rgb_map: Vec<BString>,
    proc: Option<std::process::Child>,
    ors: BString,
}

pub trait Writer {

    fn new(opts: &BaseOptions) -> Self;

    fn get_ors(&self) -> &BStr;

    fn get_file(&mut self, opts: &BaseOptions, has_header: bool) -> Box<dyn Write>;

    fn get_rgb_map(&self) -> &Vec<BString>;
    fn get_rgb_map_mut(&mut self) -> &mut Vec<BString>;

    fn set_rgb(&mut self, count: usize) {
        let rgb_map = self.get_rgb_map_mut();
        for i in rgb_map.len() .. count {
            rgb_map.push(get_rgb(i, 0.647));
        }
    }

    fn format_columns(row: Vec<BString>, ofs: &Ofs, ors: &BStr, quote_output: bool) -> FormattedRow {
        format_columns(row, ofs, ors, quote_output)
    }

    fn write_header(
        &mut self,
        file: &mut Option<Box<dyn Write>>,
        header: FormattedRow,
        padding: Option<&Vec<usize>>,
        opts: &BaseOptions,
        ofs: &Ofs,
    ) -> Result<()> {
        if !opts.drop_header {
            self.write_output(file, header.0, padding, true, opts, ofs)?;
        }
        Ok(())
    }

    fn write_row(
        &mut self,
        file: &mut Option<Box<dyn Write>>,
        row: GatheredRow,
        padding: Option<&Vec<usize>>,
        opts: &BaseOptions,
        ofs: &Ofs,
    ) -> Result<()> {
        match row {
            GatheredRow::Row(row) => self.write_output(file, row.0, padding, false, opts, ofs),
            GatheredRow::Separator => self.write_separator(file, padding, opts),
        }
    }

    fn write_separator(
        &mut self,
        file: &mut Option<Box<dyn Write>>,
        _padding: Option<&Vec<usize>>,
        opts: &BaseOptions,
    ) -> Result<()> {
        let mut sep: BString;
        let sep = if opts.colour == AutoChoices::Always {
            let width = termsize::get().map_or(80, |size| size.cols) as usize;
            sep = b"\x1b[2m".into();
            sep.push_str(b"-".repeat(width));
            sep.push_str(RESET_COLOUR);
            &sep[..]
        } else {
            b"---"
        };

        self.write_raw(file, sep.into(), opts, false)
    }

    fn write_raw(
        &mut self,
        file: &mut Option<Box<dyn Write>>,
        string: &BStr,
        opts: &BaseOptions,
        is_header: bool,
    ) -> Result<()> {
        self.write_raw_with(file, opts, is_header, |file| Ok(file.write_all(string)?))
    }

    fn write_raw_with<F: Fn(&mut Box<dyn Write>) -> Result<()>>(
        &mut self,
        file: &mut Option<Box<dyn Write>>,
        opts: &BaseOptions,
        is_header: bool,
        func: F,
    ) -> Result<()> {
        let file = file.get_or_insert_with(|| self.get_file(opts, is_header));
        let ors = self.get_ors();
        func(file).context("Failed to write row")?;
        file.write_all(ors).context("Failed to write row separator")?;
        file.flush().context("Failed to flush output")?;
        Ok(())
    }

    fn write_output(
        &mut self,
        file: &mut Option<Box<dyn Write>>,
        row: Vec<BString>,
        padding: Option<&Vec<usize>>,
        is_header: bool,
        opts: &BaseOptions,
        ofs: &Ofs,
    ) -> Result<()> {
        let formatted_row = self.format_row(row, padding, is_header, opts, ofs);
        self.write_raw(file, formatted_row.as_ref(), opts, is_header)
    }

    fn format_row(
        &mut self,
        row: Vec<BString>,
        padding: Option<&Vec<usize>>,
        is_header: bool,
        opts: &BaseOptions,
        ofs: &Ofs,
    ) -> BString {
        let colour = opts.colour == AutoChoices::Always;

        if colour && opts.rainbow_columns == AutoChoices::Always {
            // colour each column differently
            self.set_rgb(row.len());
        }

        let mut parts = BString::new(vec![]);
        let tmp_padding = vec![];
        let padding = padding.unwrap_or(&tmp_padding).iter().chain(std::iter::repeat(&0));
        let rgb = self.get_rgb_map().iter().map(|x| x.as_bstr()).chain(std::iter::repeat(b"".into()));
        let ofs = ofs.as_bstr();
        let header_colour = if is_header && colour {
            opts.header_colour.as_deref().map(|x| x.as_bytes()).or(Some(b"\x1b[1;4m"))
        } else {
            None
        };
        let header_bg_colour = if is_header && colour {
            opts.header_bg_colour.as_deref().map(|x| x.as_bytes())
        } else {
            None
        };

        for (i, ((col, rgb), &pad)) in row.iter().zip(rgb).zip(padding).enumerate() {
            if i != 0 {
                if colour {
                    parts.extend_from_slice(b"\x1b[39m");
                }
                parts.extend_from_slice(ofs);
            }
            if let Some(header_colour) = header_colour {
                parts.extend_from_slice(header_colour);
            }
            if let Some(header_bg_colour) = header_bg_colour {
                parts.extend_from_slice(header_bg_colour);
            }
            if colour {
                parts.extend_from_slice(rgb);
            }
            parts.extend_from_slice(col);
            if header_bg_colour.or(header_colour).is_some() {
                parts.extend_from_slice(RESET_COLOUR.as_bytes());
                if let Some(header_bg_colour) = header_bg_colour {
                    parts.extend_from_slice(header_bg_colour);
                }
            }
            for _ in 0 .. pad {
                parts.push(b' ');
            }
        }
        // reset colour
        if colour && !parts.is_empty() {
            parts.extend_from_slice(RESET_COLOUR.as_bytes());
        }

        parts
    }
}


impl Writer for BaseWriter {
    fn new(opts: &BaseOptions) -> Self {
        Self {
            proc: None,
            rgb_map: vec![],
            ors: opts.get_ors(),
        }
    }

    fn get_ors(&self) -> &BStr {
        self.ors.as_ref()
    }

    fn get_rgb_map(&self) -> &Vec<BString> {
        &self.rgb_map
    }

    fn get_rgb_map_mut(&mut self) -> &mut Vec<BString> {
        &mut self.rgb_map
    }

    fn get_file(&mut self, opts: &BaseOptions, has_header: bool) -> Box<dyn Write> {
        if opts.page {
            let mut command = Command::new("less");
            command.args(["-RX"]);
            if has_header && !opts.drop_header {
                command.arg("--header=1");
            }
            self.pipe_to(command)
        } else {
            Box::new(std::io::stdout().lock())
        }
    }
}

impl BaseWriter {
    pub fn pipe_to(&mut self, mut command: Command) -> Box<dyn Write> {
        let mut proc = command.stdin(Stdio::piped()).spawn().expect("Failed to start pager");
        let inner = Box::new(BufWriter::new(proc.stdin.take().expect("Failed to get pager stdin")));
        self.proc = Some(proc);
        inner
    }
}
