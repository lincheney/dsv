use clap::{Parser, ArgAction};
use regex::bytes::Regex;
use once_cell::sync::Lazy;
use std::io::{IsTerminal, Read, BufRead, BufReader, Write, BufWriter};
use bstr::{BStr, BString, ByteSlice};
use std::process::{Command as ProcessCommand, Stdio};
use colorutils_rs::Hsv;

const UTF8_BOM: &[u8] = b"\xEF\xBB\xBF";
const RESET_COLOUR: &[u8] = b"\x1b[0m";
static SPACE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());
static PPRINT: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s\s+").unwrap());

fn get_rgb(i: usize, step: f32) -> BString {
    let hue = (step * i as f32) % 1.0;
    let hsv = Hsv{ h: hue * 360.0, s: 0.3, v: 1.0 };
    let rgb = hsv.to_rgb8();
    format!("\x1b[38;2;{};{};{}m", rgb.r, rgb.g, rgb.b).as_bytes().into()
}

bitflags::bitflags! {
    #[derive(Debug)]
    pub struct Callbacks: u8 {
        const None = 0;
        const ON_HEADER = 1;
        const ON_ROW = 2;
        const ON_EOF = 4;
    }
}

#[derive(Debug)]
pub enum Ifs {
    Regex(Regex),
    Plain(BString),
    Space,
    Pretty,
}

#[derive(Debug)]
pub enum Ofs {
    Plain(BString),
    Pretty,
}

impl Ofs {
    fn as_bstr(&self) -> &BStr {
        match self {
            Ofs::Pretty => b"  ".as_bstr(),
            Ofs::Plain(ofs) => ofs.as_ref(),
        }
    }
}

#[derive(Clone, PartialEq, Debug, clap::ValueEnum)]
pub enum AutoChoices {
    Never,
    Auto,
    Always,
}

impl AutoChoices {
    fn is_on(&self, is_tty: bool) -> bool {
        match self {
            Self::Always => true,
            Self::Never => false,
            Self::Auto => is_tty,
        }
    }

    fn is_on_if<F: Fn() -> bool>(&self, is_tty: F) -> bool {
        match self {
            Self::Always => true,
            Self::Never => false,
            Self::Auto => is_tty(),
        }
    }
}

#[derive(Debug, Parser, Clone)]
#[command(name = "base")]
pub struct BaseOptions {
    #[arg(global = true, short = 'H', long, action = ArgAction::SetTrue, help = "treat first row as a header")]
    pub header: Option<bool>,
    #[arg(global = true, short = 'N', long, action = ArgAction::SetTrue, overrides_with = "header", help = "do not treat first row as header")]
    pub no_header: bool,
    #[arg(global = true, long, help = "do or not print the header")]
    pub drop_header: bool,
    #[arg(global = true, long, value_enum, default_value_t = AutoChoices::Auto, help = "print a trailer")]
    pub trailer: AutoChoices,
    #[arg(global = true, long, value_enum, default_value_t = AutoChoices::Auto, help = "number the columns in the header")]
    pub numbered_columns: AutoChoices,
    #[arg(global = true, short = 'd', long, help = "input field separator")]
    pub ifs: Option<String>,
    #[arg(global = true, long, help = "treat input field separator as a literal not a regex")]
    pub plain_ifs: bool,
    #[arg(global = true, short = 'D', long, help = "output field separator")]
    pub ofs: Option<String>,
    #[arg(global = true, long, help = "input row separator")]
    pub irs: Option<String>,
    #[arg(global = true, long, help = "output row separator")]
    pub ors: Option<String>,
    #[arg(global = true, long, help = "treat input as csv", overrides_with = "ifs")]
    pub csv: bool,
    #[arg(global = true, long, help = "treat input as tsv", overrides_with = "ifs")]
    pub tsv: bool,
    #[arg(global = true, long, help = "treat input as whitespace-separated", overrides_with = "ifs")]
    pub ssv: bool,
    #[arg(global = true, long, help = "if a row has more columns than the header, combine the last ones into one, useful with --ssv")]
    pub combine_trailing_columns: bool,
    #[arg(global = true, short = 'P', long, help = "prettified output", overrides_with = "ofs")]
    pub pretty: bool,
    #[arg(global = true, long, help = "show output in a pager (less)")]
    pub page: bool,
    #[arg(global = true, long, value_enum, default_value_t = AutoChoices::Auto, help = "enable colour")]
    pub colour: AutoChoices,
    #[arg(global = true, long, help = "ansi escape code for the header")]
    pub header_colour: Option<String>,
    #[arg(global = true, long, help = "ansi escape code for the header background")]
    pub header_bg_colour: Option<String>,
    #[arg(global = true, long, value_enum, default_value_t = AutoChoices::Auto, help = "enable rainbow columns")]
    pub rainbow_columns: AutoChoices,
    #[arg(global = true, short = 'Q', long, help = "do not handle quotes from input")]
    pub no_quoting: bool,
    #[arg(global = true, long = "no-quote-output", default_value_t = true, action = ArgAction::SetFalse, help = "don't quote output")]
    pub quote_output: bool,
}

impl BaseOptions {
    pub fn post_process(&mut self) {
        let is_tty = std::io::stdout().is_terminal();

        if self.no_header {
            self.header = Some(false);
        } else if self.header == Some(false) {
            self.header = None;
        }
        if self.irs.is_none() {
            self.irs = Some("\n".into());
        }
        if self.ors.is_none() {
            self.irs = self.irs.clone();
        }
        self.colour = if self.colour.is_on(is_tty) { AutoChoices::Always } else { AutoChoices::Never };
        if std::env::var("NO_COLOR").is_ok_and(|x| !x.is_empty()) {
            self.colour = AutoChoices::Never;
        }
        self.numbered_columns = if self.numbered_columns.is_on(is_tty) { AutoChoices::Always } else { AutoChoices::Never };
        if self.header_colour.is_none() {
            self.header_colour = Some("\x1b[1;4m".into());
        }
        if self.header_bg_colour.is_none() {
            self.header_bg_colour = Some("\x1b[48;5;237m".into());
        }
        // let ors = opts.ors.as_deref().unwrap_or("\n").into();
    }
}

struct Writer {
    rgb_map: Vec<BString>,
    inner: Option<Box<dyn Write>>,
    proc: Option<std::process::Child>,
}

impl Writer {
    fn start(&mut self, opts: &BaseOptions, has_header: bool) -> &mut Box<dyn Write> {
        self.inner.get_or_insert_with(|| {
            if opts.page {
                let mut cmd = ProcessCommand::new("less");
                cmd.args(["-RX"]);
                if has_header && !opts.drop_header {
                    cmd.arg("--header=1");
                }
                let mut proc = cmd.stdin(Stdio::piped()).spawn().expect("Failed to start pager");
                let inner = Box::new(BufWriter::new(proc.stdin.take().expect("Failed to get pager stdin")));
                self.proc = Some(proc);
                inner
            } else {
                // let mut proc = ProcessCommand::new("cat")
                    // .stdin(Stdio::piped())
                    // .spawn()
                    // .expect("Failed to start output process");
                // let inner = Box::new(BufWriter::new(proc.stdin.take().expect("Failed to get output process stdin")));
                // self.proc = Some(proc);
                // inner
                Box::new(std::io::stdout().lock())
            }
        })
    }

    fn write_header(
        &mut self,
        header: Vec<BString>,
        padding: Option<&Vec<usize>>,
        opts: &BaseOptions,
        ofs: &Ofs,
        ors: &BStr,
    ) {
        if !opts.drop_header {
            self.write_output(header, padding, true, opts, ofs, ors);
        }
    }

    fn write_row(
        &mut self,
        row: Vec<BString>,
        padding: Option<&Vec<usize>>,
        opts: &BaseOptions,
        ofs: &Ofs,
        ors: &BStr,
    ) {
        self.write_output(row, padding, false, opts, ofs, ors);
    }

    fn write_output(
        &mut self,
        row: Vec<BString>,
        padding: Option<&Vec<usize>>,
        is_header: bool,
        opts: &BaseOptions,
        ofs: &Ofs,
        ors: &BStr,
    ) {
        let formatted_row = self.format_row(row, padding, is_header, opts, ofs, ors);
        let file = self.start(opts, is_header);
        file.write_all(&formatted_row).expect("Failed to write row");
        file.write_all(ors).expect("Failed to write row separator");
        file.flush().expect("Failed to flush output");
    }

    fn format_row(
        &mut self,
        row: Vec<BString>,
        padding: Option<&Vec<usize>>,
        is_header: bool,
        opts: &BaseOptions,
        ofs: &Ofs,
        ors: &BStr,
    ) -> BString {
        let colour = opts.colour == AutoChoices::Always;
        let row = self.format_columns(row, ofs, ors, opts.quote_output);

        if colour && opts.rainbow_columns == AutoChoices::Always {
            // colour each column differently
            if row.len() > self.rgb_map.len() {
                for i in self.rgb_map.len() .. row.len() {
                    self.rgb_map.push(get_rgb(i, 0.647))
                }
            }
        }

        let mut parts = BString::new(vec![]);
        let tmp_padding = vec![];
        let padding = padding.unwrap_or(&tmp_padding).iter().chain(std::iter::repeat(&0));
        let rgb = self.rgb_map.iter().map(|x| x.as_bstr()).chain(std::iter::repeat(b"".into()));
        let ofs = ofs.as_bstr();
        let header_colour = if is_header && colour {
            opts.header_colour.as_deref().map(|x| x.as_bytes())
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
                parts.extend_from_slice(RESET_COLOUR);
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
            parts.extend_from_slice(RESET_COLOUR);
        }

        parts
    }

    fn format_columns(&self, mut row: Vec<BString>, ofs: &Ofs, ors: &BStr, quote_output: bool) -> Vec<BString> {
        if quote_output {
            // if pretty output, don't allow >1 space, no matter how long the ofs is
            let pretty_output = matches!(ofs, Ofs::Pretty);
            let ofs = ofs.as_bstr();

            for col in row.iter_mut() {
                if (pretty_output && col.is_empty()) || Self::needs_quoting(col, ofs, ors) {
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

        row
    }

    fn needs_quoting(value: &[u8], ofs: &[u8], ors: &[u8]) -> bool {
        value.contains(&b'"') || value.windows(ofs.len()).any(|window| window == ofs) || value.windows(ors.len()).any(|window| window == ors)
    }
}

pub struct Base {
    writer: Writer,
    pub opts: BaseOptions,
    header: Option<Vec<BString>>,
    row_count: usize,
    col_count: Option<usize>,
    gathered_rows: Vec<Vec<BString>>,
    out_header: Option<Vec<BString>>,
    ofs: Ofs,
    ifs: Ifs,
    ors: BString,
}

pub trait Processor<T> {

    fn new(opts: T) -> Self;

    fn run(mut cli_opts: BaseOptions, opts: T) where Self: Sized {
        let mut handler = Self::new(opts);
        handler.process_opts(&mut cli_opts);
        let mut base = Base::new(cli_opts);
        handler.process_file(std::io::stdin().lock(), &mut base, Callbacks::all());
    }

    fn determine_ifs(&self, line: &BStr, opts: &BaseOptions) -> Ifs {
        if let Some(ifs) = &opts.ifs {
            if regex::escape(ifs) != *ifs && !opts.plain_ifs {
                Ifs::Regex(Regex::new(ifs).unwrap())
            } else {
                Ifs::Plain(BString::new(ifs.as_str().into()))
            }
        } else if opts.csv {
            Ifs::Plain(b",".into())
        } else if opts.tsv {
            Ifs::Plain(b"\t".into())
        } else if opts.ssv {
            Ifs::Space
        } else {
            Self::guess_delimiter(line, b"\t".into())
        }
    }

    fn determine_ofs(&self, ifs: &Ifs, opts: &BaseOptions) -> Ofs {
        if let Some(ofs) = &opts.ofs {
            return Ofs::Plain(ofs.as_bytes().into())
        }
        if opts.pretty {
            return Ofs::Pretty
        }

        match ifs {
            Ifs::Space | Ifs::Pretty => {
                if opts.colour == AutoChoices::Always {
                    Ofs::Pretty
                } else {
                    Ofs::Plain(b"    ".into())
                }
            },
            Ifs::Plain(ifs) => {
                Ofs::Plain(ifs.clone())
            }
            Ifs::Regex(_) => {
                Ofs::Plain(b"\t".into())
            },
        }
    }

    fn guess_delimiter(line: &BStr, default: &BStr) -> Ifs {
        const GOOD_DELIMS: [u8; 2] = [b'\t', b','];
        const OTHER_DELIMS: [(&[u8; 2], usize); 4] = [(b"  ", 2), (b"  ", 1), (b"| ", 1), (b"; ", 1)];

        let mut counts: [usize; GOOD_DELIMS.len()] = [0; GOOD_DELIMS.len()];
        for (delim, counts) in GOOD_DELIMS.iter().zip(counts.iter_mut()) {
            *counts = line.split(|x| x == delim).count() - 1;
        }

        if let Some((best, &count)) = counts.iter().enumerate().max_by_key(|&(_, count)| count) && count > 0 {
            let delim = GOOD_DELIMS[best];
            return Ifs::Plain(BStr::new(&[delim]).into());
        }

        let mut counts: [usize; OTHER_DELIMS.len()] = [0; OTHER_DELIMS.len()];
        for ((delim, len), counts) in OTHER_DELIMS.iter().zip(counts.iter_mut()) {
            let delim = &delim[..*len];
            *counts = line.split_str(delim).count() - 1;
        }

        if let Some((best, &count)) = counts.iter().enumerate().max_by_key(|&(_, count)| count) && count > 0 {
            return if best == 1 && 2 * counts[0] >= counts[1] {
                Ifs::Pretty
            } else if best == 1 {
                if Regex::new(r"\S \S").unwrap().is_match(line) {
                    Ifs::Space
                } else {
                    Ifs::Pretty
                }
            } else if best == 0 {
                Ifs::Pretty
            } else {
                let delim = OTHER_DELIMS[best].0;
                Ifs::Plain(delim.into())
            };
        }

        // no idea
        Ifs::Plain(default.into())
    }

    fn determine_delimiters(&self, line: &BStr, opts: &BaseOptions) -> (Ifs, Ofs) {
        let ifs = self.determine_ifs(line, opts);
        let ofs = self.determine_ofs(&ifs, opts);
        (ifs, ofs)
    }

    fn process_file<R: Read>(&mut self, file: R, base: &mut Base, do_callbacks: Callbacks) -> bool {
        let mut reader = BufReader::new(file);
        let mut buffer = BString::new(vec![]);
        let mut row = vec![];
        let mut got_row = false;
        let mut got_line = false;
        let irs: BString = base.opts.irs.as_deref().unwrap_or("\n").as_bytes().into();

        let mut eof = false;
        while !eof {
            let mut line = if irs.len() == 1 {
                reader.read_until(irs[0], &mut buffer).unwrap();
                eof = !buffer.ends_with(&irs);
                &buffer[.. buffer.len() - if eof { 0 } else { 1 }]
            } else if let Some((left, _)) = buffer.split_once_str::<BString>(&irs) {
                left
            } else {
                // read some more
                let buf = reader.fill_buf().unwrap();
                buffer.extend_from_slice(buf);
                eof = buf.is_empty();
                if !eof {
                    continue
                }
                &buffer[..]
            };

            if eof && row.is_empty() && line.is_empty() {
                break
            }

            let line_len = line.len() + irs.len();

            if ! got_line {
                got_line = true;
                if line.starts_with(UTF8_BOM) {
                    line = &line[UTF8_BOM.len()..]; // Remove UTF-8 BOM
                }
                (base.ifs, base.ofs) = self.determine_delimiters(line.into(), &base.opts);
                if matches!(base.ifs, Ifs::Space | Ifs::Pretty) {
                    base.opts.combine_trailing_columns = true;
                }
            }

            let incomplete;
            (row, incomplete) = base.parse_line(line.into(), row, b'"');
            if !incomplete || eof {
                got_row = true;

                if base.header.is_none() && base.opts.header.is_none() {
                    base.opts.header = Some(row.iter().all(|c| matches!(c.first(), Some(b'_' | b'a' ..= b'z' | b'A' ..= b'Z'))));
                }

                let is_header = base.header.is_none() && base.opts.header == Some(true);

                if is_header {
                    let header = row.clone();
                    base.header = Some(row);
                    if do_callbacks.contains(Callbacks::ON_HEADER) && self.on_header(base, header) {
                        break
                    }
                } else if do_callbacks.contains(Callbacks::ON_ROW) && self.on_row(base, row) {
                    break
                }

                // if do_yield {
                    // yield (row, is_header)
                // }

                row = vec![];
            }

            if !eof {
                buffer.drain(..line_len);
            }
        }

        if do_callbacks.contains(Callbacks::ON_EOF) {
            self.on_eof(base);
        }

        got_row
    }

    fn process_opts(&mut self, _opts: &mut BaseOptions) {
    }

    fn on_row(&mut self, base: &mut Base, row: Vec<BString>) -> bool {
        base.on_row(row)
    }

    fn on_header(&mut self, base: &mut Base, header: Vec<BString>) -> bool {
        base.on_header(header)
    }

    fn on_eof(&mut self, base: &mut Base) {
        base.on_eof()
    }

}

impl Base {

    pub fn on_eof(&mut self) {
        let mut header_padding = None;

        if !self.gathered_rows.is_empty() {
            let padding = self.justify(&self.gathered_rows);
            for (i, (p, row)) in padding.iter().zip(self.gathered_rows.drain(..)).enumerate() {
                if i == 0 && self.out_header.is_some() {
                    header_padding = Some(p.clone());
                    self.writer.write_header(row, Some(p), &self.opts, &self.ofs, self.ors.as_ref());
                } else {
                    self.writer.write_row(row, Some(p), &self.opts, &self.ofs, self.ors.as_ref());
                }
            }
        }

        if let Some(header) = self.out_header.take() && self.opts.trailer.is_on_if(|| termsize::get().is_some_and(|size| self.row_count >= size.rows as usize)) {
            self.writer.write_header(header, header_padding.as_ref(), &self.opts, &self.ofs, self.ors.as_ref());
        }
    }

    fn justify(&self, rows: &[Vec<BString>]) -> Vec<Vec<usize>> {
        let widths: Vec<Vec<_>> = rows.iter().map(|row|
            row.iter().map(|col| col.len()).collect()
        ).collect();

        let max_col = rows.iter().map(|row| row.len()).max().unwrap_or(0);

        let max_widths: Vec<_> = (0 .. max_col).map(|i|
            widths.iter().flat_map(|w| w.get(i)).max().cloned().unwrap_or(0)
        ).collect();

        // don't pad the last column
        widths.iter().map(|w|
            w.iter().zip(&max_widths).take(w.len().saturating_sub(1)).map(|(w, m)| m - w).collect()
        ).collect()
    }

    pub fn on_row(&mut self, row: Vec<BString>) -> bool {
        self._on_row(row, None, false)
    }

    fn _on_row(&mut self, row: Vec<BString>, padding: Option<&Vec<usize>>, is_header: bool) -> bool {
        if self.col_count.is_none() {
            self.col_count = Some(row.len());
            self.writer.rgb_map.clear();
            for i in 0 .. row.len() {
                self.writer.rgb_map.push(get_rgb(i, 0.647));
            }
        }

        self.row_count += 1;

        if matches!(self.ofs, Ofs::Pretty) {
            self.gathered_rows.push(row);
        } else if is_header {
            self.writer.write_header(row, padding, &self.opts, &self.ofs, self.ors.as_ref());
        } else {
            self.writer.write_row(row, padding, &self.opts, &self.ofs, self.ors.as_ref());
        }

        false
    }

    pub fn on_header(&mut self, header: Vec<BString>) -> bool {
        self._on_header(header)
    }

    fn _on_header(&mut self, mut header: Vec<BString>) -> bool {
        if self.opts.drop_header {
            false
        } else {
            if self.opts.numbered_columns == AutoChoices::Always {
                for (i, col) in header.iter_mut().enumerate() {
                    let prefix = format!("{} ", i + 1);
                    let leading_space = col.iter().take(prefix.len()).take_while(|&&x| x == b' ').count();
                    col.splice(0 .. leading_space, prefix.into_bytes());
                }
            }
            self.out_header = Some(header.clone());

            self._on_row(header, None, true)
        }
    }

    fn next_ifs(&self, line: &BStr, start: usize, ifs: &Ifs) -> Option<(usize, usize)> {
        match ifs {
            Ifs::Space => self.next_regex_ifs(line, start, &SPACE),
            Ifs::Pretty => self.next_regex_ifs(line, start, &PPRINT),
            Ifs::Regex(ifs) => self.next_regex_ifs(line, start, ifs),
            Ifs::Plain(ifs) => {
                let idx = start + line[start..].find(ifs)?;
                Some((idx, idx + ifs.len()))
            },
        }
    }

    fn next_regex_ifs(&self, line: &BStr, start: usize, ifs: &Regex) -> Option<(usize, usize)> {
        let m = ifs.find(&line[start..])?;
        Some((start + m.start(), start + m.end()))
    }

    fn parse_line(&self, line: &BStr, mut row: Vec<BString>, quote: u8) -> (Vec<BString>, bool) {
        let allow_quoted = !self.opts.no_quoting;
        let maxcols = if self.opts.combine_trailing_columns && self.header.is_some() {
            self.header.as_ref().unwrap().len()
        } else {
            usize::MAX
        };

        if !allow_quoted || !line.contains(&quote) {
            if !row.is_empty() {
                row.last_mut().unwrap().extend_from_slice(line);
                return (row, true);
            } else if let Ifs::Plain(ifs) = &self.ifs {
                let row = line.splitn_str(maxcols, ifs).map(|x| x.to_owned().into()).collect();
                return (row, false);
            } else if let Ifs::Regex(ifs) = &self.ifs {
                return (ifs.splitn(line, maxcols).map(|x| x.to_owned().into()).collect(), false);
            }
        }

        let mut start = 0;
        let line_len = line.len();

        if !row.is_empty() {
            let (value, i) = Self::extract_column(line, 0, line_len, quote);
            row.last_mut().unwrap().extend_from_slice(&value);
            if i == usize::MAX {
                return (row, true);
            }
            start = self.next_ifs(line, i + 1, &self.ifs).unwrap_or((line_len, line_len)).1;
        }

        while start < line_len {
            if allow_quoted && line[start] == quote {
                let (value, i) = Self::extract_column(line, start + 1, line_len, quote);
                if row.len() >= maxcols {
                    row.last_mut().unwrap().extend_from_slice(&value);
                } else {
                    row.push(value);
                }
                if i == usize::MAX {
                    return (row, true);
                }
                start = self.next_ifs(line, i + 1, &self.ifs).unwrap_or((line_len, line_len)).1;
            } else {
                let (s, e) = self.next_ifs(line, start, &self.ifs).unwrap_or((line_len, line_len));
                if row.len() >= maxcols {
                    row.last_mut().unwrap().extend_from_slice(&line[start..e]);
                } else {
                    row.push(line[start..s].to_owned());
                }
                if s == line_len {
                    break;
                }
                start = e;
            }
        }

        if start == line_len {
            row.push(vec![].into());
        }

        (row, false)
    }

    fn extract_column(line: &BStr, mut start: usize, line_len: usize, quote: u8) -> (BString, usize) {
        let mut value = BString::new(vec![]);
        let mut i = line[start..].find_byte(quote).map(|pos| start + pos);

        while let Some(pos) = i {
            value.extend_from_slice(&line[start..pos]);
            if pos + 1 < line_len && line[pos + 1] == quote {
                start = pos + 2;
                i = line[start..].find_byte(quote).map(|pos| start + pos);
            } else {
                return (value, pos);
            }
        }

        value.extend_from_slice(&line[start..]);
        (value, usize::MAX)
    }

    fn new(opts: BaseOptions) -> Self {
        let ors = opts.ors.as_deref().unwrap_or("\n").into();
        Self {
            opts,
            header: None,
            row_count: 0,
            col_count: None,
            ifs: Ifs::Pretty,
            ofs: Ofs::Pretty,
            ors,
            gathered_rows: vec![],
            out_header: None,
            writer: Writer {
                inner: None,
                proc: None,
                rgb_map: vec![],
            },
        }
    }

}
