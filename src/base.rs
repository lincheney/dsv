use std::sync::mpsc::{Sender, Receiver};
use clap::{Parser, ArgAction};
use regex::bytes::Regex;
use once_cell::sync::Lazy;
use crate::writer::{BaseWriter, Writer, WriterState};
use std::io::{BufRead, IsTerminal};
use bstr::{BStr, BString, ByteSlice};
use std::process::{ExitCode};
use crate::utils::{Break, MaybeBreak};
use anyhow::{Result};
use crate::io::{Reader};

const UTF8_BOM: &[u8] = b"\xEF\xBB\xBF";
pub const RESET_COLOUR: &str = "\x1b[0m";
static SPACE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());
static PPRINT: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s\s+").unwrap());
static ANSI: Lazy<Regex> = Lazy::new(|| Regex::new(r"\x1b\[[0-9;:]*[mK]|\x1b]8;;.*?\x1b\\").unwrap());
static NON_PRINTABLE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[^ -~]").unwrap());

bitflags::bitflags! {
    #[derive(Debug, Copy, Clone)]
    pub struct Callbacks: u8 {
        const None = 0;
        const ON_HEADER = 1;
        const ON_ROW = 2;
        const ON_EOF = 4;
        const ON_OFS = 8;
    }
}

#[derive(Debug)]
pub enum Message {
    Header(Vec<BString>),
    Row(Vec<BString>),
    Separator,
    Eof,
    Raw(BString, bool, bool),
    Ofs(Ofs),
    Stderr(Vec<BString>),
    RawStderr(BString, bool, bool),
}

pub fn no_ansi_colour_len(val: &BStr) -> usize {
    ANSI.split(val).map(|x| x.len()).sum()
}

#[derive(Debug, Clone)]
pub enum Ifs {
    Regex(Regex),
    Plain(BString),
    Space,
    Pretty,
}

#[derive(Debug, Clone, Default)]
pub enum Ofs<S=BString> {
    Plain(S),
    #[default]
    Pretty,
}

impl<S: AsRef<BStr>> Ofs<S> {
    pub fn as_bstr(&self) -> &BStr {
        match self {
            Ofs::Pretty => b"  ".as_bstr(),
            Ofs::Plain(ofs) => ofs.as_ref(),
        }
    }
}

#[derive(Copy, Clone, PartialEq, Debug, clap::ValueEnum, Default)]
pub enum AutoChoices {
    #[default]
    Never,
    Auto,
    Always,
}

impl AutoChoices {
    pub fn resolve(self, is_tty: bool) -> Self {
        if self.is_on(is_tty) {
            Self::Always
        } else {
            Self::Never
        }
    }

    pub fn resolve_with<F: Fn() -> bool>(self, is_tty: F) -> Self {
        if self.is_on_if(is_tty) {
            Self::Always
        } else {
            Self::Never
        }
    }

    pub fn is_on(self, is_tty: bool) -> bool {
        match self {
            Self::Always => true,
            Self::Never => false,
            Self::Auto => is_tty,
        }
    }

    pub fn is_on_if<F: Fn() -> bool>(self, is_tty: F) -> bool {
        match self {
            Self::Always => true,
            Self::Never => false,
            Self::Auto => is_tty(),
        }
    }

    pub fn from_option(val: Option<Option<Self>>) -> Option<Self> {
        Some(val?.unwrap_or(AutoChoices::Always))
    }

    pub fn from_option_auto(val: Option<Option<Self>>) -> Self {
        Self::from_option(val).unwrap_or(Self::Auto)
    }

}

#[derive(Debug, Parser, Clone, Default)]
#[command(name = "base")]
pub struct BaseOptions {
    #[arg(global = true, short = 'H', long, action = ArgAction::SetTrue, help = "treat first row as a header")]
    pub header: Option<bool>,
    #[arg(global = true, short = 'N', long, action = ArgAction::SetTrue, overrides_with = "header", help = "do not treat first row as header")]
    pub no_header: bool,
    #[arg(global = true, long, help = "do or not print the header")]
    pub drop_header: bool,
    #[arg(global = true, long, value_enum, num_args = 0..=1, require_equals = true, help = "print a trailer")]
    pub trailer: Option<Option<AutoChoices>>,
    #[arg(global = true, long, value_enum, num_args = 0..=1, require_equals = true, help = "number the columns in the header")]
    pub numbered_columns: Option<Option<AutoChoices>>,
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
    #[arg(global = true, long, alias = "color", value_enum, num_args = 0..=1, require_equals = true, help = "enable colour")]
    pub colour: Option<Option<AutoChoices>>,
    #[arg(global = true, long, help = "ansi escape code for the header")]
    pub header_colour: Option<String>,
    #[arg(global = true, long, help = "ansi escape code for the header background")]
    pub header_bg_colour: Option<String>,
    #[arg(global = true, long, value_enum, num_args = 0..=1, require_equals = true, help = "enable rainbow columns")]
    pub rainbow_columns: Option<Option<AutoChoices>>,
    #[arg(global = true, long, value_enum, num_args = 0..=1, require_equals = true, help = "enable hyperlink columns")]
    pub hyperlink_columns: Option<Option<AutoChoices>>,
    #[arg(global = true, short = 'Q', long, help = "do not handle quotes from input")]
    pub no_quoting: bool,
    #[arg(global = true, long = "no-quote-output", default_value_t = true, action = ArgAction::SetFalse, help = "don't quote output")]
    pub quote_output: bool,

    #[clap(skip)]
    pub inner: BaseOptionsInner,
}

#[derive(Debug, Clone, Default)]
pub struct BaseOptionsInner {
    pub trailer: AutoChoices,
    pub numbered_columns: AutoChoices,
    pub colour: AutoChoices,
    pub hyperlink_columns: AutoChoices,
    pub rainbow_columns: AutoChoices,
    pub is_stdout_tty: bool,
    pub is_stderr_tty: bool,
    pub stderr_colour: bool,
}

impl BaseOptions {
    pub fn post_process(&mut self, is_stdout_tty: Option<bool>) {
        self.inner.is_stdout_tty = is_stdout_tty.unwrap_or_else(|| std::io::stdout().is_terminal());
        self.inner.is_stderr_tty = std::io::stderr().is_terminal();

        if self.no_header {
            self.header = Some(false);
        } else if self.header == Some(false) {
            self.header = None;
        }
        if self.irs.is_none() {
            self.irs = Some("\n".into());
        }
        if self.ors.is_none() {
            self.ors = self.irs.clone();
        }
        if std::env::var("NO_COLOR").is_ok_and(|x| !x.is_empty()) {
            self.inner.colour = AutoChoices::Never;
            self.inner.stderr_colour = false;
        } else {
            let colour = AutoChoices::from_option_auto(self.colour);
            self.inner.colour = colour.resolve(self.inner.is_stdout_tty);
            self.inner.stderr_colour = !self.page && colour.is_on(self.inner.is_stderr_tty);
        }
        if self.header_bg_colour.is_none() {
            self.header_bg_colour = Some("\x1b[48;5;237m".into());
        }
        self.inner.numbered_columns = AutoChoices::from_option_auto(self.numbered_columns);
        self.inner.trailer = AutoChoices::from_option_auto(self.trailer);
        self.inner.rainbow_columns = AutoChoices::from_option_auto(self.rainbow_columns);
        // hyperlinks are off by default right now
        self.inner.hyperlink_columns = AutoChoices::from_option(self.hyperlink_columns).unwrap_or(AutoChoices::Never);
        // let ors = opts.ors.as_deref().unwrap_or("\n").into();
    }

    pub fn get_ors(&self) -> BString {
        crate::utils::unescape_str(self.ors.as_deref().unwrap_or("\n")).into_owned()
    }
}

#[derive(Debug, Clone)]
pub struct FormattedRow(pub Vec<BString>);

#[derive(Debug)]
pub enum GatheredRow {
    Row(FormattedRow),
    Stderr(FormattedRow),
    Separator,
}

pub trait Processor<W: Writer + Send + 'static=BaseWriter> {

    fn make_writer(&self, opts: BaseOptions) -> Output::<W> {
        Output::new(opts)
    }

    fn run(self, base: &mut Base, receiver: Receiver<Message>) -> Result<ExitCode> where Self: Sized {
        self.register_cleanup();
        let mut writer = self.make_writer(base.opts.clone());
        base.scope.spawn(move || {
            writer.run(receiver)
        });
        self.process_file(std::io::stdin().lock(), base, Callbacks::all())
    }

    fn determine_ifs(&self, line: &BStr, opts: &BaseOptions) -> Ifs {
        if let Some(ifs) = &opts.ifs {
            if regex::escape(ifs) != *ifs && !opts.plain_ifs {
                Ifs::Regex(Regex::new(ifs).unwrap())
            } else {
                Ifs::Plain(crate::utils::unescape_str(ifs).into_owned())
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
            return Ofs::Plain(crate::utils::unescape_str(ofs).into_owned())
        }
        if opts.pretty {
            return Ofs::Pretty
        }

        match ifs {
            Ifs::Space | Ifs::Pretty => {
                if opts.inner.colour == AutoChoices::Always {
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
        const OTHER_DELIMS: [&str; 4] = ["  ", " ", "|", ";"];

        let mut counts: [usize; GOOD_DELIMS.len()] = [0; GOOD_DELIMS.len()];
        for (delim, counts) in GOOD_DELIMS.iter().zip(counts.iter_mut()) {
            *counts = line.split(|x| x == delim).count() - 1;
        }

        if let Some((best, &count)) = counts.iter().enumerate().max_by_key(|&(_, count)| count) && count > 0 {
            let delim = GOOD_DELIMS[best];
            return Ifs::Plain(BStr::new(&[delim]).into());
        }

        let mut counts: [usize; OTHER_DELIMS.len()] = [0; OTHER_DELIMS.len()];
        for (delim, counts) in OTHER_DELIMS.iter().zip(counts.iter_mut()) {
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
                let delim = OTHER_DELIMS[best];
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

    fn process_file<R: BufRead>(mut self, file: R, base: &mut Base, do_callbacks: Callbacks) -> Result<ExitCode> where Self: Sized {

        let result = (|| {
            let mut reader = Reader::new(file);
            let mut prev_row = vec![];
            let mut first_row = true;
            let mut first_read = true;

            while !reader.is_eof {
                reader.read()?;

                let mut lines = reader.line_reader();
                while let Some((mut line, last_line)) = lines.get_line(base.irs.as_ref()) {

                    if first_read {
                        first_read = false;
                        // Remove UTF-8 BOM
                        line = line.strip_prefix(UTF8_BOM).unwrap_or(line).into();

                        let (ifs, ofs) = self.determine_delimiters(line, &base.opts);
                        base.ifs = ifs;
                        if do_callbacks.contains(Callbacks::ON_OFS) && self.on_ofs(base, ofs).is_err() {
                            break
                        }
                        if matches!(base.ifs, Ifs::Space | Ifs::Pretty) {
                            base.opts.combine_trailing_columns = true;
                        }
                    }

                    let (row, incomplete) = self.parse_line(base, line, prev_row, b'"');
                    if !incomplete || (lines.is_eof() && last_line) {

                        let is_header = if first_row {
                            // got the first row, is it a header
                            first_row = true;
                            base.opts.header.unwrap_or_else(|| row.iter().all(|c| matches!(c.first(), Some(b'_' | b'a' ..= b'z' | b'A' ..= b'Z'))))
                        } else {
                            false
                        };

                        if is_header {
                            base.header_len = Some(row.len());
                            if do_callbacks.contains(Callbacks::ON_HEADER) {
                                self.on_header(base, row)?;
                            }
                        } else if do_callbacks.contains(Callbacks::ON_ROW) {
                            self.on_row(base, row)?;
                        }
                        prev_row = vec![];
                    } else {
                        prev_row = row;
                    }
                }
            }

            Ok(())
        })();

        crate::utils::chain_errors(
            [
                do_callbacks.contains(Callbacks::ON_EOF).then(|| self.on_eof_detailed(base)),
                Some(result.and(Ok(ExitCode::SUCCESS)))
            ].into_iter().flatten()
        )
    }

    fn forward_messages(mut self, base: &mut Base, receiver: Receiver<Message>) -> Result<ExitCode> where Self: Sized {
        let mut err = Ok(());
        for msg in &receiver {
            let result = match msg {
                Message::Row(row) => self.on_row(base, row),
                Message::Header(header) => self.on_header(base, header),
                Message::Eof => Break.to_err(),
                Message::Separator => Ok(()), // do nothing
                Message::Raw(value, ors, clear) => Break::when(base.write_raw(value, ors, clear).is_err()),
                Message::Ofs(ofs) => Break::when(self.on_ofs(base, ofs).is_err()),
                Message::Stderr(row) => Break::when(base.write_stderr(row).is_err()),
                Message::RawStderr(value, ors, clear) => Break::when(base.write_raw_stderr(value, ors, clear).is_err()),
            };
            match Break::is_break(result) {
                Ok(true) => break,
                Err(e) => { err = Err(e); break; },
                _ => {},
            }
        }
        crate::utils::chain_errors([
            err.and(Ok(ExitCode::SUCCESS)),
            self.on_eof_detailed(base),
        ])
    }

    fn parse_line(&self, base: &mut Base, line: &BStr, row: Vec<BString>, quote: u8) -> (Vec<BString>, bool) {
        base.parse_line(line, row, quote)
    }

    fn register_cleanup(&self) {
    }

    fn on_row(&mut self, base: &mut Base, row: Vec<BString>) -> Result<()> {
        base.on_row(row)
    }

    fn on_header(&mut self, base: &mut Base, header: Vec<BString>) -> Result<()> {
        base.on_header(header)
    }

    fn on_eof(self, base: &mut Base) -> Result<bool> where Self: Sized {
        base.on_eof()
    }

    fn on_eof_detailed(self, base: &mut Base) -> Result<ExitCode> where Self: Sized {
        self.on_eof(base).map(|success| if success { ExitCode::SUCCESS } else { ExitCode::FAILURE })
    }

    fn on_ofs(&mut self, base: &mut Base, ofs: Ofs) -> MaybeBreak {
        base.on_ofs(ofs)
    }
}

pub struct DefaultProcessor{}
impl Processor for DefaultProcessor{}

#[derive(Clone)]
pub struct Base<'a, 'b> {
    pub sender: Sender<Message>,
    pub opts: BaseOptions,
    header_len: Option<usize>,
    pub ifs: Ifs,
    pub irs: BString,
    pub scope: &'a std::thread::Scope<'a, 'b>,
}

impl<'a, 'b> Base<'a, 'b> {

    pub fn new(opts: BaseOptions, sender: Sender<Message>, scope: &'a std::thread::Scope<'a, 'b>) -> Self {
        Self {
            sender,
            header_len: None,
            ifs: Ifs::Pretty,
            irs: crate::utils::unescape_str(opts.irs.as_deref().unwrap_or("\n")).into_owned(),
            opts,
            scope,
        }
    }

    fn next_ifs(line: &BStr, start: usize, ifs: &Ifs) -> Option<(usize, usize)> {
        match ifs {
            Ifs::Space => Self::next_regex_ifs(line, start, &SPACE),
            Ifs::Pretty => Self::next_regex_ifs(line, start, &PPRINT),
            Ifs::Regex(ifs) => Self::next_regex_ifs(line, start, ifs),
            Ifs::Plain(ifs) => {
                let idx = start + line[start..].find(ifs)?;
                Some((idx, idx + ifs.len()))
            },
        }
    }

    fn next_regex_ifs(line: &BStr, start: usize, ifs: &Regex) -> Option<(usize, usize)> {
        let m = ifs.find(&line[start..])?;
        Some((start + m.start(), start + m.end()))
    }

    fn parse_line(&self, line: &BStr, mut row: Vec<BString>, quote: u8) -> (Vec<BString>, bool) {
        let allow_quoted = !self.opts.no_quoting;
        let maxcols = if self.opts.combine_trailing_columns && let Some(header_len) = self.header_len {
            Some(header_len)
        } else {
            None
        };

        if !allow_quoted || !line.contains(&quote) {
            if let Some(last) = row.last_mut() {
                last.extend_from_slice(line);
                return (row, true);
            } else if let Ifs::Plain(ifs) = &self.ifs {
                let row = if let Some(maxcols) = maxcols {
                    line.splitn_str(maxcols, ifs).map(|x| x.into()).collect()
                } else {
                    line.split_str(ifs).map(|x| x.into()).collect()
                };
                return (row, false);
            } else if let Ifs::Regex(ifs) = &self.ifs {
                let row = if let Some(maxcols) = maxcols {
                    ifs.splitn(line, maxcols).map(|x| x.into()).collect()
                } else {
                    ifs.split(line).map(|x| x.into()).collect()
                };
                return (row, false);
            }
        }

        let mut start = Some(0);

        if let Some(last) = row.last_mut() {
            let (value, i) = Self::extract_column(line, 0, quote);
            last.extend_from_slice(&value);
            if let Some(i) = i {
                start = Self::next_ifs(line, i + 1, &self.ifs).unzip().1;
            } else {
                return (row, true);
            }
        }

        while let Some(s) = start && let Some(&c) = line.get(s) {

            let row_full = maxcols.is_some_and(|m| row.len() >= m);

            if allow_quoted && c == quote {
                let (value, i) = Self::extract_column(line, s + 1, quote);
                if row_full {
                    row.last_mut().unwrap().extend_from_slice(&value);
                } else {
                    row.push(value);
                }
                if let Some(i) = i {
                    start = Self::next_ifs(line, i + 1, &self.ifs).unzip().1;
                } else {
                    return (row, true);
                }
            } else {
                let se = Self::next_ifs(line, s, &self.ifs).unzip();
                if row_full {
                    row.last_mut().unwrap().extend_from_slice(&line[s..se.1.unwrap_or(line.len())]);
                } else {
                    row.push(line[s..se.0.unwrap_or(line.len())].to_owned());
                }
                start = se.0.zip(se.1).map(|(s, e)| e.max(s+1));
            }
        }

        if start.is_some() {
            row.push(vec![].into());
        }

        (row, false)
    }

    fn extract_column(line: &BStr, mut start: usize, quote: u8) -> (BString, Option<usize>) {
        let mut value = BString::new(vec![]);

        // find the next quote
        while let Some(pos) = line[start..].find_byte(quote).map(|pos| start + pos) {
            value.extend_from_slice(&line[start..pos]);
            // is next char also a quote
            if let Some(&c) = line.get(pos + 1) && c == quote {
                value.push(quote);
                start = pos + 2;
            } else {
                return (value, Some(pos));
            }
        }

        value.extend_from_slice(&line[start..]);
        (value, None)
    }

    pub fn on_eof(&self) -> Result<bool> {
        Ok(self.sender.send(Message::Eof).is_ok())
    }

    pub fn on_separator(&self) -> MaybeBreak {
        Break::when(self.sender.send(Message::Separator).is_err())
    }

    pub fn on_row(&self, row: Vec<BString>) -> Result<()> {
        Break::when(self.sender.send(Message::Row(row)).is_err())
    }

    pub fn on_header(&self, header: Vec<BString>) -> Result<()> {
        Break::when(self.sender.send(Message::Header(header)).is_err())
    }

    pub fn write_raw(&self, value: BString, ors: bool, clear: bool) -> MaybeBreak {
        Break::when(self.sender.send(Message::Raw(value, ors, clear)).is_err())
    }

    pub fn on_ofs(&self, ofs: Ofs) -> MaybeBreak {
        Break::when(self.sender.send(Message::Ofs(ofs)).is_err())
    }

    pub fn write_stderr(&self, row: Vec<BString>) -> MaybeBreak {
        Break::when(self.sender.send(Message::Stderr(row)).is_err())
    }

    pub fn write_raw_stderr(&self, value: BString, ors: bool, clear: bool) -> MaybeBreak {
        Break::when(self.sender.send(Message::RawStderr(value, ors, clear)).is_err())
    }

}

pub struct Output<W: Writer=BaseWriter> {
    opts: BaseOptions,
    writer: W,
    row_count: usize,
    col_count: Option<usize>,
    gathered_header: Option<FormattedRow>,
    gathered_rows: Vec<GatheredRow>,
    pub ofs: Ofs,
}

impl<W: Writer> Output<W> {

    pub fn new(opts: BaseOptions) -> Self {
        let writer = W::new(&opts);
        Self {
            opts,
            writer,
            row_count: 0,
            col_count: None,
            gathered_header: None,
            gathered_rows: vec![],
            ofs: Ofs::Pretty,
        }
    }

    fn justify(header: Option<&FormattedRow>, rows: &[GatheredRow]) -> Vec<Vec<usize>> {
        fn row_filter_fn<'a>(row: &'a GatheredRow, empty_vec: &'a FormattedRow) -> &'a FormattedRow {
            match row {
                GatheredRow::Row(row) | GatheredRow::Stderr(row) => row,
                GatheredRow::Separator => empty_vec,
            }
        }
        let empty_vec = FormattedRow(vec![]);
        let row_filter = |row| row_filter_fn(row, &empty_vec);

        let widths: Vec<Vec<_>> = header.into_iter()
            .chain(rows.iter().map(row_filter))
            .map(|row| row.0.iter().map(|col| no_ansi_colour_len(col.as_ref())).collect())
            .collect();

        let max_col = rows.iter().map(|row| row_filter(row).0.len()).max().unwrap_or(0);
        let max_col = max_col.max(header.map_or(0, |h| h.0.len()));

        let max_widths: Vec<_> = (0 .. max_col).map(|i|
            widths.iter().filter_map(|w| w.get(i)).max().copied().unwrap_or(0)
        ).collect();

        // don't pad the last column
        widths.iter()
            .map(|w| w.iter().zip(&max_widths).take(w.len().saturating_sub(1)).map(|(w, m)| m - w).collect())
            .collect()
    }


    fn on_eof(&mut self, state: &mut WriterState) -> Result<()> {
        let mut header_padding = None;
        let trailer = if let Some(header) = &self.gathered_header && self.opts.inner.trailer.is_on_if(|| termsize::get().is_some_and(|size| self.row_count >= size.rows as usize)) {
            Some(header.clone())
        } else {
            None
        };

        if matches!(self.ofs, Ofs::Pretty) && (self.gathered_header.is_some() || !self.gathered_rows.is_empty()) {
            let padding = Self::justify(self.gathered_header.as_ref(), &self.gathered_rows);

            let padding = if let Some(header) = self.gathered_header.take() {
                let (first, new_padding) = padding.split_first().unwrap();
                header_padding = Some(first.clone());
                self.writer.write_header(state, header, header_padding.as_ref(), &self.opts, &self.ofs)?;
                new_padding
            } else {
                &padding[..]
            };

            for (p, row) in padding.iter().zip(self.gathered_rows.drain(..)) {
                self.writer.write_row(state, row, Some(p), &self.opts, &self.ofs)?;
            }
        }

        if let Some(trailer) = trailer {
            self.writer.write_header(state, trailer, header_padding.as_ref(), &self.opts, &self.ofs)?;
        }
        Ok(())
    }

    fn on_separator(&mut self, state: &mut WriterState) -> Result<()> {
        self.row_count += 1;

        if matches!(self.ofs, Ofs::Pretty) {
            self.gathered_rows.push(GatheredRow::Separator);
        } else {
            self.writer.write_row(state, GatheredRow::Separator, None, &self.opts, &self.ofs)?;
        }

        Ok(())
    }

    fn on_row(&mut self, state: &mut WriterState, row: Vec<BString>, is_header: bool, stderr: bool) -> Result<()> {
        if self.col_count.is_none() {
            self.col_count = Some(row.len());
        }

        self.row_count += 1;

        let row = W::format_columns(row, &self.ofs, state.ors.as_ref(), self.opts.quote_output);

        match &self.ofs {
            Ofs::Pretty => if is_header {
                self.gathered_header = Some(row);
            } else if stderr {
                self.gathered_rows.push(GatheredRow::Stderr(row));
            } else {
                self.gathered_rows.push(GatheredRow::Row(row));
            },
            Ofs::Plain(_) => if is_header {
                self.gathered_header = Some(row.clone());
                self.writer.write_header(state, row, None, &self.opts, &self.ofs)?;
            } else {
                self.writer.write_row(state, GatheredRow::Row(row), None, &self.opts, &self.ofs)?;
            },
        }

        Ok(())
    }

    fn on_header(&mut self, state: &mut WriterState, mut header: Vec<BString>) -> Result<()> {
        if let Some(hyperlinks) = state.hyperlinks.as_mut() {
            // the parameters and the URI must not contain any bytes outside of the 32-126
            hyperlinks.1 = header.iter()
                .map(|h| NON_PRINTABLE.replace_all(h, |c: &regex::bytes::Captures| -> String {
                    format!("%{:02x}", c.get(0).unwrap().as_bytes()[0])
                }).into_owned())
                .map(BString::new)
                .collect();
        }

        if self.opts.drop_header {
            Ok(())
        } else {
            if self.opts.inner.numbered_columns == AutoChoices::Always {
                for (i, col) in header.iter_mut().enumerate() {
                    let prefix = format!("{} ", i + 1);
                    let leading_space = col.iter().take(prefix.len()).take_while(|&&x| x == b' ').count();
                    col.splice(0 .. leading_space, prefix.into_bytes());
                }
            }

            self.on_row(state, header, true, false)
        }
    }

    fn on_raw(&mut self, state: &mut WriterState, value: BString, ors: bool, clear: bool) -> Result<()> {
        self.writer.write_raw(state, value, ors, &self.opts, false, clear)
    }

    fn on_ofs(&mut self, ofs: Ofs) -> MaybeBreak {
        self.ofs = ofs;
        Ok(())
    }

    fn on_stderr(&mut self, state: &mut WriterState, row: Vec<BString>) -> Result<()> {
        self.on_row(state, row, false, true)
    }

    fn on_raw_stderr(&mut self, state: &mut WriterState, value: BString, ors: bool, clear: bool) -> Result<()> {
        self.writer.write_raw_stderr(state, value, ors, &self.opts, clear)
    }

    pub fn run(&mut self, receiver: Receiver<Message>) -> Result<()> {
        let mut state = WriterState{
            ors: self.opts.get_ors(),
            hyperlinks: self.opts.inner.hyperlink_columns
                .is_on(self.opts.inner.is_stdout_tty)
                .then(|| (std::process::id(), vec![])),
            ..WriterState::default()
        };
        for msg in receiver {
            self.handle_message(&mut state, msg)?;
        }
        Ok(())
    }

    pub fn handle_message(&mut self, state: &mut WriterState, msg: Message) -> Result<()> {
        match msg {
            Message::Row(row) => self.on_row(state, row, false, false),
            Message::Header(header) => self.on_header(state, header),
            Message::Eof => self.on_eof(state),
            Message::Separator => self.on_separator(state),
            Message::Raw(value, ors, clear) => self.on_raw(state, value, ors, clear),
            Message::Ofs(ofs) => Ok(self.on_ofs(ofs)?),
            Message::Stderr(row) => self.on_stderr(state, row),
            Message::RawStderr(value, ors, clear) => self.on_raw_stderr(state, value, ors, clear),
        }
    }

}
