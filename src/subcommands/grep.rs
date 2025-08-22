use anyhow::{Result, Context};
use std::collections::{VecDeque, HashSet};
use std::io::{BufReader, BufRead};
use crate::base;
use regex::bytes::{Regex, RegexBuilder};
use bstr::{BString};
use clap::{Parser, ArgAction};

const MATCH_COLOUR: &str = "\x1b[1;31m";

#[derive(Parser, Default)]
pub struct CommonOpts {
    #[arg(short = 'e', long, action = ArgAction::Append, help = "pattern to search for")]
    regexp: Vec<String>,
    #[arg(short = 'F', long, action = ArgAction::SetTrue, help = "treat all patterns as literals instead of as regular expressions")]
    fixed_strings: bool,
    #[arg(short = 'f', long, action = ArgAction::Append, help = "obtain patterns from FILE")]
    file: Vec<String>,
    #[arg(short = 'w', long, action = ArgAction::SetTrue, help = "select only those matches surrounded by word boundaries")]
    word_regexp: bool,
    #[arg(short = 'x', long, action = ArgAction::SetTrue, help = "select only those matches that exactly match the column")]
    field_regexp: bool,
    #[arg(short = 's', long, action = ArgAction::SetTrue, help = "search case sensitively")]
    case_sensitive: bool,
    #[arg(short = 'm', long, default_value_t = usize::MAX, value_name = "NUM", help = "show only the first NUM matching rows")]
    max_count: usize,
    #[arg(short = 'k', long, action = ArgAction::Append, help = "search only on these fields")]
    fields: Vec<String>,
    #[arg(short = 'r', long, action = ArgAction::SetTrue, help = "treat fields as regexes")]
    regex: bool,
    #[arg(long, action = ArgAction::SetTrue, help = "exclude, rather than include, field names")]
    complement: bool,
}

#[derive(Parser, Default)]
#[command(about = "print lines that match patterns")]
pub struct Opts {
    #[arg(action = ArgAction::Append, required_unless_present_any = ["regexp", "file"], help = "pattern to search for")]
    pub patterns: Vec<String>,
    #[arg(long, help = "replaces every match with the given text")]
    pub replace: Option<String>,
    #[arg(short = 'n', long, help = "show line numbers")]
    line_number: bool,
    #[arg(long, help = "print both matching and non-matching lines")]
    pub passthru: bool,
    #[arg(short = 'A', long, value_name = "NUM", help = "show NUM lines after each match")]
    after_context: Option<usize>,
    #[arg(short = 'B', long, value_name = "NUM", help = "show NUM lines before each match")]
    before_context: Option<usize>,
    #[arg(short = 'C', long, value_name = "NUM", help = "show NUM lines before and after each match")]
    context: Option<usize>,
    #[arg(short = 'c', long, help = "print only the count of matching rows")]
    count: bool,
    #[arg(short = 'v', long, help = "select non-matching lines")]
    invert_match: bool,
    #[command(flatten)]
    pub common: CommonOpts,
}

pub struct Handler {
    opts: Opts,
    matched_count: usize,
    pattern: Regex,
    replace: Option<String>,
    last_matched: Option<usize>,
    before: Option<VecDeque<Vec<BString>>>,
    after: usize,
    row_num: usize,
    column_slicer: crate::column_slicer::ColumnSlicer,
    allowed_fields: (HashSet<usize>, usize),
}

impl Handler {
    pub fn new(mut opts: Opts) -> Self {
        if opts.passthru {
            opts.before_context = None;
            opts.after_context = None;
            opts.context = None;
        }

        let after = opts.after_context.or(opts.context).unwrap_or(0);
        let before = opts.before_context.or(opts.context);
        let before = before.map(|b| VecDeque::with_capacity(b));
        let column_slicer = crate::column_slicer::ColumnSlicer::new(&opts.common.fields, opts.common.regex);

        Self {
            opts,
            before,
            after,
            row_num: 0,
            last_matched: None,
            matched_count: 0,
            pattern: Regex::new("").unwrap(),
            replace: None,
            column_slicer,
            allowed_fields: (HashSet::new(), 0),
        }
    }
}

impl base::Processor for Handler {
    fn process_opts(&mut self, opts: &mut base::BaseOptions, is_tty: bool) {
        self._process_opts(opts, is_tty);
        // no need to replace if invert and not passthru
        #[allow(clippy::nonminimal_bool)]
        if !(self.opts.invert_match && self.opts.passthru) && !self.opts.count && opts.colour == base::AutoChoices::Always {
            if let Some(mut replace) = self.opts.replace.take() {
                replace.insert_str(0, MATCH_COLOUR);
                replace.push_str(base::RESET_COLOUR);
                self.replace = Some(replace);
            } else {
                self.replace = Some(format!("{}$1{}", MATCH_COLOUR, base::RESET_COLOUR));
            }
        }
    }

    fn on_start(&mut self, _base: &mut base::Base) -> Result<bool> {
        let mut patterns = std::mem::take(&mut self.opts.patterns);
        for file in &self.opts.common.file {
            let file = std::fs::File::open(file).with_context(|| format!("failed to open {file}"))?;
            let file = BufReader::new(file);
            for line in file.lines() {
                patterns.push(line?);
            }
        }

        if self.opts.common.fixed_strings {
            for pat in patterns.iter_mut() {
                *pat = regex::escape(pat);
            }
        }
        let pattern = patterns.join("|");

        self.opts.common.case_sensitive = self.opts.common.case_sensitive || pattern.chars().any(|c| c.is_ascii_uppercase());

        // field overrides word
        let pattern = if self.opts.common.field_regexp {
            format!("^({pattern})$")
        } else if self.opts.common.word_regexp {
            format!("\\b({pattern})\\b")
        } else {
            format!("({pattern})")
        };
        self.pattern = RegexBuilder::new(&pattern)
            .case_insensitive(!self.opts.common.case_sensitive)
            .build()?;
        Ok(false)
    }

    fn on_header(&mut self, base: &mut base::Base, mut header: Vec<BString>) -> Result<bool> {
        self.column_slicer.make_header_map(&header);
        if self.opts.line_number {
            header.insert(0, b"n".into());
        }
        if self.opts.count {
            Ok(false)
        } else {
            base.on_header(header)
        }
    }

    fn on_eof(&mut self, base: &mut base::Base) -> Result<bool> {
        let result = base.on_eof();
        if self.opts.count {
            let output: BString = format!("{}", self.matched_count).into();
            base.write_raw(output);
        }
        result
    }


    fn on_row(&mut self, base: &mut base::Base, mut row: Vec<BString>) -> Result<bool> {
        self.row_num += 1;

        let matched = self.grep(&mut row);
        if matched {
            // matched this line
            if self.matched_count < self.opts.common.max_count {
                self.last_matched = Some(self.row_num);
            }
            self.matched_count += 1;
        }

        if !self.opts.count {
            if matched {
                // print the lines before
                if let Some(before) = &mut self.before {
                    let len = before.len();
                    for (i, mut r) in before.drain(..).enumerate() {
                        let i = i + self.row_num - len;
                        if self.opts.line_number {
                            r.insert(0, format!("{i}").into());
                        }
                        if base.on_row(r)? {
                            return Ok(true)
                        }
                    }
                }
            }

            // print this line if matched or it is in after or we are doing passthru
            if matched || self.opts.passthru || self.last_matched.is_some_and(|lm| lm + self.after >= self.row_num) {
                if self.opts.line_number {
                    row.insert(0, format!("{}", self.row_num).into());
                }
                if base.on_row(row)? {
                    return Ok(true)
                }
            } else {
                if let Some(before) = &mut self.before {
                    // this line might be a before
                    if before.len() >= before.capacity() {
                        before.pop_front();
                    }
                    before.push_back(row);
                }
                return Ok(false)
            }
        }

        // quit if reached max count
        Ok(self.matched_count >= self.opts.common.max_count && self.last_matched.is_some_and(|lm| lm + self.after <= self.row_num))
    }
}

impl Handler {
    fn grep(&mut self, row: &mut [BString]) -> bool {
        let mut matched = false;

        let allowed_fields = if self.opts.common.fields.is_empty() {
            None
        } else {
            if self.allowed_fields.1 < row.len() {
                let indices: Vec<_> = (0..row.len()).collect();
                let fields = self.column_slicer.slice_with::<usize, fn(usize)->usize>(&indices, self.opts.common.complement, None);
                self.allowed_fields.0 = fields.iter().copied().collect::<HashSet<_>>();
                self.allowed_fields.1 = row.len();
            }
            Some(&self.allowed_fields.0)
        };

        for (i, col) in row.iter_mut().enumerate() {
            if allowed_fields.as_ref().is_some_and(|x| x.contains(&i)) {
                continue
            }

            matched = if let Some(replace) = &self.replace {
                let replace: &[u8] = replace.as_ref();
                let replaced = self.pattern.replace_all(col, replace);
                let matched = matches!(replaced, std::borrow::Cow::Owned(_));
                if matched {
                    *col = replaced.into_owned().into();
                }
                matched
            } else {
                self.pattern.is_match(col)
            } || matched;

            if matched != self.opts.invert_match && self.replace.is_none() {
                return true
            }
        }
        matched
    }
}
