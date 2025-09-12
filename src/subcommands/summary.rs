use anyhow::{Result};
use crate::base;
use regex::bytes::{Regex};
use bstr::{BString, ByteSlice, BStr};
use clap::{Parser, ArgAction, CommandFactory, error::{ErrorKind, ContextKind, ContextValue}};
use crate::column_slicer::ColumnSlicer;
use std::collections::HashMap;
use once_cell::sync::Lazy;

fn nice_float(val: f64) -> String {
    let mut s = format!("{val:.3}");
    s.truncate(s.trim_end_matches('0').len());
    s.truncate(s.trim_end_matches('.').len());
    s
}

static NUM_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^\d+(?:\.\d+)?").unwrap());

fn parse_size(bytes: &BStr) -> Option<f64> {
    let m = NUM_REGEX.find(bytes)?;
    let suffix = bytes[m.end() .. ].trim();
    let len = suffix.len().min(3);
    let mut suffix_copy = [0; 3];
    suffix_copy[..len].copy_from_slice(&suffix[..len]);
    suffix_copy.make_ascii_lowercase();

    let mul = match &suffix_copy[..len] {
        b"" | b"b" => 1,
        b"k" | b"kb" => 10usize.pow(3),
        b"kib" => 2usize.pow(10),
        b"m" | b"mb" => 10usize.pow(6),
        b"mib" => 2usize.pow(20),
        b"g" | b"gb" => 10usize.pow(9),
        b"gib" => 2usize.pow(30),
        b"t" | b"tb" => 10usize.pow(12),
        b"tib" => 2usize.pow(40),
        b"p" | b"pb" => 10usize.pow(15),
        b"pib" => 2usize.pow(50),
        _ => return None,
    };
    let val: f64 = std::str::from_utf8(m.as_bytes()).unwrap().parse().unwrap();
    Some(val * mul as f64)
}

fn make_counter<T: Eq + std::hash::Hash, I: IntoIterator<Item=T>>(values: I) -> HashMap<T, usize> {
    let mut counts = HashMap::new();
    for c in values {
        counts.entry(c).and_modify(|v| *v += 1).or_insert(1);
    }
    counts
}

fn get_quartiles<T>(values: &[T]) -> (&T, &T, &T) {
    let len = values.len() as f64;
    (
        &values[((len - 1.0) * 0.25).round() as usize],
        &values[((len - 1.0) * 0.5).round() as usize],
        &values[((len - 1.0) * 0.75).round() as usize],
    )
}

#[derive(Parser, Default)]
#[command(about = "product automatic summaries of the data")]
pub struct Opts {
    #[arg(long, value_enum, default_value_t = base::AutoChoices::Auto, help = "show a separator between the columns")]
    col_sep: base::AutoChoices,
    #[arg(help = "select only these fields")]
    fields: Vec<String>,
    #[arg(short = 'x', long, help = "exclude, rather than include, field names")]
    complement: bool,
    #[arg(short = 'r', long, help = "treat fields as regexes")]
    regex: bool,
    #[arg(short = 't', num_args = 2, action = ArgAction::Append, long, value_names = ["A", "B"], help = "assume field A is type B")]
    r#type: Vec<String>,
}

pub struct Handler {
    header: Option<Vec<BString>>,
    complement: bool,
    rows: Vec<Vec<BString>>,
    column_slicer: Option<ColumnSlicer>,
    col_sep: bool,
    types: Vec<(String, Type)>,
}

#[derive(Clone, Copy)]
enum Type {
    Date,
    Enum,
    Percent,
    Number,
    Size,
}

const CUTOFF: f64 = 0.8;

impl Handler {
    pub fn new(opts: Opts, base: &mut base::Base) -> Result<Self> {
        base.opts.pretty = true;

        // verify the types
        let mut types = vec![];
        let mut type_map = HashMap::new();
        type_map.insert("date", Type::Date);
        type_map.insert("enum", Type::Enum);
        type_map.insert("percent", Type::Percent);
        type_map.insert("number", Type::Number);
        type_map.insert("size", Type::Size);

        for [field, typ] in opts.r#type.as_chunks::<2>().0 {
            if let Some(&typ) = type_map.get(typ.as_str()) {
                types.push((field.clone(), typ));
            } else {
                let mut allowed: Vec<_> = type_map.keys().map(|k| (**k).to_string()).collect();
                allowed.sort();

                let cmd = crate::subcommands::Cli::command();
                let mut err = clap::Error::new(ErrorKind::InvalidValue).with_cmd(&cmd);
                err.insert(ContextKind::InvalidArg, ContextValue::String("--type".into()));
                err.insert(ContextKind::InvalidValue, ContextValue::String(typ.to_owned()));
                err.insert(ContextKind::ValidValue, ContextValue::Strings(allowed));
                err.exit();
            }
        }

        Ok(Self{
            complement: opts.complement,
            column_slicer: (!opts.fields.is_empty()).then(|| ColumnSlicer::new(&opts.fields, opts.regex)),
            col_sep: opts.col_sep.is_on(base.opts.is_stdout_tty),
            header: None,
            rows: vec![],
            types,
        })
    }
}

impl base::Processor for Handler {

    fn on_header(&mut self, _base: &mut base::Base, header: Vec<BString>) -> Result<bool> {
        let header = if let Some(slicer) = &mut self.column_slicer {
            slicer.make_header_map(&header);
            slicer.slice(&header, self.complement, true)
        } else {
            header
        };
        self.header = Some(header);
        Ok(false)
    }

    fn on_row(&mut self, _base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        let row = self.column_slicer.as_ref().map(|slicer| slicer.slice(&row, self.complement, true)).unwrap_or(row);
        self.rows.push(row);
        Ok(false)
    }

    fn on_eof(self, base: &mut base::Base) -> Result<bool> {
        if base.on_header(vec![b"column".into(), b"type".into(), b"key".into(), b"value".into()])? {
            return Ok(true)
        }

        let mut header = self.header.unwrap_or_default();
        let num_cols = self.rows.iter().map(|r| r.len()).max().unwrap_or(0).max(header.len());

        let mut column_slicer = ColumnSlicer::new(&[], false);
        column_slicer.make_header_map(&header);

        if header.len() < num_cols {
            header.extend((header.len() .. num_cols).map(|i| format!("{i}").into()));
        }

        let mut types = vec![None; header.len()];
        for (field, typ) in self.types {
            if let Some(t) = column_slicer.get_single_field_index(field.as_ref()).and_then(|i| types.get_mut(i)) {
                *t = Some(typ);
            }
        }

        for (i, (h, t)) in header.into_iter().zip(types).enumerate() {
            if self.col_sep && i > 0 && base.on_separator() {
                return Ok(true)
            }

            let column: Vec<_> = self.rows.iter().map(|r| r.get(i)).collect();
            // what is it

            if column.iter().all(|c| c.is_none()) {
                if base.on_row(vec![h, b"(empty)".into()])? {
                    return Ok(true)
                }
            } else if let Some(t) = t {
                let result = match t {
                    Type::Date => display_date(base, &h, &column, 0.),
                    Type::Number => display_numeric(base, &h, &column, 0.),
                    Type::Enum => display_enum(base, &h, &column, 0.),
                    Type::Size => display_size(base, &h, &column, 0.),
                    Type::Percent => display_percentage(base, &h, &column, 0.),
                };
                if let Some(result) = result && result? {
                    return Ok(true)
                }
            } else if let Some(result) =
                                display_enum(base, &h, &column, CUTOFF)
                    .or_else(|| display_date(base, &h, &column, CUTOFF))
                    .or_else(|| display_numeric(base, &h, &column, CUTOFF))
                    .or_else(|| display_percentage(base, &h, &column, CUTOFF))
                    .or_else(|| display_size(base, &h, &column, CUTOFF))
            {
                if result? {
                    return Ok(true)
                }
            } else if display_enum(base, &h, &column, 0.).unwrap()? {
                return Ok(true)
            }
        }

        base.on_eof()
    }
}

fn display_stats<I: Iterator<Item=Vec<BString>>>(base: &mut base::Base, stats: I) -> Result<bool> {
    for row in stats {
        if base.on_row(row)? {
            return Ok(true)
        }
    }
    Ok(false)
}

fn display_enum(base: &mut base::Base, header: &BString, column: &Vec<Option<&BString>>, cutoff: f64) -> Option<Result<bool>> {
    const N: usize = 5;

    let non_blank: Vec<_> = column.iter().flatten().filter(|v| !v.is_empty()).copied().collect();
    let counts = make_counter(non_blank.iter().copied());
    let mut counts: Vec<_> = counts.into_iter().map(|(k, v)| (k.clone(), v)).collect();
    counts.sort_by_key(|(_, v)| *v);
    counts.reverse();
    let most_common: usize = counts.iter().take(N).map(|(_, v)| v).copied().sum();
    if (most_common as f64 / column.len() as f64) < cutoff {
        return None
    }

    let counts_len = counts.len();
    let common: Vec<_> = counts.iter().take(N).filter(|(_, v)| *v > 1).cloned().collect();
    let common: Vec<(BString, usize)> = if counts.len() <= N + 1 {
        counts
    } else {
        common
    };
    let mut stats = common.clone();

    if non_blank.len() != column.len() {
        stats.push((b"[empty string]".into(), column.len() - non_blank.len() ));
    }

    let typ: &[u8];
    let stats = if stats.is_empty() {
        let mut stats: Vec<(&[u8], BString)> = vec![];
        // no common strings, do some word stats etc instead
        typ = b"string";
        let min_len = column.iter().flatten().map(|c| c.len()).min().unwrap();
        stats.push((b"min length", format!("{min_len}").into()));
        let max_len = column.iter().flatten().map(|c| c.len()).max().unwrap();
        stats.push((b"max length", format!("{max_len}").into()));
        let words = column.iter().flatten().flat_map(|c| c.fields()).count();
        stats.push((b"words", format!("{words}").to_string().into()));
        stats.push((b"[example]", column.iter().flatten().next().unwrap().to_owned().clone() ));
        stats
    } else {
        typ = b"enum";
        let other: usize = non_blank.len() - common.iter().map(|(_, v)| v).sum::<usize>();
        if other > 0 {
            stats.push((format!("[{} other values]", counts_len - common.len()).into(), other ));
        }

        stats.iter()
            .map(|(k, v)| (k.as_ref(), format!("{} ({})%", v, nice_float(100. * *v as f64 / column.len() as f64)).into()))
            .collect()
    };

    Some(display_stats(base, stats.into_iter().map(|(k, v)| {
        vec![header.clone(), typ.into(), k.into(), v]
    })))
}

fn display_date(base: &mut base::Base, header: &BString, column: &Vec<Option<&BString>>, cutoff: f64) -> Option<Result<bool>> {
    const DATE_YARDSTICK: f64 = chrono::NaiveDate
        ::from_ymd_opt(2000, 1, 1).unwrap()
        .and_hms_opt(0, 0, 0).unwrap()
        .and_utc()
        .timestamp() as _;

    let parsed: Vec<_> = column.iter().map(|&c| {
        let c = std::str::from_utf8(c?).ok()?;
        [
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
            "%d/%m/%y %H:%M:%S",
        ].iter().find_map(|f| chrono::DateTime::parse_from_str(c, f).map(|d| d.to_utc()).ok())
        .map(|date| date.timestamp() as f64)
        .or_else(|| {
            let val = c.parse().ok()?;
            if val > DATE_YARDSTICK * 1000. {
                // this is in milliseconds
                Some(val / 1000.)
            } else if val > DATE_YARDSTICK {
                Some(val)
            } else {
                None
            }
        })
    }).collect();

    let stats = get_numeric_stats(&parsed, cutoff, |x| {
        chrono::DateTime::from_timestamp(x.floor() as _, ((x % 1.) * 1_000_000.) as _).unwrap().to_rfc3339()
    })?;
    Some(display_stats(base, stats.into_iter().map(|(k, v)| {
        vec![header.clone(), b"date".into(), k.into(), v.into()]
    })))
}

fn get_numeric_stats<F: Fn(f64) -> String>(
    column: &[Option<f64>],
    cutoff: f64,
    formatter: F,
) -> Option<Vec<(&str, String)>> {

    let valid_len = column.iter().flatten().count();
    if (valid_len as f64 / column.len() as f64) < cutoff {
        return None
    }

    let mut non_nan: Vec<_> = column.iter().flatten().filter(|x| !x.is_nan()).copied().collect();
    non_nan.sort_by(f64::total_cmp);
    let finite: Vec<_> = non_nan.iter().filter(|x| !x.is_infinite()).copied().collect();
    let quartiles = get_quartiles(&non_nan);

    let mean = if finite.is_empty() { &non_nan } else { &finite };
    let mean = mean.iter().copied().sum::<f64>() / mean.len() as f64;

    let mut stats = vec![
        ("min", formatter(non_nan[0])),
        ("first quartile", formatter(*quartiles.0)),
        ("mean", formatter(mean)),
        ("median", formatter(*quartiles.1)),
        ("third quartile", formatter(*quartiles.2)),
        ("max", formatter(*non_nan.last().unwrap())),
    ];
    if non_nan.len() != valid_len {
        stats.push(("nan", (valid_len - non_nan.len()).to_string()));
    }
    if valid_len != column.len() {
        stats.push(("non numeric", (column.len() - valid_len).to_string()));
    }
    Some(stats)
}

fn display_numeric(base: &mut base::Base, header: &BString, column: &Vec<Option<&BString>>, cutoff: f64) -> Option<Result<bool>> {
    let parsed: Vec<_> = column.iter().map(|c| std::str::from_utf8(c.as_ref()?).ok()?.parse().ok()).collect();
    let stats = get_numeric_stats(&parsed, cutoff, nice_float)?;
    Some(display_stats(base, stats.into_iter().map(|(k, v)| {
        vec![header.clone(), b"numeric".into(), k.into(), v.into()]
    })))
}
fn display_percentage(base: &mut base::Base, header: &BString, column: &Vec<Option<&BString>>, cutoff: f64) -> Option<Result<bool>> {
    let parsed: Vec<_> = column.iter().map(|&c| std::str::from_utf8(c?.strip_suffix(b"%")?).ok()?.parse().ok()).collect();
    let stats = get_numeric_stats(&parsed, cutoff, |x| format!("{}%", nice_float(x)))?;
    Some(display_stats(base, stats.into_iter().map(|(k, v)| {
        vec![header.clone(), b"percent".into(), k.into(), v.into()]
    })))
}
fn display_size(base: &mut base::Base, header: &BString, column: &Vec<Option<&BString>>, cutoff: f64) -> Option<Result<bool>> {
    let parsed: Vec<_> = column.iter().map(|&c| parse_size(c?.as_ref())).collect();
    let stats = get_numeric_stats(&parsed, cutoff, |size| {
        let suffixes = ["b", "kb", "mb", "gb", "tb", "pb"];
        let (exp, suffix) = suffixes.iter()
            .enumerate()
            .filter(|(exp, _)| size < 1_000usize.pow(*exp as u32 + 1) as f64)
            .chain(std::iter::once((suffixes.len() - 1, suffixes.last().unwrap())))
            .next()
            .unwrap();
        format!("{} {}", nice_float(size / 1_000usize.pow(exp as u32) as f64), suffix)
    })?;
    Some(display_stats(base, stats.into_iter().map(|(k, v)| {
        vec![header.clone(), b"size".into(), k.into(), v.into()]
    })))
}
