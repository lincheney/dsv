use anyhow::{Result};
use crate::base;
use regex::bytes::{Regex};
use bstr::{BString, ByteSlice, BStr};
use clap::{Parser};
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

fn make_counter<T: Eq + std::hash::Hash, I: Iterator<Item=T>>(values: I) -> HashMap<T, usize> {
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
}

pub struct Handler {
    header: Option<Vec<BString>>,
    rows: Vec<Vec<BString>>,
    col_sep: bool,
}

const CUTOFF: f64 = 0.8;

impl Handler {
    pub fn new(opts: Opts, base: &mut base::Base, is_tty: bool) -> Result<Self> {
        base.opts.pretty = true;
        Ok(Self{
            col_sep: opts.col_sep.is_on(is_tty),
            header: None,
            rows: vec![],
        })
    }
}

impl base::Processor for Handler {

    fn on_header(&mut self, _base: &mut base::Base, header: Vec<BString>) -> Result<bool> {
        self.header = Some(header);
        Ok(false)
    }

    fn on_row(&mut self, _base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        self.rows.push(row);
        Ok(false)
    }

    fn on_eof(self, base: &mut base::Base) -> Result<bool> {
        if base.on_header(vec![b"column".into(), b"type".into(), b"key".into(), b"value".into()])? {
            return Ok(true)
        }

        let mut header = self.header.unwrap_or_default();
        let num_cols = self.rows.iter().map(|r| r.len()).max().unwrap_or(0).max(header.len());

        if header.len() < num_cols {
            header.extend((header.len() .. num_cols).map(|i| format!("{i}").into()));
        }

        for (i, h) in header.into_iter().enumerate() {
            let column: Vec<_> = self.rows.iter().map(|r| r.get(i)).collect();
            // what is it

            if column.iter().all(|c| c.is_none()) {
                if base.on_row(vec![h, b"(empty)".into()])? {
                    return Ok(true)
                }
            } else if let Some(result) = display_enum(base, &h, &column, CUTOFF)
                    .or_else(|| display_date(base, &h, &column, CUTOFF))
                    .or_else(|| display_numeric(base, &h, &column, CUTOFF))
                    .or_else(|| display_percentage(base, &h, &column, CUTOFF))
                    .or_else(|| display_size(base, &h, &column, CUTOFF))
                    .or_else(|| display_enum(base, &h, &column, 0.))
                && result?
            {
                return Ok(true)
            }

            if self.col_sep && base.on_separator() {
                return Ok(true)
            }
        }

        base.on_eof()
                // elif self.is_numeric(numbers := _utils.parse_value([c.strip().removesuffix(b'%') for c in col])) >= cutoff:
                    // if self.display_numeric(h, numbers, formatter=self.format_percentage):
                        // break
    }
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
        stats.push((b"[empty string]".to_owned().into(), column.len() - non_blank.len() ));
    }

    let typ: &BStr;
    let stats = if stats.is_empty() {
        let mut stats: Vec<(&[u8], BString)> = vec![];
        // no common strings, do some word stats etc instead
        typ = b"string".into();
        let min_len = column.iter().map(|c| c.map_or(0, |c| c.len())).min().unwrap();
        stats.push((b"min length", format!("{min_len}").into()));
        let max_len = column.iter().map(|c| c.map_or(0, |c| c.len())).max().unwrap();
        stats.push((b"max length", format!("{max_len}").into()));
        let words = column.iter().flatten().flat_map(|c| c.fields()).count();
        stats.push((b"words", format!("{words}").to_string().into()));
        stats.push((b"[example]", column.iter().flatten().next().unwrap().to_owned().clone() ));
        stats
    } else {
        typ = b"enum".into();
        let other: usize = non_blank.len() - common.iter().map(|(_, v)| v).sum::<usize>();
        if other > 0 {
            stats.push((format!("[{} other values]", counts_len - common.len()).into(), other ));
        }

        stats.iter()
            .map(|(k, v)| (k.as_ref(), format!("{} ({})%", v, nice_float(100. * *v as f64 / column.len() as f64)).into()))
            .collect()
    };

    Some((|| {
        for (k, v) in stats.iter() {
            if base.on_row(vec![header.clone(), typ.to_owned(), k.to_owned().into(), v.clone()])? {
                return Ok(true)
            }
        }
        Ok(false)
    })())
}

fn display_date(base: &mut base::Base, header: &BString, column: &Vec<Option<&BString>>, cutoff: f64) -> Option<Result<bool>> {
    const DATE_YARDSTICK: chrono::NaiveDate = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();

    let date_yardstick: chrono::NaiveDateTime = DATE_YARDSTICK.into();
    let date_yardstick = date_yardstick.and_utc().timestamp();
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
        ].iter().filter_map(|f| chrono::DateTime::parse_from_str(c, f).map(|d| d.to_utc()).ok()).next()
        .or_else(|| {
            let val = c.parse::<f64>().ok()?;
            if val > (date_yardstick * 1000) as f64 {
                // this is in milliseconds
                chrono::DateTime::from_timestamp((val / 1000.) as _, ((val % 1000.) * 1_000_000.) as _)
            } else if val > date_yardstick as f64 {
                chrono::DateTime::from_timestamp(val.floor() as _, ((val % 1.) * 1_000_000.) as _)
            } else {
                None
            }
        })
    }).collect();
    let mut valid: Vec<_> = parsed.iter().flatten().collect();
    if (valid.len() as f64 / parsed.len() as f64) < cutoff {
        return None
    }

    valid.sort();
    let quartiles = get_quartiles(&valid);
    let mean = valid.iter().map(|d| d.timestamp()).sum::<i64>() as f64 / valid.len() as f64;
    let mean = chrono::DateTime::from_timestamp((mean / 1000.) as _, ((mean % 1000.) * 1_000_000.) as _).unwrap();

    let stats = [
        ("min", valid[0]),
        ("first quartile", quartiles.0),
        ("mean", &mean),
        ("median", quartiles.1),
        ("third quartile", quartiles.2),
        ("max", valid.last().unwrap()),
    ];

    Some((|| {
        for (k, v) in stats.iter() {
            if base.on_row(vec![header.clone(), b"date".into(), k.as_bytes().into(), v.to_rfc3339().into()])? {
                return Ok(true)
            }
        }
        Ok(false)
    })())
}

fn get_numeric_stats<F: Fn(f64) -> String>(
    column: &[Option<f64>],
    cutoff: f64,
    formatter: F,
) -> Option<Vec<(BString, BString)>> {

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
        ("min".into(), formatter(non_nan[0]).into()),
        ("first quartile".into(), formatter(*quartiles.0).into()),
        ("mean".into(), formatter(mean).into()),
        ("median".into(), formatter(*quartiles.1).into()),
        ("third quartile".into(), formatter(*quartiles.2).into()),
        ("max".into(), formatter(*non_nan.last().unwrap()).into()),
    ];
    if non_nan.len() != valid_len {
        stats.push(("nan".into(), (valid_len - non_nan.len()).to_string().into()));
    }
    if valid_len != column.len() {
        stats.push(("non numeric".into(), (column.len() - valid_len).to_string().into()));
    }
    Some(stats)
}

fn display_numeric(base: &mut base::Base, header: &BString, column: &Vec<Option<&BString>>, cutoff: f64) -> Option<Result<bool>> {
    let parsed: Vec<_> = column.iter().map(|c| std::str::from_utf8(c.as_ref()?.as_ref()).ok()?.parse::<f64>().ok()).collect();
    let stats = get_numeric_stats(&parsed, cutoff, nice_float)?;
    Some((|| {
        for (k, v) in stats.iter() {
            if base.on_row(vec![header.clone(), b"numeric".into(), k.as_bytes().into(), v.as_bytes().into()])? {
                return Ok(true)
            }
        }
        Ok(false)
    })())
}
fn display_percentage(base: &mut base::Base, header: &BString, column: &Vec<Option<&BString>>, cutoff: f64) -> Option<Result<bool>> {
    let parsed: Vec<_> = column.iter().map(|&c| std::str::from_utf8(c?.strip_suffix(b"%")?).ok()?.parse::<f64>().ok()).collect();
    let stats = get_numeric_stats(&parsed, cutoff, |x| format!("{}%", nice_float(x)))?;
    Some((|| {
        for (k, v) in stats.iter() {
            if base.on_row(vec![header.clone(), b"percent".into(), k.as_bytes().into(), v.as_bytes().into()])? {
                return Ok(true)
            }
        }
        Ok(false)
    })())
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
    Some((|| {
        for (k, v) in stats.iter() {
            if base.on_row(vec![header.clone(), b"size".into(), k.as_bytes().into(), v.as_bytes().into()])? {
                return Ok(true)
            }
        }
        Ok(false)
    })())
}
