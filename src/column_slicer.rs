use std::cmp::min;
use bstr::BString;
use once_cell::sync::Lazy;
use regex::bytes::Regex;
use std::collections::HashMap;

static FIELD_REGEX: Lazy<regex::Regex> = Lazy::new(|| regex::Regex::new(r"^(\d+)?-(\d+)?$").unwrap());

pub fn make_header_map(header: &[BString]) -> HashMap<BString, usize> {
    header.iter().enumerate().map(|(i, k)| (k.clone(), i)).collect()
}

#[derive(Clone)]
pub struct ColumnSlicer {
    fields: Vec<Field>,
    headers: HashMap<BString, usize>,
}

#[derive(Clone)]
enum Field {
    Range(usize, usize),
    Index(usize),
    Regex(Regex),
    Name(BString),
}

impl ColumnSlicer {
    pub fn from_names<'a, F: Iterator<Item=&'a BString>>(fields: F) -> Self {
        Self {
            fields: fields.cloned().map(Field::Name).collect(),
            headers: HashMap::new(),
        }
    }

    pub fn new<'a, I: IntoIterator<Item=&'a String>>(fields: I, is_regex: bool) -> Self {
        let mut new_fields = vec![];

        for field in fields {
            if field != "-" && let Some(captures) = FIELD_REGEX.captures(field) {
                let start = captures.get(1).map_or(1usize, |m| m.as_str().parse().unwrap()).saturating_sub(1);
                let end = captures.get(2).map_or(usize::MAX, |m| m.as_str().parse().unwrap());
                new_fields.push(Field::Range(start, end));
            } else if let Ok(index) = field.parse::<usize>() {
                new_fields.push(Field::Index(index.saturating_sub(1)));
            } else if is_regex {
                new_fields.push(Field::Regex(Regex::new(field).unwrap()));
            } else {
                new_fields.push(Field::Name(field.clone().into()));
            }
        }

        Self {
            fields: new_fields,
            headers: HashMap::new(),
        }
    }

    pub fn make_header_map(&mut self, header: &[BString]) {
        self.headers = make_header_map(header);
    }

    pub fn get_single_field_index(&self, field: &str) -> Option<usize> {
        if let Ok(i) = field.parse::<usize>() {
            Some(i.saturating_sub(1))
        } else {
            self.headers.get(field.as_bytes()).copied()
        }
    }

    fn range_for_row(start: usize, end: usize, len: usize) -> std::ops::Range<usize> {
        min(start, len) .. min(end, len)
    }

    pub fn matches(&self, index: usize) -> bool {
        self.fields.is_empty() ||
        self.fields.iter().any(|field| match field {
            Field::Range(start, end) => (*start .. *end).contains(&index),
            Field::Index(i) => *i == index,
            Field::Regex(regex) => self.headers.iter().any(|(k, &v)| v == index && regex.is_match(k)),
            Field::Name(name) => self.headers.get(name) == Some(&index),
        })
    }

    pub fn indices(&self, len: usize, complement: bool) -> impl Iterator<Item=usize> {

        let iters = if complement {
            let iter = (0..len).filter(|&index| !self.matches(index));
            (Some(iter), None, None)
        } else if self.fields.is_empty() {
            // actually means you want everything
            (None, Some(0..len), None)
        } else {
            let iter = self.fields.iter()
                .filter_map(move |field| match field {
                    Field::Range(start, end) => Some(Self::range_for_row(*start, *end, len)),
                    Field::Index(i) => Some(*i .. i+1),
                    Field::Name(name) => self.headers.get(name).map(|&i| i .. i+1),
                    Field::Regex(_) => None,
                }).chain(self.fields.iter()
                    .filter_map(|field| if let Field::Regex(regex) = field { Some(regex) } else { None })
                    .flat_map(|regex| {
                        self.headers.iter().filter(|(k, _)| regex.is_match(k)).map(|(_, &v)| v .. v+1)
                    })
                ).flatten();
            (None, None, Some(iter))
        };

        iters.0.into_iter().flatten().chain(iters.1.into_iter().flatten()).chain(iters.2.into_iter().flatten())
    }

    pub fn slice(
        &self,
        row: &[BString],
        complement: bool,
        allow_empty: bool,
    ) -> Vec<BString> {
        self.slice_with(row, complement, allow_empty.then_some(|_| b"".into()))
    }

    pub fn slice_with<T: Clone, F: Fn(usize) -> T>(
        &self,
        row: &[T],
        complement: bool,
        default: Option<F>,
    ) -> Vec<T> {

        self.indices(row.len(), complement)
            .filter_map(|i| row.get(i).cloned().or_else(|| default.as_ref().map(|d| d(i))))
            .collect()
    }
}
