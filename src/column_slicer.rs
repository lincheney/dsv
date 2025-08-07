use std::cmp::min;
use bstr::BString;
use once_cell::sync::Lazy;
use regex::bytes::Regex;
use std::collections::HashMap;

static FIELD_REGEX: Lazy<regex::Regex> = Lazy::new(|| regex::Regex::new(r"^(\d+)?-(\d+)?$").unwrap());

pub struct ColumnSlicer {
    fields: Vec<Field>,
    headers: HashMap<BString, usize>,
}

enum Field {
    Range(usize, usize),
    Index(usize),
    Regex(Regex),
    Name(BString),
}

impl ColumnSlicer {
    pub fn new(fields: &Vec<String>, is_regex: bool) -> Self {
        let mut new_fields = vec![];

        for field in fields {
            if field != "-" && let Some(captures) = FIELD_REGEX.captures(field) {
                let start = captures.get(1).map_or(0, |m| m.as_str().parse().unwrap());
                let end = captures.get(2).map_or(usize::MAX, |m| m.as_str().parse::<usize>().unwrap() + 1);
                new_fields.push(Field::Range(start, end));
            } else if let Ok(index) = field.parse::<usize>() {
                new_fields.push(Field::Index(index - 1));
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
        self.headers = header.iter().enumerate().map(|(i, k)| (k.clone(), i)).collect();
    }

    fn range_for_row(start: usize, end: usize, len: usize) -> std::ops::Range<usize> {
        min(start, len) .. min(end, len)
    }

    pub fn slice(
        &self,
        row: &[BString],
        complement: bool,
        allow_empty: bool,
    ) -> Vec<BString> {
        self.slice_with::<fn(usize) -> BString>(row, complement, allow_empty, None)
    }

    pub fn slice_with<F: Fn(usize) -> BString>(
        &self,
        row: &[BString],
        complement: bool,
        allow_empty: bool,
        default: Option<F>,
    ) -> Vec<BString> {

        if self.fields.is_empty() {
            return vec![];
        }

        if complement {
            let mut row: Vec<_> = row.iter().map(Some).collect();
            for field in &self.fields {
                match field {
                    Field::Range(start, end) => {
                        let range = Self::range_for_row(*start, *end, row.len());
                        row[range].fill(None);
                    },
                    Field::Index(i) => {
                        if let Some(col) = row.get_mut(*i) {
                            *col = None;
                        }
                    },
                    Field::Regex(regex) => {
                        for (k, &v) in &self.headers {
                            if regex.is_match(k) && let Some(col) = row.get_mut(v) {
                                *col = None;
                            }
                        }
                    },
                    Field::Name(name) => {
                        if let Some(&i) = self.headers.get(name) && let Some(col) = row.get_mut(i) {
                            *col = None;
                        }
                    },
                }
            }
            row.iter().filter_map(|x| x.cloned()).collect()

        } else {
            let mut new_row = vec![];

            for field in &self.fields {
                match field {
                    Field::Range(start, end) => {
                        let range = Self::range_for_row(*start, *end, row.len());
                        new_row.extend_from_slice(&row[range]);
                    },
                    Field::Index(i) => {
                        if let Some(col) = row.get(*i) {
                            new_row.push(col.clone());
                        } else if allow_empty {
                            new_row.push(default.as_ref().map(|f| f(*i)).unwrap_or_else(|| b"".into()));
                        }
                    },
                    Field::Regex(regex) => {
                        for (k, &v) in &self.headers {
                            if regex.is_match(k) {
                                new_row.push(row[v].clone());
                            }
                        }
                    },
                    Field::Name(name) => {
                        if let Some(&i) = self.headers.get(name) {
                            if let Some(col) = row.get(i) {
                                new_row.push(col.clone());
                            } else if allow_empty {
                                new_row.push(default.as_ref().map(|f| f(i)).unwrap_or_else(|| b"".into()));
                            }
                        }
                    },
                }
            }
            new_row
        }
    }
}
