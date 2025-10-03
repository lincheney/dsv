use crate::utils::Break;
use crate::utils::MaybeBreak;
use anyhow::{Result, Context};
use std::sync::mpsc::{self, Sender, Receiver};
use crate::base::*;
use bstr::{BString};
use std::collections::{HashSet, HashMap, hash_map::Entry};
use crate::column_slicer::ColumnSlicer;
use clap::{Parser};

#[derive(Parser, Clone)]
#[command(about = "join lines of two files on a common field")]
pub struct Opts {
    #[arg(value_name = "FILE", help = "join stdin with FILE")]
    file: String,
    #[arg(conflicts_with_all = ["left_fields", "right_fields"], help = "join on these fields from stdin and FILE")]
    fields: Vec<String>,
    #[arg(short = '1', help = "join on these fields from stdin")]
    left_fields: Vec<String>,
    #[arg(short = '2', help = "join on these fields from FILE")]
    right_fields: Vec<String>,
    #[arg(short = 'e', value_name = "STRING", help = "replace missing input fields with STRING")]
    empty_value: Option<String>,
    #[arg(long, help = "treat fields as regexes")]
    regex: bool,
    #[arg(long, help = "rename header from stdin according to this %%-format string")]
    rename_1: Option<String>,
    #[arg(long, help = "rename header from FILE according to this %%-format string")]
    rename_2: Option<String>,
    #[arg(short = 'a', overrides_with_all = ["join", "inner", "left", "right", "outer"], value_parser = ["1", "2"], help = "also print unpairable lines from the given file")]
    show_all: Vec<String>,
    #[arg(long, value_parser = ["inner", "left", "right", "outer"], help = "type of join to perform")]
    join: Option<String>,
    #[arg(long, help = "do a inner join")]
    inner: bool,
    #[arg(long, help = "do a left join")]
    left: bool,
    #[arg(long, help = "do a right join")]
    right: bool,
    #[arg(long, help = "do a outer join")]
    outer: bool,
}

#[derive(Copy, Clone)]
enum Join {
    Inner,
    Left,
    Right,
    Outer,
}

type Row = Vec<BString>;

pub struct Handler {
    inner: Child,
    err_receiver: Receiver<Result<()>>,
}

impl Handler {
    pub fn new(mut opts: Opts, base: &mut Base) -> Result<Self> {
        if !opts.fields.is_empty() {
            opts.left_fields = opts.fields.clone();
            opts.right_fields = std::mem::take(&mut opts.fields);
        }

        let join = match opts.join.as_deref() {
            Some("inner") => Join::Inner,
            Some("left") => Join::Left,
            Some("right") => Join::Right,
            Some("outer") => Join::Outer,
            _ if opts.inner => Join::Inner,
            _ if opts.left => Join::Left,
            _ if opts.right => Join::Right,
            _ if opts.outer => Join::Outer,
            _ => match (opts.show_all.iter().any(|x| x == "1"), opts.show_all.iter().any(|x| x == "2")) {
                (true, true) => Join::Outer,
                (true, false) => Join::Left,
                (false, true) => Join::Right,
                (false, false) => Join::Inner,
            },
        };

        let (sender, receiver) = mpsc::channel();
        let (err_sender, err_receiver) = mpsc::channel();
        let child = Child{ got_header: false, left: true, sender: Some(sender) };

        // start a thread to join everything
        {
            let mut base = base.clone();
            let opts = opts.clone();
            let err_sender = err_sender.clone();
            base.scope.spawn(move || {
                let result = (|| {
                    Joiner::default().do_joining(join, &opts, receiver, &mut base)?;
                    base.on_eof()?;
                    Ok(())
                })();
                err_sender.send(result).unwrap();
            });
        }

        // start a thread to read from rhs
        {
            let mut base = base.clone();
            let right_file = std::mem::take(&mut opts.file);
            let mut child = child.clone();
            child.left = false;
            base.scope.spawn(move || {
                let result = (|| {
                    let file = std::fs::File::open(&right_file).with_context(|| format!("failed to open {right_file}"))?;
                    let file = std::io::BufReader::new(file);
                    child.process_file(file, &mut base, Callbacks::ON_HEADER | Callbacks::ON_ROW)?;
                    Ok(())
                })();
                err_sender.send(result).unwrap();
            });
        }

        // and we will read from lhs ...

        Ok(Self {
            inner: child,
            err_receiver,
        })
    }
}

impl Processor for Handler {
    fn on_row(&mut self, base: &mut Base, row: Vec<BString>) -> Result<()> {
        self.inner.on_row(base, row)
    }

    fn on_header(&mut self, base: &mut Base, header: Vec<BString>) -> Result<()> {
        self.inner.on_header(base, header)
    }

    fn on_ofs(&mut self, _base: &mut Base, ofs: Ofs) -> MaybeBreak {
        Break::when(self.inner.sender.as_ref().unwrap().send((true, Message::Ofs(ofs))).is_err())
    }

    fn on_eof(self, _base: &mut Base) -> Result<bool> {
        drop(self.inner.sender);
        crate::utils::chain_errors(self.err_receiver)?;
        Ok(false)
    }
}

#[derive(Default)]
struct Joiner {
    key_len: usize,
    left_len: usize,
    right_len: usize,
}

type JoinStore = HashMap<Row, Vec<Row>>;

impl Joiner {
    fn do_joining(&mut self, join: Join, opts: &Opts, receiver: Receiver<(bool, Message)>, base: &mut Base) -> Result<()> {
        let fields = (&opts.left_fields, &opts.right_fields);
        let is_fields_set = !fields.0.is_empty() || !fields.1.is_empty();
        let mut stores = (HashMap::new(), HashMap::new());
        let mut headers = (None, None);
        let mut slicers = (ColumnSlicer::new(fields.0, opts.regex), ColumnSlicer::new(fields.1, opts.regex));
        let mut got_headers = false;
        let mut buffer = vec![];

        for (is_left, msg) in &receiver {
            match msg {
                Message::Separator => unreachable!(),
                Message::Raw(..) => unreachable!(),
                Message::Eof => (),
                Message::Stderr(_) => unreachable!(),
                Message::RawStderr(..) => unreachable!(),
                Message::Ofs(ofs) => if is_left {
                    base.on_ofs(ofs)?;
                },
                Message::Header(header) => {
                    if is_left {
                        headers.0 = Some(header);
                    } else {
                        headers.1 = Some(header);
                    }

                    if !got_headers && let Some(headers) = headers.0.as_ref().zip(headers.1.as_ref()) {
                        got_headers = true;

                        if ! is_fields_set {
                            // get common fields
                            let left: HashSet<_> = headers.0.iter().collect();
                            let right: HashSet<_> = headers.1.iter().collect();
                            let fields: Vec<_> = left.intersection(&right).copied().cloned().collect();
                            if fields.is_empty() {
                                // default join field is the first
                                slicers.0 = ColumnSlicer::new(&["1".into()], false);
                                slicers.1 = ColumnSlicer::new(&["1".into()], false);
                            } else {
                                slicers.0 = ColumnSlicer::from_names(fields.iter());
                                slicers.1 = ColumnSlicer::from_names(fields.iter());
                            }
                        }

                        // make header maps
                        slicers.0.make_header_map(headers.0);
                        slicers.1.make_header_map(headers.1);

                        // paste the headers together
                        let mut header = slicers.0.slice(headers.0, false, true);
                        let mut left = slicers.0.slice(headers.0, true, true);
                        let mut right = slicers.1.slice(headers.1, true, true);

                        if let Some(rename) = &opts.rename_1 {
                            for h in &mut left {
                                *h = crate::utils::percent_format(rename.as_bytes().into(), h.as_ref());
                            }
                        }
                        if let Some(rename) = &opts.rename_2 {
                            for h in &mut right {
                                *h = crate::utils::percent_format(rename.as_bytes().into(), h.as_ref());
                            }
                        }

                        header.append(&mut left);
                        header.append(&mut right);
                        base.on_header(header)?;

                        // clear out the buffered rows
                        // their side must be the other side
                        for row in buffer.drain(..) {
                            if self.on_row(!is_left, row, &mut stores, &slicers, base)? {
                                return Ok(())
                            }
                        }
                    }
                },
                Message::Row(row) => {
                    if !got_headers {
                        // stick it in the buffer for later
                        buffer.push(row);
                    } else if self.on_row(is_left, row, &mut stores, &slicers, base)? {
                        return Ok(())
                    }
                },
            }
        }

        if matches!(join, Join::Left | Join::Outer) {
            for (key, rows) in &stores.0 {
                if !stores.1.contains_key(key) {
                    for row in rows {
                        let row = self.make_row(key, Some(row), None, &slicers, opts.empty_value.as_ref());
                        base.on_row(row)?;
                    }
                }
            }
        }

        if matches!(join, Join::Right | Join::Outer) {
            for (key, rows) in &stores.1 {
                if !stores.0.contains_key(key) {
                    for row in rows {
                        let row = self.make_row(key, None, Some(row), &slicers, opts.empty_value.as_ref());
                        base.on_row(row)?;
                    }
                }
            }
        }
        Ok(())

    }

    fn make_row(
        &self,
        key: &Row,
        left: Option<&Row>,
        right: Option<&Row>,
        slicers: &(ColumnSlicer, ColumnSlicer),
        empty_value: Option<&String>,
    ) -> Row {
        let mut new_row = key.clone();
        new_row.resize(new_row.len().max(self.key_len), b"".into());

        let old_len = new_row.len();
        if let Some(left) = left {
            new_row.append(&mut slicers.0.slice(left, true, true));
        }
        let empty = empty_value.filter(|_| left.is_none()).map_or(b"" as _, |x| x.as_bytes());
        new_row.resize(new_row.len().max(old_len + self.left_len), empty.into());

        let old_len = new_row.len();
        if let Some(right) = right {
            new_row.append(&mut slicers.1.slice(right, true, true));
        } else if let Some(empty) = empty_value {
            new_row.resize(new_row.len().max(old_len + self.right_len), empty.as_bytes().into());
        }

        new_row
    }

    fn on_row(
        &mut self,
        is_left: bool,
        row: Row,
        stores: &mut (JoinStore, JoinStore),
        slicers: &(ColumnSlicer, ColumnSlicer),
        base: &mut Base,
    ) -> Result<bool> {

        if is_left && self.key_len == 0 {
            self.key_len = slicers.0.slice(&row, false, true).len();
        }
        if is_left && self.left_len == 0 {
            self.left_len = slicers.0.slice(&row, true, true).len();
        }
        if !is_left && self.right_len == 0 {
            self.right_len = slicers.1.slice(&row, true, true).len();
        }

        let left = (&slicers.0, &mut stores.0);
        let right = (&slicers.1, &mut stores.1);
        let (mut this, mut other) = (left, right);
        if !is_left {
            (this, other) = (other, this);
        }

        let key = this.0.slice(&row, false, true);
        // find any joined rows
        if let Some(other_rows) = other.1.get(&key) {
            for other_row in other_rows {
                let rows = if is_left { (&row, other_row) } else { (other_row, &row) };
                let row = self.make_row(&key, Some(rows.0), Some(rows.1), slicers, None);
                base.on_row(row)?;
            }
        }
        // put it in the store
        match this.1.entry(key) {
            Entry::Occupied(mut entry) => { entry.get_mut().push(row); },
            Entry::Vacant(entry) => { entry.insert(vec![row]); },
        }
        Ok(false)
    }
}

#[derive(Clone)]
struct Child {
    got_header: bool,
    left: bool,
    sender: Option<Sender<(bool, Message)>>,
}

impl Processor for Child {
    fn on_header(&mut self, _base: &mut Base, header: Vec<BString>) -> Result<()> {
        self.got_header = true;
        Break::when(self.sender.as_ref().unwrap().send((self.left, Message::Header(header))).is_err())
    }

    fn on_row(&mut self, base: &mut Base, row: Vec<BString>) -> Result<()> {
        if !self.got_header {
            self.on_header(base, vec![])?;
        }
        Break::when(self.sender.as_ref().unwrap().send((self.left, Message::Row(row))).is_err())
    }
}
