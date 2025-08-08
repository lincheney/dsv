use std::sync::mpsc::{self, Sender, Receiver};
use crate::base;
use anyhow::Result;
use bstr::{BString};
use std::collections::{HashSet, HashMap, hash_map::Entry};
use crate::column_slicer::ColumnSlicer;
use clap::{Parser, ArgAction};

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
    #[arg(short = 'e', value_name = "NAME", help = "replace missing input fields with STRING")]
    empty_value: Option<String>,
    #[arg(long, action = ArgAction::SetTrue, help = "treat fields as regexes")]
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
    opts: Opts,
    join: Join,
}

impl Handler {
    pub fn new(mut opts: Opts) -> Self {
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

        Self {
            opts,
            join,
        }
    }
}

impl base::Processor for Handler {
    fn process_file<R: std::io::Read>(
        &mut self,
        file: R,
        base: &mut base::Base,
        do_callbacks: base::Callbacks,
    ) -> Result<std::process::ExitCode> {

        let (sender, receiver) = mpsc::channel();
        let (tx, rx) = mpsc::channel();

        let mut left = Child{ ofs_sender: Some(tx), sender: sender.clone(), got_header: false };
        let mut right = Child{ ofs_sender: None, sender, got_header: false };

        std::thread::scope(|scope| {

            // start a thread to read from rhs
            let cli_opts = base.opts.clone();
            let right_file = std::mem::take(&mut self.opts.file);
            scope.spawn(move || {
                let file = std::fs::File::open(right_file).unwrap();
                let mut base = base::Base::new(cli_opts);
                right.process_file(file, &mut base, do_callbacks)
            });

            // start a thread to join everything
            let cli_opts = base.opts.clone();
            let opts = self.opts.clone();
            let join = self.join;
            scope.spawn(move || {
                let mut base = base::Base::new(cli_opts);
                base.ofs = rx.recv().unwrap();
                Joiner::default().do_joining(join, &opts, receiver, &mut base);
                base.on_eof();
            });

            // and we will read from lhs
            let result = left.process_file(file, base, do_callbacks);
            drop(left);
            result
        })
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
    fn do_joining(&mut self, join: Join, opts: &Opts, receiver: Receiver<(bool, Row)>, base: &mut base::Base) {
        let fields = (&opts.left_fields, &opts.right_fields);
        let is_fields_set = !fields.0.is_empty() || !fields.1.is_empty();
        let mut stores = (HashMap::new(), HashMap::new());
        let mut headers = (None, None);
        let mut slicers = (ColumnSlicer::new(fields.0, opts.regex), ColumnSlicer::new(fields.1, opts.regex));
        let mut got_headers = false;
        let mut buffer = vec![];

        for (is_left, row) in receiver.iter() {
            if is_left && headers.0.is_none() {
                headers.0 = Some(row);
            } else if !is_left && headers.1.is_none() {
                headers.1 = Some(row);
            } else {
                // non header

                if !got_headers {
                    // stick it in the buffer for later
                    buffer.push(row);
                } else if self.on_row(is_left, row, &mut stores, &slicers, base) {
                    return
                }
            }

            if !got_headers && let Some(headers) = headers.0.as_ref().zip(headers.1.as_ref()) {
                got_headers = true;

                if ! is_fields_set {
                    // get common fields
                    let left: HashSet<_> = headers.0.iter().collect();
                    let right: HashSet<_> = headers.1.iter().collect();
                    let fields: Vec<_> = left.intersection(&right).cloned().cloned().collect();
                    if fields.is_empty() {
                        // default join field is the first
                        slicers.0 = ColumnSlicer::new(&vec!["1".into()], false);
                        slicers.1 = ColumnSlicer::new(&vec!["1".into()], false);
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
                header.append(&mut slicers.0.slice(headers.0, true, true));
                header.append(&mut slicers.1.slice(headers.1, true, true));

        // if self.opts.rename_1 {
        // left = [self.opts.rename_1 % h for h in left]
        // }
        // if self.opts.rename_2:
        // right = [self.opts.rename_2 % h for h in right]

                if base.on_header(header) {
                    return
                }
                // clear out the buffered rows
                // their side must be the other side
                for row in buffer.drain(..) {
                    if self.on_row(!is_left, row, &mut stores, &slicers, base) {
                        return
                    }
                }
            }

        }

        if matches!(join, Join::Left | Join::Outer) {
            for (key, rows) in &stores.0 {
                if !stores.1.contains_key(key) {
                    for row in rows {
                        let row = self.make_row(key, Some(row), None, &slicers);
                        if base.on_row(row) {
                            return
                        }
                    }
                }
            }
        }

        if matches!(join, Join::Right | Join::Outer) {
            for (key, rows) in &stores.1 {
                if !stores.0.contains_key(key) {
                    for row in rows {
                        let row = self.make_row(key, None, Some(row), &slicers);
                        if base.on_row(row) {
                            return
                        }
                    }
                }
            }
        }

    }

    fn make_row(
        &self,
        key: &Row,
        left: Option<&Row>,
        right: Option<&Row>,
        slicers: &(ColumnSlicer, ColumnSlicer),
    ) -> Row {
        let mut new_row = key.clone();
        new_row.extend(std::iter::repeat_n(b"".into(), self.key_len.saturating_sub(key.len())));

        let left_len = if let Some(left) = left {
            let left = &mut slicers.0.slice(left, true, true);
            let left_len = left.len();
            new_row.append(left);
            left_len
        } else {
            0
        };

        if let Some(right) = right {
            new_row.extend(std::iter::repeat_n(b"".into(), self.left_len.saturating_sub(left_len)));
            new_row.append(&mut slicers.1.slice(right, true, true));
        }

        new_row
    }

    fn on_row(
        &mut self,
        is_left: bool,
        row: Row,
        stores: &mut (JoinStore, JoinStore),
        slicers: &(ColumnSlicer, ColumnSlicer),
        base: &mut base::Base,
    ) -> bool {

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
                let row = self.make_row(&key, Some(rows.0), Some(rows.1), slicers);
                if base.on_row(row) {
                    return true
                }
            }
        }
        // put it in the store
        match this.1.entry(key) {
            Entry::Occupied(mut entry) => { entry.get_mut().push(row); },
            Entry::Vacant(entry) => { entry.insert(vec![row]); },
        }
        false
    }
}

struct Child {
    ofs_sender: Option<Sender<base::Ofs>>,
    sender: Sender<(bool, Row)>,
    got_header: bool,
}

impl Child {
    fn notify_header(&mut self, header: &Row, base: &base::Base) {
        self.got_header = true;
        if let Some(ofs_sender) = &self.ofs_sender {
            ofs_sender.send(base.ofs.clone()).unwrap();
        }
        self.sender.send((self.ofs_sender.is_some(), header.clone())).unwrap();
    }
}

impl base::Processor for Child {
    fn on_header(&mut self, base: &mut base::Base, header: Vec<BString>) -> bool {
        self.notify_header(&header, base);
        false
    }

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> bool {
        if !self.got_header {
            self.notify_header(&vec![], base);
        }
        self.sender.send((self.ofs_sender.is_some(), row)).unwrap();
        false
    }
}
