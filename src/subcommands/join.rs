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

        let right_file = std::mem::take(&mut self.opts.file);
        let (sender, receiver) = mpsc::channel();
        let (tx, rx) = mpsc::channel();

        let mut left = Child{ ofs_sender: Some(tx), sender: sender.clone(), got_header: false };
        let mut right = Child{ ofs_sender: None, sender: sender, got_header: false };

        std::thread::scope(|scope| {

            // start a thread to read from rhs
            let cli_opts = base.opts.clone();
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
                let ofs = rx.recv().unwrap();
                let mut base = base::Base::new(cli_opts);
                base.ofs = ofs;
                Joiner::default().do_joining(join, &opts, receiver, &mut base);
                base.on_eof();
            });

            // and we will read from lhs
            // self._process_file(file, base, do_callbacks)
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
        let fields_set = !opts.left_fields.is_empty() || !opts.right_fields.is_empty();
        let mut left_store = HashMap::new();
        let mut right_store = HashMap::new();
        let mut left_header = None;
        let mut right_header = None;
        let mut left_slicer = ColumnSlicer::new(&opts.left_fields, opts.regex);
        let mut right_slicer = ColumnSlicer::new(&opts.left_fields, opts.regex);
        let mut got_headers = false;
        let mut buffer = vec![];

        for (is_left, row) in receiver.iter() {
            if is_left && left_header.is_none() {
                left_header = Some(row);
            } else if !is_left && right_header.is_none() {
                right_header = Some(row);
            } else {
                // non header

                if !got_headers {
                    // stick it in the buffer
                    buffer.push(row);
                } else if self.on_row(is_left, row, (&mut left_store, &mut right_store), (&left_slicer, &right_slicer), base) {
                    return
                }

            }

            if !got_headers && let Some((left, right)) = left_header.as_ref().zip(right_header.as_ref()) {
                got_headers = true;
                if ! fields_set {
                    // get common fields
                    let left: HashSet<_> = left.iter().collect();
                    let right: HashSet<_> = right.iter().collect();
                    let fields: Vec<_> = left.intersection(&right).cloned().cloned().collect();
                    if fields.is_empty() {
                        // default join field is the first
                        left_slicer = ColumnSlicer::new(&vec!["1".into()], false);
                        right_slicer = ColumnSlicer::new(&vec!["1".into()], false);
                    } else {
                        left_slicer = ColumnSlicer::from_names(fields.iter());
                        right_slicer = ColumnSlicer::from_names(fields.iter());
                    }
                }

                // make header maps
                left_slicer.make_header_map(left);
                right_slicer.make_header_map(right);

                // paste the headers together
                let mut header = left_slicer.slice(left, false, true);
                header.append(&mut left_slicer.slice(left, true, true));
                header.append(&mut right_slicer.slice(right, true, true));
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
                    if self.on_row(!is_left, row, (&mut left_store, &mut right_store), (&left_slicer, &right_slicer), base) {
                        return
                    }
                }
            }

        }

        if matches!(join, Join::Left | Join::Outer) {
            for (key, rows) in &left_store {
                if !right_store.contains_key(key) {
                    for row in rows {
                        let mut new_row = key.clone();
                        new_row.extend(std::iter::repeat_n(b"".into(), self.key_len - key.len()));
                        new_row.append(&mut left_slicer.slice(row, true, true));
                        new_row.extend(std::iter::repeat_n(b"".into(), self.right_len));
                        if base.on_row(new_row) {
                            return
                        }
                    }
                }
            }
        }

        if matches!(join, Join::Right | Join::Outer) {
            for (key, rows) in &right_store {
                if !left_store.contains_key(key) {
                    for row in rows {
                        let mut new_row = key.clone();
                        new_row.extend(std::iter::repeat_n(b"".into(), self.key_len - key.len() + self.left_len));
                        new_row.append(&mut right_slicer.slice(row, true, true));
                        if base.on_row(new_row) {
                            return
                        }
                    }
                }
            }
        }

    }

    fn on_row(
        &mut self,
        is_left: bool,
        row: Row,
        stores: (&mut JoinStore, &mut JoinStore),
        slicers: (&ColumnSlicer, &ColumnSlicer),
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

        let left = (slicers.0, stores.0);
        let right = (slicers.1, stores.1);
        let (mut this, mut other) = (left, right);
        if !is_left {
            (this, other) = (other, this);
        }

        let key = this.0.slice(&row, false, true);
        // find any joined rows
        if let Some(other_rows) = other.1.get(&key) {
            for other_row in other_rows {
                let mut new_row = key.clone();
                new_row.append(&mut slicers.0.slice(if is_left { &row } else { other_row }, true, true));
                new_row.append(&mut slicers.1.slice(if !is_left { &row } else { other_row }, true, true));

                // new_row.extend_from_slice
                if base.on_row(new_row) {
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
