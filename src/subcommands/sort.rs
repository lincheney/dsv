use crate::base;
use bstr::{BString, ByteSlice};
use crate::column_slicer::ColumnSlicer;
use std::process::{Command, Child, ChildStdin, ChildStdout, Stdio};
use std::io::{BufRead, BufReader, BufWriter, Write};
use clap::{Parser, ArgAction};

const ORS: u8 = b'\x00';

#[derive(Parser)]
#[command(about = "sort the rows", disable_help_flag = true, disable_version_flag = true)]
pub struct Opts {
    #[arg(required_unless_present = "old_style_fields", help = "sort based only on these fields")]
    fields: Vec<String>,
    #[arg(short = 'k', long = "fields", value_name = "fields", help = "sort based only on these fields")]
    old_style_fields: Vec<String>,
    #[arg(short = 'x', long, action = ArgAction::SetTrue, help = "exclude, rather than include, field names")]
    complement: bool,
    #[arg(long, action = ArgAction::SetTrue, help = "treat fields as regexes")]
    regex: bool,
    #[arg(short = 'b', long, action = ArgAction::SetTrue, help = "ignore leading blanks")]
    ignore_leading_blanks: bool,
    #[arg(long, action = ArgAction::SetTrue, help = "consider only blanks and alphanumeric characters")]
    dictionary_order: bool,
    #[arg(short = 'f', long, action = ArgAction::SetTrue, help="fold lower case to upper case characters")]
    ignore_case: bool,
    #[arg(short = 'g', long, action = ArgAction::SetTrue, help="compare according to general numerical value")]
    general_numeric_sort: bool,
    #[arg(short = 'i', long, action = ArgAction::SetTrue, help="consider only printable characters")]
    ignore_nonprinting: bool,
    #[arg(short = 'M', long, action = ArgAction::SetTrue, help="sort by month name e.g. JAN < DEC")]
    month_sort: bool,
    #[arg(short = 'h', long, action = ArgAction::SetTrue, help="compare human readable numbers e.g. 4K < 2G")]
    human_numeric_sort: bool,
    #[arg(short = 'n', long, action = ArgAction::SetTrue, help="compare according to string numerical value")]
    numeric_sort: bool,
    #[arg(short = 'R', long, action = ArgAction::SetTrue, help="shuffle, but group identical keys")]
    random_sort: bool,
    #[arg(short = 'r', long, action = ArgAction::SetTrue, help="sort in reverse order")]
    reverse: bool,
    #[arg(short = 'V', long, action = ArgAction::SetTrue, help="natural sort of version numbers within text")]
    version_sort: bool,
}

struct Proc {
    child: Child,
    stdin: BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
}

pub struct Handler {
    proc: Option<Proc>,
    opts: Opts,
    ofs: base::Ofs,
    column_slicer: ColumnSlicer,
    rows: Vec<Option<Vec<BString>>>,
}

impl base::Processor<Opts> for Handler {
    fn new(mut opts: Opts) -> Self {
        opts.fields.extend(opts.old_style_fields.iter().flat_map(|x| x.split(",")).map(|x| x.into()));
        let column_slicer = ColumnSlicer::new(&opts.fields, opts.regex);
        Self {
            proc: None,
            opts,
            ofs: base::Ofs::Plain(b"\t".into()),
            column_slicer,
            rows: vec![],
        }
    }

    fn on_header(&mut self, base: &mut base::Base, header: Vec<BString>) -> bool {
        self.column_slicer.make_header_map(&header);
        base.on_header(header)
    }

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> bool {
        let key = self.column_slicer.slice(&row, self.opts.complement, true);
        let key = base.writer.format_columns(key, &self.ofs, (&[ORS]).into(), true);
        // add row index as first column
        let index = [format!("{}", self.rows.len()).into()];
        let key = index.iter().chain(key.iter());
        let mut key = bstr::join(self.ofs.as_bstr(), key);
        key.push(ORS);

        let proc = self.start_proc();
        proc.stdin.write_all(&key).unwrap();

        self.rows.push(Some(row));
        false
    }

    fn on_eof(&mut self, base: &mut base::Base) {
        if let Some(mut proc) = self.proc.take() {
            drop(proc.stdin.into_inner());

            // get the sorted values

            let ofs = self.ofs.as_bstr();
            let mut buf = vec![];
            while proc.stdout.read_until(ORS, &mut buf).unwrap() > 0 {
                if buf.ends_with(&[ORS]) {
                    buf.pop();
                }
                let index = buf.split_str(ofs).next().unwrap();
                let index: usize = std::str::from_utf8(index).unwrap().parse().unwrap();
                let row = self.rows[index].take().unwrap();
                if base.on_row(row) {
                    break
                }
                buf.clear();
            }
            proc.child.wait().unwrap();
        }
        base.on_eof()
    }
}

impl Handler {
    fn start_proc(&mut self) -> &mut Proc {
        self.proc.get_or_insert_with(|| {
            let mut cmd = Command::new("sort");
            cmd.args(["-z", "-k2"]);
            if self.opts.ignore_leading_blanks { cmd.arg("--ignore-leading-blanks"); }
            if self.opts.dictionary_order { cmd.arg("--dictionary-order"); }
            if self.opts.ignore_case { cmd.arg("--ignore-case"); }
            if self.opts.general_numeric_sort { cmd.arg("--general-numeric-sort"); }
            if self.opts.ignore_nonprinting { cmd.arg("--ignore-nonprinting"); }
            if self.opts.month_sort { cmd.arg("--month-sort"); }
            if self.opts.human_numeric_sort { cmd.arg("--human-numeric-sort"); }
            if self.opts.numeric_sort { cmd.arg("--numeric-sort"); }
            if self.opts.random_sort { cmd.arg("--random-sort"); }
            if self.opts.reverse { cmd.arg("--reverse"); }
            if self.opts.version_sort { cmd.arg("--version-sort"); }

            let mut child = cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().expect("failed to start sort");
            let stdin = BufWriter::new(child.stdin.take().unwrap());
            let stdout = BufReader::new(child.stdout.take().unwrap());
            Proc { child, stdin, stdout }
        })
    }
}
