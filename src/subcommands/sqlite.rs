use crate::base;
use bstr::BString;
use std::process::{Command, Child, ChildStdin, ChildStdout, Stdio};
use std::io::{BufReader, BufWriter, Write};
use clap::{Parser};

const DELIM: &str = "\t";

#[derive(Parser)]
#[command(about = "use sql on the data")]
pub struct Opts {
    #[arg(required = true, help = "sql statements to run")]
    sql: Vec<String>,
    #[arg(short = 't', long, default_value = "input", help = "name of sql table")]
    table: String,
}

struct Proc {
    child: Child,
    stdin: BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
}

pub struct Handler {
    proc: Option<Proc>,
    opts: Opts,
    got_header: bool,
}

impl base::Processor<Opts> for Handler {
    fn new(opts: Opts) -> Self {
        Self {
            proc: None,
            got_header: false,
            opts,
        }
    }

    fn process_opts(&mut self, opts: &mut base::BaseOptions, _is_tty: bool) {
        opts.ofs = Some("\t".into());
    }

    fn on_header(&mut self, base: &mut base::Base, header: Vec<BString>) -> bool {
        self.got_header = true;
        self.on_row(base, header)
    }

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> bool {
        const ORS: &[u8] = b"\n";

        if !self.got_header {
            panic!("cannot use sqlite without a header");
        }
        let proc = self.start_proc();
        let row = base.writer.format_columns(row, &base.ofs, ORS.into(), true);
        proc.stdin.write_all(&row.join(DELIM.as_bytes())).unwrap();
        proc.stdin.write_all(ORS).unwrap();
        false
    }

    fn on_eof(&mut self, base: &mut base::Base) {
        if let Some(mut proc) = self.proc.take() {
            drop(proc.stdin.into_inner());
            base.ifs = base::Ifs::Plain(DELIM.into());

            let mut cat = super::cat::Handler::new(std::default::Default::default());
            let _ = cat.process_file(proc.stdout, base, base::Callbacks::all());

            proc.child.wait().unwrap();
        }
        base.on_eof()
    }
}

impl Handler {
    fn start_proc(&mut self) -> &mut Proc {
        self.proc.get_or_insert_with(|| {
            let import_sql = format!(".import /dev/stdin {}", self.opts.table);
            let other_sql = self.opts.sql.join(" ");
            let mut cmd = Command::new("sqlite3");
            cmd.args([
                "-csv", "-header",
                "-separator", DELIM,
                "-cmd", &import_sql,
                "-cmd", &other_sql,
            ]);
            let mut child = cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().expect("failed to start sqlite");
            let stdin = BufWriter::new(child.stdin.take().unwrap());
            let stdout = BufReader::new(child.stdout.take().unwrap());
            Proc { child, stdin, stdout }
        })
    }
}
