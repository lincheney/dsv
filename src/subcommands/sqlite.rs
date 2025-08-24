use anyhow::{Result, Context};
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

impl Handler {
    pub fn new(opts: Opts) -> Result<Self> {
        Ok(Self {
            proc: None,
            got_header: false,
            opts,
        })
    }
}

impl base::Processor for Handler {

    fn on_header(&mut self, base: &mut base::Base, header: Vec<BString>) -> Result<bool> {
        self.got_header = true;
        self.on_row(base, header)
    }

    fn on_row(&mut self, _base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        const ORS: &[u8] = b"\n";

        if !self.got_header {
            panic!("cannot use sqlite without a header");
        }
        let proc = self.start_proc()?;
        let row = crate::writer::format_columns(row, &base::Ofs::Plain(DELIM.as_bytes()), ORS.into(), true).0;
        proc.stdin.write_all(&row.join(DELIM.as_bytes()))?;
        proc.stdin.write_all(ORS)?;
        Ok(false)
    }

    fn on_eof(&mut self, base: &mut base::Base) -> Result<bool> {
        if let Some(mut proc) = self.proc.take() {
            drop(proc.stdin.into_inner());
            base.ifs = base::Ifs::Plain(DELIM.into());

            let mut cat = super::cat::Handler::new(std::default::Default::default())?;
            let _ = cat.process_file(proc.stdout, base, base::Callbacks::all());

            proc.child.wait()?;
        }
        base.on_eof()
    }
}

impl Handler {
    fn start_proc(&mut self) -> Result<&mut Proc> {
        let proc = &mut self.proc;
        if let Some(proc) = proc {
            Ok(proc)
        } else {
            let import_sql = format!(".import /dev/stdin {}", self.opts.table);
            let other_sql = self.opts.sql.join(" ");
            let mut cmd = Command::new("sqlite3");
            cmd.args([
                "-csv", "-header",
                "-separator", DELIM,
                "-cmd", &import_sql,
                "-cmd", &other_sql,
            ]);
            let mut child = cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().context("failed to start sqlite")?;
            let stdin = BufWriter::new(child.stdin.take().unwrap());
            let stdout = BufReader::new(child.stdout.take().unwrap());
            Ok(proc.insert(Proc { child, stdin, stdout }))
        }
    }
}
