use crate::utils::Break;
use crate::utils::MaybeBreak;
use anyhow::{Result, Context};
use std::sync::mpsc::{self, Sender, Receiver};
use crate::base::{self, Processor, Ofs};
use bstr::{BString, BStr, ByteVec};
use std::process::{Child, Command, ChildStdin, Stdio, ExitStatus};
use std::os::unix::process::ExitStatusExt;
use std::io::{BufReader, BufWriter, Write};
use crate::column_slicer::ColumnSlicer;
use clap::{Parser};

#[derive(Parser, Clone)]
#[command(about = "pipe rows through a process")]
pub struct Opts {
    #[arg(short = 'k', long, help = "pipe only these fields")]
    fields: Vec<String>,
    #[arg(short = 'x', long, help = "exclude, rather than include, field names")]
    complement: bool,
    #[arg(short = 'r', long, help = "treat fields as regexes")]
    regex: bool,
    #[arg(short = 'a', long, help = "append output as extra fields rather than replacing")]
    append_columns: Vec<String>,
    #[arg(short = 'q', long, help = "do not do CSV quoting on the input")]
    no_quote_input: bool,
    #[arg(trailing_var_arg = true, help = "command to pipe rows through")]
    command: Vec<String>,
}

struct Proc {
    child: Child,
    stdin: BufWriter<ChildStdin>,
}

pub struct Handler {
    opts: Opts,
    column_slicer: Option<ColumnSlicer>,
    proc: Option<Proc>,
    ofs: Ofs,
    row_sender: Sender<Vec<BString>>,
    err_sender: Sender<Result<()>>,
    err_receiver: Receiver<Result<()>>,
}

impl Handler {
    pub fn new(opts: Opts, base: &mut base::Base) -> Result<Self> {
        let column_slicer = if opts.fields.is_empty() {
            None
        } else {
            Some(ColumnSlicer::new(&opts.fields, opts.regex))
        };

        let (sender, receiver) = mpsc::channel();
        let (row_sender, row_receiver) = mpsc::channel();
        let (err_sender, err_receiver) = mpsc::channel();

        let joiner = JoinHandler{
            row_receiver,
            column_slicer: column_slicer.clone(),
            append_len: opts.append_columns.len(),
            complement: opts.complement,
            header_len: 0,
        };
        {
            let mut base = base.clone();
            let err_sender = err_sender.clone();
            base.scope.spawn(move || {
                let result = joiner.forward_messages(&mut base, receiver);
                err_sender.send(result).unwrap();
            });
        }

        base.sender = sender;
        Ok(Self {
            proc: None,
            opts,
            ofs: Ofs::default(),
            column_slicer,
            row_sender,
            err_sender,
            err_receiver,
        })
    }
}

impl Handler {
    fn start_proc(&mut self, base: &base::Base) -> Result<&mut Proc> {
        let proc = &mut self.proc;
        if let Some(proc) = proc {
            Ok(proc)
        } else {
            let mut cmd = Command::new(&self.opts.command[0]);
            cmd.args(&self.opts.command[1..]);
            let mut child = cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().context("failed to start process")?;
            let stdin = BufWriter::new(child.stdin.take().unwrap());
            let stdout = BufReader::new(child.stdout.take().unwrap());

            let mut base = base.clone();
            match &self.ofs {
                Ofs::Pretty => base.opts.pretty = true,
                Ofs::Plain(ofs) => base.opts.ofs = Some(ofs.to_string()),
            }
            let err_sender = self.err_sender.clone();
            base.scope.spawn(move || {
                base.opts.header = Some(false);
                let result = PipeHandler{}.process_file(stdout, &mut base, base::Callbacks::all()).map(|_| ());
                err_sender.send(result).unwrap();
            });

            Ok(proc.insert(Proc {
                child,
                stdin,
            }))
        }
    }
}

impl base::Processor for Handler {

    fn on_ofs(&mut self, base: &mut base::Base, ofs: Ofs) -> MaybeBreak {
        self.ofs = ofs.clone();
        base.on_ofs(ofs)
    }

    fn on_header(&mut self, base: &mut base::Base, mut header: Vec<BString>) -> Result<()> {
        if let Some(slicer) = &mut self.column_slicer {
            slicer.make_header_map(&header);
        }
        header.extend(self.opts.append_columns.iter().map(|x| x.as_bytes().into()));
        base.on_header(header)
    }

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> Result<()> {
        let input = self.column_slicer.as_ref().map(|s| s.slice(&row, self.opts.complement, true));
        let ors = base.opts.get_ors();
        let ofs: &BStr = match &self.ofs {
            Ofs::Pretty => b"\t".into(),
            Ofs::Plain(ofs) => ofs.as_ref(),
        };
        let mut input: BString = bstr::join(ofs, input.as_ref().unwrap_or(&row)).into();
        input.push_str(ors);

        let proc = self.start_proc(base)?;
        let end_stdin = match proc.stdin.write_all(&input) {
            Ok(_) => false,
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => true,
            Err(e) => Err(e)?,
        };
        // proc.stdin.flush()?;
        Break::when(self.row_sender.send(row).is_err() || end_stdin)
    }

    fn on_eof(self, base: &mut base::Base) -> Result<bool> {
        let Self{proc, row_sender, err_receiver, ..} = self;
        drop(row_sender);

        let success = if let Some(Proc{mut child, stdin}) = proc {
            drop(stdin);

            let result1 = err_receiver.recv().unwrap().map(|_| ExitStatus::from_raw(0));
            let result2 = child.wait().map_err(anyhow::Error::new);
            crate::utils::chain_errors([result1, result2])?.success()
        } else {
            true
        };

        base.on_eof()?;
        err_receiver.recv().unwrap()?;
        Ok(!success)
    }
}

struct PipeHandler { }
impl base::Processor for PipeHandler {}

struct JoinHandler {
    row_receiver: Receiver<Vec<BString>>,
    column_slicer: Option<ColumnSlicer>,
    complement: bool,
    append_len: usize,
    header_len: usize,
}

impl base::Processor for JoinHandler {
    fn on_header(&mut self, base: &mut base::Base, header: Vec<BString>) -> Result<()> {
        self.header_len = header.len();
        base.on_header(header)
    }

    fn on_row(&mut self, base: &mut base::Base, mut row: Vec<BString>) -> Result<()> {
        let mut input = self.row_receiver.recv().unwrap();
        if self.append_len > 0 {
            if self.header_len - self.append_len > input.len() {
                input.resize(self.header_len - self.append_len, b"".into());
            }
            input.append(&mut row);
        } else if let Some(slicer) = &self.column_slicer {
            let row_len = row.len();
            for (i, col) in slicer.indices(input.len(), self.complement).zip(row.drain(..).chain(std::iter::repeat(b"".into()))) {
                if i >= input.len() {
                    if i >= row_len {
                        // this is meant to be blank
                        continue
                    }
                    input.resize(i+1, b"".into());
                }
                input[i] = col;
            }
        }
        base.on_row(input)
    }
}
