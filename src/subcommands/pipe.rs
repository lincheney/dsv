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
    sender: Sender<Vec<BString>>,
    err_receiver: Receiver<Result<()>>,
}

pub struct Handler {
    opts: Opts,
    column_slicer: Option<ColumnSlicer>,
    proc: Option<Proc>,
    header: Option<Vec<BString>>,
    ofs: Ofs,
}

impl Handler {
    pub fn new(opts: Opts, _base: &mut base::Base) -> Result<Self> {
        let column_slicer = if opts.fields.is_empty() {
            None
        } else {
            Some(ColumnSlicer::new(&opts.fields, opts.regex))
        };
        Ok(Self {
            proc: None,
            opts,
            ofs: Ofs::default(),
            column_slicer,
            header: None,
        })
    }
}

impl Handler {
    fn start_proc(&mut self, base: &base::Base) -> Result<&mut Proc> {
        let proc = &mut self.proc;
        if let Some(proc) = proc {
            Ok(proc)
        } else {
            let (sender, receiver) = mpsc::channel();
            let (err_sender, err_receiver) = mpsc::channel();

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
            let header = self.header.take();
            let mut handler = PipeHandler{
                receiver,
                column_slicer: self.column_slicer.clone(),
                append: !self.opts.append_columns.is_empty(),
                complement: self.opts.complement,
                header_len: header.as_ref().map(|h| h.len() - self.opts.append_columns.len()),
            };

            base.scope.spawn(move || {
                let result = (|| {
                    base.opts.header = Some(false);
                    if let Some(header) = header && let Err(e) = handler.on_header(&mut base, header) {
                        crate::utils::chain_errors([
                            Err(e),
                            base.on_eof(),
                        ])?;
                    } else {
                        handler.process_file(stdout, &mut base, base::Callbacks::all())?;
                    }
                    Ok(())
                })();
                err_sender.send(result).unwrap();
            });

            Ok(proc.insert(Proc {
                child,
                stdin,
                sender,
                err_receiver,
            }))
        }
    }
}

impl base::Processor for Handler {

    fn on_ofs(&mut self, base: &mut base::Base, ofs: Ofs) -> MaybeBreak {
        self.ofs = ofs.clone();
        base.on_ofs(ofs)
    }

    fn on_header(&mut self, _base: &mut base::Base, mut header: Vec<BString>) -> Result<()> {
        if let Some(slicer) = &mut self.column_slicer {
            slicer.make_header_map(&header);
        }
        header.extend(self.opts.append_columns.iter().map(|x| x.as_bytes().into()));
        self.header = Some(header);
        Ok(())
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
        proc.stdin.write_all(&input)?;
        // proc.stdin.flush()?;
        Break::when(proc.sender.send(row).is_err())
    }

    fn on_eof(self, base: &mut base::Base) -> Result<bool> {
        let success = if let Some(Proc{mut child, stdin, sender, err_receiver}) = self.proc {
            drop(sender);
            drop(stdin);

            let result1 = err_receiver.recv().unwrap().map(|_| ExitStatus::from_raw(0));
            let result2 = child.wait().map_err(anyhow::Error::new);
            drop(err_receiver);
            crate::utils::chain_errors([result1, result2])?.success()
        } else {
            true
        };
        base.on_eof()?;
        Ok(!success)
    }
}

struct PipeHandler {
    receiver: Receiver<Vec<BString>>,
    column_slicer: Option<ColumnSlicer>,
    complement: bool,
    append: bool,
    header_len: Option<usize>,
}

impl base::Processor for PipeHandler {
    // no headers

    fn on_row(&mut self, base: &mut base::Base, mut row: Vec<BString>) -> Result<()> {
        let mut input = self.receiver.recv().unwrap();
        if self.append {
            if let Some(header_len) = self.header_len && header_len > input.len() {
                input.resize(header_len, b"".into());
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
