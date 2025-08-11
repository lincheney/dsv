use std::sync::mpsc::{self, Sender, Receiver};
use crate::base::{self, Processor};
use bstr::{BString, BStr, ByteVec};
use crate::writer::Writer;
use std::process::{Child, Command, ChildStdin, Stdio};
use std::io::{BufReader, BufWriter, Write};
use crate::column_slicer::ColumnSlicer;
use clap::{Parser, ArgAction};

#[derive(Parser, Clone)]
#[command(about = "pipe rows through a process")]
pub struct Opts {
    #[arg(short = 'k', long, help = "pipe only these fields")]
    fields: Vec<String>,
    #[arg(short = 'x', long, action = ArgAction::SetTrue, help = "exclude, rather than include, field names")]
    complement: bool,
    #[arg(short = 'r', long, action = ArgAction::SetTrue, help = "treat fields as regexes")]
    regex: bool,
    #[arg(short = 'a', long, help = "append output as extra fields rather than replacing")]
    append_columns: Vec<String>,
    #[arg(short = 'q', long, action = ArgAction::SetTrue, help = "do not do CSV quoting on the input")]
    no_quote_input: bool,
    #[arg(help = "command to pipe rows through")]
    command: Vec<String>,
}

struct Proc {
    child: Child,
    stdin: BufWriter<ChildStdin>,
    sender: Sender<Vec<BString>>,
    thread: std::thread::JoinHandle<()>,
}

pub struct Handler {
    opts: Opts,
    column_slicer: Option<ColumnSlicer>,
    proc: Option<Proc>,
    header: Vec<BString>,
}

impl Handler {
    pub fn new(opts: Opts) -> Self {
        let column_slicer = if opts.fields.is_empty() {
            None
        } else {
            Some(ColumnSlicer::new(&opts.fields, opts.regex))
        };
        Self {
            proc: None,
            opts,
            column_slicer,
            header: vec![],
        }
    }
}

impl Handler {
    fn start_proc(&mut self, base: &base::Base) -> &mut Proc {
        self.proc.get_or_insert_with(|| {
            let (sender, receiver) = mpsc::channel();

            let mut cmd = Command::new(&self.opts.command[0]);
            cmd.args(&self.opts.command[1..]);
            let mut child = cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().expect("failed to start process");
            let stdin = BufWriter::new(child.stdin.take().unwrap());
            let stdout = BufReader::new(child.stdout.take().unwrap());

            let mut cli_opts = base.opts.clone();
            match &base.ofs {
                base::Ofs::Pretty => cli_opts.pretty = true,
                base::Ofs::Plain(ofs) => cli_opts.ofs = Some(ofs.to_string()),
            }
            let mut handler = PipeHandler{
                receiver,
                column_slicer: self.column_slicer.clone(),
                append: !self.opts.append_columns.is_empty(),
                complement: self.opts.complement,
                header_len: self.header.len() - self.opts.append_columns.len(),
            };
            let header = std::mem::take(&mut self.header);

            let thread = std::thread::spawn(move || {
                cli_opts.header = Some(false);
                let mut base = base::Base::new(cli_opts);
                if base.on_header(header) {
                    base.on_eof();
                    return
                }
                handler.process_file(stdout, &mut base, base::Callbacks::all()).unwrap();
            });

            Proc { child, stdin, sender, thread }
        })
    }
}

impl base::Processor for Handler {

    fn on_header(&mut self, _base: &mut base::Base, mut header: Vec<BString>) -> bool {
        if let Some(slicer) = &mut self.column_slicer {
            slicer.make_header_map(&header);
        }
        header.extend(self.opts.append_columns.iter().map(|x| x.as_bytes().into()));
        self.header = header;
        false
    }

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> bool {
        let input = self.column_slicer.as_ref().map(|s| s.slice(&row, self.opts.complement, true));
        let ors = base.writer.get_ors();
        let ofs: &BStr = match &base.ofs {
            base::Ofs::Pretty => b"\t".into(),
            base::Ofs::Plain(ofs) => ofs.as_ref(),
        };
        let mut input: BString = bstr::join(ofs, input.as_ref().unwrap_or(&row)).into();
        input.push_str(ors);

        let proc = self.start_proc(base);
        proc.stdin.write_all(&input).unwrap();
        proc.sender.send(row).unwrap();
        false
    }

    fn on_eof(&mut self, base: &mut base::Base) {
        if let Some(Proc{mut child, thread, stdin, sender}) = self.proc.take() {
            drop(sender);
            drop(stdin);
            child.wait().unwrap();
            thread.join().unwrap();
        }
        base.on_eof()
    }
}

struct PipeHandler {
    receiver: Receiver<Vec<BString>>,
    column_slicer: Option<ColumnSlicer>,
    complement: bool,
    append: bool,
    header_len: usize,
}

impl base::Processor for PipeHandler {
    // no headers
    fn on_row(&mut self, base: &mut base::Base, mut row: Vec<BString>) -> bool {
        let mut input = self.receiver.recv().unwrap();
        if self.append {
            if self.header_len > input.len() {
                input.resize(self.header_len, b"".into());
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
