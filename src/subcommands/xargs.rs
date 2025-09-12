use crate::column_slicer::{make_header_map};
use regex::bytes::{Regex, Captures};
use once_cell::sync::Lazy;
use anyhow::{Result};
use std::sync::{mpsc};
use crate::base;
use std::ffi::{OsString};
use std::os::unix::{ffi::OsStringExt, process::ExitStatusExt};
use std::collections::{VecDeque, HashMap, hash_map::Entry};
use std::os::fd::{RawFd, AsRawFd};
use std::process::{Child, Command, Stdio, ChildStdout, ChildStderr};
use std::io::{Read};
use bstr::{BString, BStr, ByteSlice};
use clap::{Parser, CommandFactory, error::{ErrorKind, ContextKind, ContextValue}};

#[derive(Parser, Default, Clone)]
#[command(about = "build and execute command lines ")]
pub struct Opts {
    #[arg(short = 'p', long, help = "run up to num processes at a time, the default is 1")]
    max_procs: Option<String>,
    #[arg(short = 'j', long, overrides_with = "max_procs", help = "run up to num processes at a time, the default is 1")]
    jobs: Option<String>,
    #[arg(long, value_enum, default_value_t = base::AutoChoices::Auto, help = "print a progress bar")]
    progress_bar: base::AutoChoices,
    #[arg(short, long, help = "enable verbose logging")]
    verbose: bool,
    #[arg(trailing_var_arg = true, help = "command and arguments to run")]
    command: Vec<String>,
}

pub struct Handler {
    sender: mio_channel::Sender<Message>,
    err_receiver: mpsc::Receiver<Result<()>>,
}

enum Message {
    Header(Vec<BString>),
    Row(Vec<BString>),
    Eof,
}

struct BufferedReader<R> {
    inner: R,
    fd: Option<RawFd>,
    buffer: BString,
    used: usize,
}

impl<R: Read+AsRawFd> BufferedReader<R> {
    fn new(inner: R, token: mio::Token, registry: &mio::Registry) -> Result<Self> {
        let fd = inner.as_raw_fd();
        unsafe {
            if libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK) != 0 || libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) != 0 {
                return Err(std::io::Error::last_os_error())?;
            }
        }

        registry.register(&mut mio::unix::SourceFd(&fd), token, mio::Interest::READABLE)?;
        Ok(Self{
            fd: Some(fd),
            inner,
            buffer: vec![].into(),
            used: 0,
        })
    }

    fn is_eof(&self) -> bool {
        self.fd.is_none()
    }

    fn close(&mut self, registry: &mio::Registry) -> Result<()> {
        if let Some(fd) = self.fd.take() {
            registry.deregister(&mut mio::unix::SourceFd(&fd))?;
        }
        debug_assert!(self.is_eof());
        Ok(())
    }

    fn read_once(&mut self, registry: &mio::Registry) -> Result<bool> {
        let slice = &mut self.buffer[self.used..];
        match self.inner.read(slice) {
            Ok(0) => {
                // eof
                self.close(registry)?;
                Ok(false)
            },
            Ok(count) => {
                self.used += count;
                Ok(count != slice.len())
            },
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => Ok(true),
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => Ok(false),
            Err(err) => Err(err)?,
        }
    }

    fn read(&mut self, registry: &mio::Registry) -> Result<&mut Self> {
        const READ_AMOUNT: usize = 4096;
        loop {
            // more space
            let new_size = self.used + READ_AMOUNT;
            if self.buffer.len() < new_size {
                self.buffer.resize(new_size, 0);
            }
            if !self.read_once(registry)? {
                return Ok(self)
            }
        }
    }

    fn get_lines(&mut self, irs: &BStr) -> impl Iterator<Item=BString> {
        let mut start = 0;
        std::iter::from_fn(move || {
            let slice = &self.buffer[start..self.used];
            if let Some(i) = slice.find(irs) {
                start = self.used.min(start + i + irs.len());
                Some(slice[..i].into())

            // this is the last line - output if eof, otherwise save it for later
            } else if self.is_eof() {
                self.used = 0;
                start = 0;
                if slice.is_empty() {
                    None
                } else {
                    Some(slice.into())
                }
            } else {
                if self.used != start {
                    self.buffer.drain(..start);
                }
                self.used -= start;
                None
            }
        })
    }
}

static PLACEHOLDERS: Lazy<Regex> = Lazy::new(|| Regex::new(r"\{\{|\}\}|\{[^}]*}").unwrap());

#[derive(Copy, Clone)]
struct EventMarker(usize);
#[derive(Debug, Clone, Copy)]
enum EventType {
    Stdout = 0,
    Stderr = 1,
    Pidfd = 2,
}
impl EventMarker {
    fn for_event_type(self, et: EventType) -> mio::Token {
        mio::Token(self.0 * 3 + et as usize)
    }
    fn from_token(token: mio::Token) -> (Self, EventType) {
        let et = match token.0 % 3 {
            0 => EventType::Stdout,
            1 => EventType::Stderr,
            2 => EventType::Pidfd,
            _ => unreachable!(),
        };
        (Self(token.0 / 3), et)
    }
}

struct Logger {
    row: Vec<BString>,
    dirty: bool,
}

impl Logger {
    fn write_line(&mut self, base: &base::Base, line: BString, stderr: bool) -> Result<bool> {
        self.dirty = true;
        let mut row = self.row.clone();
        row.push(line);
        if stderr {
            Ok(base.write_stderr(row))
        } else {
            base.on_row(row)
        }
    }
}

struct Proc {
    child: Option<(Child, mio_pidfd::PidFd)>,
    success: bool,

    stdout: BufferedReader<ChildStdout>,
    stderr: BufferedReader<ChildStderr>,
}

impl Proc {

    fn format_arg(
        format: &BStr,
        keys: Option<&HashMap<BString, usize>>,
        values: &[BString],
    ) -> Result<OsString> {

        let mut err = Ok(());
        let result = PLACEHOLDERS.replace_all(format, |c: &Captures| -> &BStr {
            match c.get(0).unwrap().as_bytes() {
                b"{{" => b"{".into(),
                b"}}" => b"}".into(),
                x => {
                    if x == b"{}" {
                        values[0].as_ref()
                    } else if let Ok(x) = std::str::from_utf8(&x[1..x.len()-1]) && let Ok(x) = x.parse::<usize>() {
                        values[x].as_ref()
                    } else if let Some(&x) = keys.and_then(|keys| keys.get(&x[1..x.len()-1])) {
                        values[x].as_ref()
                    } else {
                        let x: &BStr = x.into();
                        err = Err(anyhow::anyhow!("invalid placeholder: {x:?}"));
                        b"".into()
                    }
                },
            }
        });

        err?;
        Ok(OsString::from_vec(result.into_owned()))
    }

    fn new(
        token: EventMarker,
        command: &[String],
        keys: Option<&HashMap<BString, usize>>,
        values: &[BString],
        registry: &mio::Registry,
    ) -> Result<Self> {

        let mut cmd = if command.len() == 1 && command[0].contains(' ') {
            // this is probably a shell script
            let mut cmd = Command::new("bash");
            cmd.arg("-c");
            cmd.arg(Self::format_arg(command[0].as_bytes().into(), keys, values)?);
            cmd
        } else {
            let arg0 = Self::format_arg(command[0].as_bytes().into(), keys, values)?;
            let mut cmd = Command::new(arg0);
            for c in &command[1..] {
                cmd.arg(Self::format_arg(c.as_bytes().into(), keys, values)?);
            }
            cmd
        };

        let mut child = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| anyhow::anyhow!("failed to start process: {e}: {}", command[0]))?;

        let pidfd = mio_pidfd::PidFd::new(&child)?;
        let mut proc = Self{
            stdout: BufferedReader::new(child.stdout.take().unwrap(), token.for_event_type(EventType::Stdout), registry)?,
            stderr: BufferedReader::new(child.stderr.take().unwrap(), token.for_event_type(EventType::Stderr), registry)?,
            child: Some((child, pidfd)),
            success: false,
        };
        registry.register(&mut proc.child.as_mut().unwrap().1, token.for_event_type(EventType::Pidfd), mio::Interest::READABLE)?;

        Ok(proc)
    }

    fn exited(&self) -> bool {
        self.child.is_none()
    }

    fn handle_event(
        &mut self,
        et: EventType,
        registry: &mio::Registry,
        logger: &mut Logger,
        base: &mut base::Base,
    ) -> Result<bool> {

        match et {
            EventType::Stdout => {
                for line in self.stdout.read(registry)?.get_lines(base.irs.as_ref()) {
                    if logger.write_line(base, line, false)? {
                        return Ok(true)
                    }
                }
            },
            EventType::Stderr => {
                for line in self.stderr.read(registry)?.get_lines(base.irs.as_ref()) {
                    if logger.write_line(base, line, true)? {
                        return Ok(true)
                    }
                }
            },
            EventType::Pidfd => {
                crate::utils::chain_errors([
                    self.stdout.close(registry),
                    self.stderr.close(registry),
                    self.child.take().map_or(Ok(()), |(mut child, mut pidfd)| {
                        let r1 = registry.deregister(&mut pidfd).map_err(|e| e.into());
                        let r2 = child.wait().map_err(|e| e.into());
                        let r3 = r2.as_ref().map_or(Ok(()), |r| {
                            self.success = r.success();
                            let line = format!("exited with {:?}", r.into_raw() - 255);
                            logger.write_line(base, line.into(), true)?;
                            Ok(())
                        });
                        crate::utils::chain_errors([r1, r2.map(|_| ()), r3.map(|_| ())])
                    }),
                    // write in the remaining lines
                    self.handle_event(EventType::Stdout, registry, logger, base).map(|_| ()),
                    self.handle_event(EventType::Stderr, registry, logger, base).map(|_| ()),
                ])?;
            },
        }

        Ok(false)
    }

}

impl Drop for Proc {
    fn drop(&mut self) {
        if let Some((child, _pidfd)) = &mut self.child {
            unsafe {
                libc::kill(child.id() as _, libc::SIGTERM);
            }
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[derive(Copy, Clone, PartialEq, Default)]
struct ProcStats {
    total: usize,
    succeeded: usize,
    finished: usize,
    queued: usize,
}

fn divmod(x: usize, y: usize) -> (usize, usize) {
    (x / y, x % y)
}

impl ProcStats {
    fn started(&self) -> usize { self.total - self.queued }
    fn failed(&self) -> usize { self.finished - self.succeeded }
    fn running(&self) -> usize { self.started() - self.finished }

    fn draw_progress_bar(&self, base: &mut base::Base, opts: &Opts, newline: bool) -> bool {
        if !opts.progress_bar.is_on(base.opts.is_tty) {
            return false
        }

        const WIDTH: usize = 40;
        let mut bars = [
            divmod(WIDTH * self.succeeded, self.total),
            divmod(WIDTH * self.failed(), self.total),
            divmod(WIDTH * self.running(), self.total),
            divmod(WIDTH * self.queued, self.total),
        ];
        for b in &mut bars {
            if b.0 == 0 && b.1 > 0 {
                // at least length 1
                *b = (1, 0);
            }
        }
        loop {
            let width = bars.iter().map(|(x, _)| x).sum::<usize>();
            if width >= WIDTH {
                break
            }
            let i = bars.iter().enumerate().max_by_key(|(_, (_, y))| y).unwrap().0;
            if bars[i].1 == 0 {
                bars[3] = (WIDTH - width, 0);
                break
            }
            // round this one up
            bars[i] = (bars[i].0 + 1, 0);
        }

        let [(succeeded, _), (failed, _), (running, _), (queued, _)] = bars;

        let colour = base.opts.colour == base::AutoChoices::Always;
        let mut bar = format!(
            concat!(
                "{clear}[{succeeded_colour}{0:",
                "=",
                ">succeeded_len$}{failed_colour}{0:",
                "=",
                ">failed_len$}{clear}{0:",
                "=",
                ">running_len$}{clear}{0:",
                " ",
                ">queued_len$}] ({finished} / {total}) ({failed} failed)\r",
            ),
            "",
            succeeded_colour = if colour { "\x1b[32m" } else { "" },
            failed_colour = if colour { "\x1b[31m" } else { "" },
            clear = if colour { "\x1b[0m" } else { "" },
            succeeded_len = succeeded,
            failed_len = failed,
            running_len = running,
            queued_len = queued,
            finished = self.finished,
            total = self.total,
            failed = self.failed(),
        );
        if newline {
            bar.push('\n');
        }
        base.write_raw_stderr(bar.into(), false)
    }
}

#[derive(Default)]
struct ProcStore {
    opts: Opts,
    queue: VecDeque<Vec<BString>>,
    job_limit: usize,
    inner: HashMap<usize, (Proc, Logger)>,
    stats: ProcStats,
    keys: Option<HashMap<BString, usize>>,
}

impl ProcStore {
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn queue_proc(
        &mut self,
        base: &mut base::Base,
        values: Vec<BString>,
        registry: &mio::Registry,
    ) -> Result<bool> {
        self.stats.total += 1;
        if self.job_limit == 0 || self.inner.len() < self.job_limit {
            // start immediately
            self.start_proc(base, values, registry)
        } else {
            self.queue.push_back(values);
            self.stats.queued = self.queue.len();
            if self.stats.draw_progress_bar(base, &self.opts, false) {
                return Ok(true)
            }
            Ok(false)
        }
    }

    fn start_proc(
        &mut self,
        base: &mut base::Base,
        values: Vec<BString>,
        registry: &mio::Registry,
    ) -> Result<bool> {

        let token = self.stats.started() + 1;
        let mut logger = Logger{ row: values, dirty: false };
        let result = Proc::new(
            EventMarker(token),
            &self.opts.command,
            self.keys.as_ref(),
            &logger.row,
            registry,
        );
        match result {
            Ok(proc) => {
                self.inner.insert(token, (proc, logger));
            },
            Err(e) => {
                self.stats.finished += 1;
                let line = e.to_string();
                if logger.write_line(base, line.into(), true)? {
                    return Ok(true)
                }
            },
        }
        Ok(self.stats.draw_progress_bar(base, &self.opts, false))
    }

    fn handle_event(&mut self, base: &mut base::Base, token: mio::Token, registry: &mio::Registry) -> Result<bool> {
        let (marker, et) = EventMarker::from_token(token);
        let mut entry = match self.inner.entry(marker.0) {
            Entry::Occupied(e) => e,
            Entry::Vacant(_) => unreachable!(),
        };

        let (proc, logger) = entry.get_mut();
        let result = proc.handle_event(et, registry, logger, base);
        if proc.success {
            self.stats.succeeded += 1;
        }
        if proc.exited() {
            self.stats.finished += 1;
        }

        if logger.dirty || proc.success || proc.exited() {
            logger.dirty = false;
            if self.stats.draw_progress_bar(base, &self.opts, false) {
                return Ok(true)
            }
        }

        if proc.exited() {
            entry.remove();
            // can we start a new proc?
            while (self.job_limit == 0 || self.inner.len() < self.job_limit) && let Some(values) = self.queue.pop_front() {
                self.stats.queued = self.queue.len();
                if self.start_proc(base, values, registry)? {
                    return Ok(true)
                }
            }
        }
        result
    }
}

fn proc_loop(
    base: &mut base::Base,
    mut receiver: mio_channel::Receiver<Message>,
    send_notify: mpsc::Sender<()>,
    opts: Opts,
    job_limit: usize,
) -> Result<()> {

    let mut poll = mio::Poll::new()?;
    let mut events = mio::Events::with_capacity(255);

    poll.registry().register(&mut receiver, mio::Token(0), mio::Interest::READABLE)?;

    let mut proc_store = ProcStore{
        opts,
        job_limit,
        ..ProcStore::default()
    };
    let mut got_eof = false;
    // im ready
    send_notify.send(()).unwrap();

    let result = (|| {
        while !got_eof || !proc_store.is_empty() {
            poll.poll(&mut events, None)?;
            for event in &events {
                if event.token() == mio::Token(0) {
                    loop {
                        match receiver.try_recv() {
                            Ok(Message::Header(h)) => {
                                proc_store.keys = Some(make_header_map(&h));
                                if base.on_header(h)? {
                                    return Ok(())
                                }
                            },
                            Ok(Message::Row(row)) => {
                                // spawn a new process
                                if proc_store.queue_proc(base, row, poll.registry())? {
                                    return Ok(())
                                }
                            },
                            Ok(Message::Eof) => {
                                // no more rows
                                poll.registry().deregister(&mut receiver)?;
                                got_eof = true;
                            },
                            Err(mpsc::TryRecvError::Empty) => break,
                            Err(e) => { Err(e)?; },
                        }
                    }
                } else if proc_store.handle_event(base, event.token(), poll.registry())? {
                    return Ok(())
                }
            }
        }
        Ok(())
    })();

    proc_store.stats.draw_progress_bar(base, &proc_store.opts, true);
    result
}

impl Handler {
    pub fn new(opts: Opts, base: &mut base::Base, _is_tty: bool) -> Result<Self> {
        let (sender, receiver) = mio_channel::channel();
        let (err_sender, err_receiver) = mpsc::channel();
        let (send_notify, recv_notify) = mpsc::channel();

        let job_limit = opts.jobs.as_ref().or(opts.max_procs.as_ref()).map_or(1, |jobs| {
            if let Ok(j) = jobs.parse::<usize>() {
                j
            } else if jobs.ends_with('%') && let Ok(j) = jobs[..jobs.len()-1].parse::<usize>() {
                let max = match std::thread::available_parallelism() {
                    Ok(max) => max.get(),
                    Err(e) => {
                        base.write_raw_stderr(format!("{e}\n").into(), false);
                        1
                    },
                };
                (max * j / 100).max(1)
            } else {
                let cmd = crate::subcommands::Cli::command();
                let mut err = clap::Error::new(ErrorKind::InvalidValue).with_cmd(&cmd);
                err.insert(ContextKind::InvalidArg, ContextValue::String("--type".into()));
                err.insert(ContextKind::InvalidValue, ContextValue::String(jobs.clone()));
                err.exit();
            }
        });

        let mut base = base.clone();
        base.scope.spawn(move || {
            let result = proc_loop(&mut base, receiver, send_notify, opts, job_limit);
            let _ = base.on_eof();
            err_sender.send(result).unwrap();
        });
        recv_notify.recv().unwrap();

        Ok(Self {
            sender,
            err_receiver,
        })
    }
}

impl base::Processor for Handler {
    fn on_header(&mut self, _base: &mut base::Base, header: Vec<BString>) -> Result<bool> {
        self.sender.send(Message::Header(header)).unwrap();
        Ok(false)
    }

    fn on_row(&mut self, _base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        self.sender.send(Message::Row(row)).unwrap();
        Ok(false)
    }

    fn on_eof(self, _base: &mut base::Base) -> Result<bool> {
        self.sender.send(Message::Eof).unwrap();
        self.err_receiver.recv().unwrap()?;
        Ok(false)
    }
}
