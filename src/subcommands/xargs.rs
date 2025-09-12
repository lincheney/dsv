use crate::writer::{get_rgb};
use crate::column_slicer::{make_header_map};
use regex::bytes::{Regex, Captures};
use std::path::Path;
use anyhow::{Result};
use std::sync::{mpsc};
use crate::base;
use std::ffi::{OsString, OsStr};
use std::os::unix::{ffi::OsStringExt, ffi::OsStrExt, process::ExitStatusExt};
use std::collections::{VecDeque, HashMap, hash_map::Entry};
use std::os::fd::{AsFd, RawFd, AsRawFd};
use std::process::{Child, Command, Stdio, ChildStdout, ChildStderr};
use std::io::{Read};
use bstr::{BString, BStr, ByteSlice, ByteVec};
use clap::{Parser, CommandFactory, ArgAction, error::{ErrorKind, ContextKind, ContextValue}};
use nix::sys::stat::{fstat, SFlag};
use nix::fcntl::{fcntl, FcntlArg, OFlag, FdFlag};
use nix::sys::signal::{kill, SIGTERM};
use std::borrow::Cow;

fn shell_quote<T: AsRef<[u8]>, I: IntoIterator<Item=T>>(values: I) -> BString {
    let mut new: BString = b"".into();
    for (i, val) in values.into_iter().enumerate() {
        if i > 0 {
            new.push(b' ');
        }

        let val = val.as_ref();
        if !val.is_empty() && val.iter().all(|c| matches!(c, b'-' | b'_' | b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9')) {
            new.push_str(val);
        } else {
            new.push(b'\'');
            new.append(&mut val.replace(b"'", b"'\\''"));
            new.push(b'\'');
        }
    }
    new
}

mod verbosity {
    // pub const LOW: u8 = 0;
    pub const EXIT_CODE: u8 = 1;
    pub const ALL: u8 = 2;
}

#[derive(Parser, Default, Clone)]
#[command(about = "build and execute command lines ")]
pub struct Opts {
    #[arg(short = 'I', long, default_value = "{}", help = "use the replacement string instead of {}")]
    replace_str: String,
    #[arg(short = 'p', long, help = "run up to num processes at a time, the default is 1")]
    max_procs: Option<String>,
    #[arg(short, long, overrides_with = "max_procs", help = "run up to num processes at a time, the default is 1")]
    jobs: Option<String>,
    #[arg(long, value_enum, default_value_t = base::AutoChoices::Auto, help = "print a progress bar")]
    progress_bar: base::AutoChoices,
    #[arg(long, value_enum, default_value_t = base::AutoChoices::Auto, help = "enable rainbow rows")]
    rainbow_rows: base::AutoChoices,
    #[arg(short, long, action = ArgAction::Count, help = "enable verbose logging")]
    verbose: u8,
    #[arg(long, help = "print the job to run but do not run the job")]
    dry_run: bool,
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

impl<R: Read+AsFd> BufferedReader<R> {
    fn new(inner: R, token: mio::Token, registry: &mio::Registry) -> Result<Self> {
        let fd = inner.as_fd();
        if fcntl(fd, FcntlArg::F_SETFL(OFlag::O_NONBLOCK)).is_err()
        || fcntl(fd, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC)).is_err() {
            return Err(std::io::Error::last_os_error())?;
        }

        let fd = fd.as_raw_fd();
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
    colour: Option<(BString, BString)>,
    dirty: bool,
}

impl Logger {
    fn write_line(&mut self, base: &base::Base, mut line: BString, stderr: bool) -> Result<bool> {
        self.dirty = true;
        let mut row = self.row.clone();
        if let Some((dark, light)) = &self.colour {
            if !row.is_empty() {
                row[0].insert_str(0, dark);
            }
            line.insert_str(0, light);
        }
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

    fn lookup_key_index(keys: Option<&HashMap<BString, usize>>, val: &[u8]) -> Option<usize> {
        if val.is_empty() {
            Some(0)
        } else if let Ok(x) = std::str::from_utf8(val) && let Ok(x) = x.parse::<usize>() {
            Some(x)
        } else if let Some(&x) = keys.and_then(|keys| keys.get(val)) {
            Some(x)
        } else {
            None
        }
    }

    fn format_arg(
        placeholder_regex: &Regex,
        format: &BStr,
        keys: Option<&HashMap<BString, usize>>,
        values: &[BString],
    ) -> Result<BString> {

        let mut err = Ok(());
        let result = placeholder_regex.replace_all(format, |c: &Captures| -> Cow<[u8]> {
            if let Some(c) = c.get(1).or(c.get(2)) {
                return c.as_bytes()[..1].to_owned().into()
            }

            let text = c.get(0).unwrap().as_bytes();
            let inner = &text[1..text.len()-1];
            let len = inner.len();
            let as_path = |i: usize| Path::new(OsStr::from_bytes(&values[i]));

            if let Some(i) = Self::lookup_key_index(keys, inner) {
                values[i].as_bytes().into()

            } else if inner.ends_with(b".") && let Some(i) = Self::lookup_key_index(keys, &inner[..len-1]) {
                as_path(i).with_extension("").into_os_string().into_encoded_bytes().into()

            } else if inner.ends_with(b"/") && let Some(i) = Self::lookup_key_index(keys, &inner[..len-1]) {
                as_path(i).file_name().map_or(b"" as _, |p| p.as_encoded_bytes()).into()

            } else if inner.ends_with(b"//") && let Some(i) = Self::lookup_key_index(keys, &inner[..len-2]) {
                as_path(i).parent().map_or(b"" as _, |p| p.as_os_str().as_encoded_bytes()).into()

            } else if inner.ends_with(b"/.") && let Some(i) = Self::lookup_key_index(keys, &inner[..len-2]) {
                as_path(i).file_name()
                    .map(|path| Path::new(path).with_extension(""))
                    .map_or(b"".into(), |p| p.into_os_string().into_encoded_bytes().into())

            } else {
                let x: &BStr = text.into();
                err = Err(anyhow::anyhow!("invalid placeholder: {x:?}"));
                b"".into()
            }
        });

        err?;
        Ok(result.into_owned().into())
    }

    fn format_args(
        placeholder_regex: &Regex,
        command: &[String],
        keys: Option<&HashMap<BString, usize>>,
        values: &[BString],
    ) -> Result<Vec<BString>> {

        let command = if command.is_empty() {
            // just print out
            let mut cmd = vec![
                b"printf".into(),
                b"%s\n".into(),
            ];
            cmd.extend(values.iter().cloned());
            cmd
        } else if command.len() == 1 && command[0].contains(' ') {
            // this is probably a shell script
            vec![
                b"bash".into(),
                b"-c".into(),
                Self::format_arg(placeholder_regex, command[0].as_bytes().into(), keys, values)?,
            ]
        } else {
            let mut cmd = vec![];
            for c in command {
                cmd.push(Self::format_arg(placeholder_regex, c.as_bytes().into(), keys, values)?);
            }
            cmd
        };
        Ok(command)
    }

    fn new(
        token: EventMarker,
        command: &[BString],
        registry: &mio::Registry,
    ) -> Result<Self> {

        let mut child = Command::new(OsString::from_vec(command[0].to_vec()))
            .args(command[1..].iter().map(|c| OsString::from_vec(c.to_vec())))
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
        opts: &Opts,
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
                            if opts.verbose >= verbosity::ALL || (!self.success && opts.verbose >= verbosity::EXIT_CODE) {
                                let line = format!("exited with {:?}", r.into_raw() - 255);
                                logger.write_line(base, line.into(), true)?;
                            }
                            Ok(())
                        });
                        crate::utils::chain_errors([r1, r2.map(|_| ()), r3.map(|_| ())])
                    }),
                    // write in the remaining lines
                    self.handle_event(EventType::Stdout, registry, logger, base, opts).map(|_| ()),
                    self.handle_event(EventType::Stderr, registry, logger, base, opts).map(|_| ()),
                ])?;
            },
        }

        Ok(false)
    }

}

impl Drop for Proc {
    fn drop(&mut self) {
        if let Some((child, _pidfd)) = &mut self.child {
            let _ = kill(nix::unistd::Pid::from_raw(child.id() as _), SIGTERM);
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
        if !opts.progress_bar.is_on(false) {
            return false
        }

        const WIDTH: usize = 40;
        let total = self.total.max(1);
        let mut bars = [
            divmod(WIDTH * self.succeeded, total),
            divmod(WIDTH * self.failed(), total),
            divmod(WIDTH * self.running(), total),
            divmod(WIDTH * self.queued, total),
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

        let colour = base.opts.stderr_colour;
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

struct ProcStore {
    opts: Opts,
    queue: VecDeque<Vec<BString>>,
    job_limit: usize,
    placeholder_regex: Regex,
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

        let token = self.stats.started();
        let mut logger = Logger{
            row: values,
            dirty: false,
            colour: if base.opts.colour.is_on(base.opts.is_stdout_tty) && self.opts.rainbow_rows.is_on(base.opts.is_stdout_tty) {
                Some((
                    get_rgb(token-1, None, Some(0.5)),
                    get_rgb(token-1, None, Some(0.2)),
                ))
            } else {
                None
            },
        };
        let command = Proc::format_args(&self.placeholder_regex, &self.opts.command, self.keys.as_ref(), &logger.row)?;
        if self.opts.dry_run || self.opts.verbose >= verbosity::ALL {
            let mut line: BString = b"starting process: ".into();
            line.append(&mut shell_quote(&command));
            logger.write_line(base, line, true)?;
        }

        if self.opts.dry_run {
            self.stats.succeeded += 1;
            self.stats.finished += 1;
        } else {
            let result = Proc::new(
                EventMarker(token),
                &command,
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
        }
        Ok(self.stats.draw_progress_bar(base, &self.opts, false))
    }

    fn handle_event(
        &mut self,
        base: &mut base::Base,
        token: mio::Token,
        registry: &mio::Registry,
    ) -> Result<bool> {

        let (marker, et) = EventMarker::from_token(token);
        let mut entry = match self.inner.entry(marker.0) {
            Entry::Occupied(e) => e,
            Entry::Vacant(_) => unreachable!(),
        };

        let (proc, logger) = entry.get_mut();
        let result = proc.handle_event(et, registry, logger, base, &self.opts);
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
    placeholder_regex: Regex,
) -> Result<()> {

    let mut proc_store = ProcStore{
        opts,
        job_limit,
        placeholder_regex,
        inner: HashMap::new(),
        stats: ProcStats::default(),
        keys: None,
        queue: VecDeque::new(),
    };

    let result = (|| {
        proc_store.stats.draw_progress_bar(base, &proc_store.opts, false);

        let mut poll = mio::Poll::new()?;
        let mut events = mio::Events::with_capacity(255);
        poll.registry().register(&mut receiver, mio::Token(0), mio::Interest::READABLE)?;
        let mut got_eof = false;
        // im ready
        send_notify.send(()).unwrap();

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

    let _ = base.on_eof();
    proc_store.stats.draw_progress_bar(base, &proc_store.opts, true);
    result
}

impl Handler {
    pub fn new(mut opts: Opts, base: &mut base::Base) -> Result<Self> {
        opts.progress_bar = opts.progress_bar.resolve_if(|| {
            base.opts.is_stderr_tty && (
                base.opts.is_stdout_tty
                || fstat(std::io::stdout().as_fd())
                    .map(|s| !SFlag::S_IFIFO.intersects(SFlag::from_bits_truncate(s.st_mode)))
                    .unwrap_or(false)
            )
        });

        opts.rainbow_rows = opts.rainbow_rows.resolve(base.opts.colour.is_on(base.opts.is_stdout_tty) && base.opts.is_stdout_tty);
        if opts.rainbow_rows.is_on(base.opts.is_stdout_tty) {
            base.opts.rainbow_columns = base::AutoChoices::Never;
        }

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

        if !matches!(opts.replace_str.len(), 1 | 2) {
            let cmd = crate::subcommands::Cli::command();
            let mut err = clap::Error::new(ErrorKind::InvalidValue).with_cmd(&cmd);
            err.insert(ContextKind::InvalidArg, ContextValue::String("--replace-str".into()));
            err.insert(ContextKind::InvalidValue, ContextValue::String(opts.replace_str));
            err.insert(ContextKind::ValidValue, ContextValue::Strings(vec!["a string with 1-2 chars".into()]));
            err.exit();
        }
        let left = &opts.replace_str[0..1];
        let right = opts.replace_str.get(1..2).unwrap_or(left);
        let regex = format!(r"({l}{l})|({r}{r})|{l}[^{r}]*{r}", l = regex::escape(left), r = regex::escape(right));
        let placeholder_regex = Regex::new(&regex).unwrap();

        let (sender, receiver) = mio_channel::channel();
        let (err_sender, err_receiver) = mpsc::channel();
        let (send_notify, recv_notify) = mpsc::channel();

        let mut base = base.clone();
        base.scope.spawn(move || {
            let result = proc_loop(
                &mut base,
                receiver,
                send_notify,
                opts,
                job_limit,
                placeholder_regex,
            );
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
