use std::collections::VecDeque;
use std::io::{Read, Write};
use bstr::{BString, BStr, ByteSlice};
use anyhow::Result;
use nix::fcntl::{fcntl, FcntlArg, OFlag, FdFlag};
use std::os::fd::{AsFd, RawFd, AsRawFd};

fn make_non_blocking<F: AsFd>(fd: F) -> Result<()> {
    let fd = fd.as_fd();
    if fcntl(fd, FcntlArg::F_SETFL(OFlag::O_NONBLOCK)).is_err()
    || fcntl(fd, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC)).is_err() {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

pub struct Reader<R> {
    inner: R,
    buffer: BString,
    used: usize,
    pub is_eof: bool,
}

impl<R: Read+AsFd> Reader<R> {
    pub fn make_non_blocking(&mut self) -> Result<()> {
        make_non_blocking(&self.inner)
    }

    pub fn get_raw_fd(&self) -> RawFd {
        self.inner.as_fd().as_raw_fd()
    }
}

impl<R: Read> Reader<R> {
    pub fn new(inner: R) -> Self {
        Self{
            inner,
            buffer: vec![].into(),
            used: 0,
            is_eof: false,
        }
    }

    pub fn read(&mut self) -> Result<&mut Self> {
        const READ_AMOUNT: usize = 4096;
        loop {
            // more space
            let new_size = self.used + READ_AMOUNT;
            if self.buffer.len() < new_size {
                self.buffer.resize(new_size, 0);
            }

            let slice = &mut self.buffer[self.used..];
            match self.inner.read(slice) {
                Ok(count) => {
                    self.used += count;
                    if count < slice.len() {
                        self.is_eof = count == 0;
                        break
                    }
                },
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => (),
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(err) => Err(err)?,
            }
        }
        Ok(self)
    }

    pub fn line_reader<'a>(&'a mut self) -> LineReader<'a, R> {
        LineReader{ inner: self, start: 0 }
    }

}

pub struct LineReader<'a, R> {
    inner: &'a mut Reader<R>,
    start: usize,
}

impl<R> Drop for LineReader<'_, R> {
    fn drop(&mut self) {
        if self.inner.used != self.start {
            self.inner.buffer.drain(..self.start);
        }
        self.inner.used -= self.start;
    }
}

impl<R: Read> LineReader<'_, R> {
    pub fn is_eof(&self) -> bool {
        self.inner.is_eof
    }

    pub fn get_line(&mut self, irs: &BStr) -> Option<(&BStr, bool)> {
        let strip_cr = irs == b"\n";

        let slice = &self.inner.buffer[self.start .. self.inner.used];
        if let Some((mut line, rest)) = slice.split_once_str(irs) {
            // got a line
            self.start = self.inner.used - rest.len();
            if strip_cr {
                line = line.strip_suffix(b"\r").unwrap_or(line);
            }
            Some((line.into(), rest.is_empty()))

        // this is the last line - output if eof, otherwise save it for later
        } else if self.inner.is_eof && !slice.is_empty() {
            self.start = self.inner.used;
            Some((slice.into(), true))

        } else {
            None
        }
    }
}

pub struct Writer<W> {
    inner: W,
    buffers: VecDeque<BString>,
    written: usize,
    pub is_eof: bool,
}

impl<W: Write+AsFd> Writer<W> {
    pub fn make_non_blocking(&mut self) -> Result<()> {
        make_non_blocking(&self.inner)
    }

    pub fn get_raw_fd(&self) -> RawFd {
        self.inner.as_fd().as_raw_fd()
    }
}

impl<W: Write> Writer<W> {
    pub fn new(inner: W) -> Self {
        Self{
            inner,
            buffers: VecDeque::new(),
            written: 0,
            is_eof: false,
        }
    }

    pub fn write(&mut self, buffer: BString) {
        self.buffers.push_back(buffer);
    }

    pub fn flush(&mut self) -> Result<bool> {
        // returns true if we can write more

        while let Some(buffer) = self.buffers.front() {
            match self.inner.write(&buffer[self.written..]) {
                Ok(count) => {
                    if count == 0 {
                        self.is_eof = true;
                        return Ok(false)
                    }
                    self.written += count;
                    if self.written == buffer.len() {
                        self.buffers.pop_front();
                        self.written = 0;
                    }
                },
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => (),
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(err) => Err(err)?,
            }
        }

        Ok(!self.buffers.is_empty())
    }
}
