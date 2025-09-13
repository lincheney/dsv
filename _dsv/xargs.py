import sys
import os
import subprocess
import re
import stat
import asyncio
import argparse
from collections import deque
from queue import Queue
import threading
from . import _utils
from ._column_slicer import _ColumnSlicer
from ._base import _Base

class FormattingError(Exception):
    pass

def shell_quote(values):
    return b' '.join(
        b"'" + val.replace(b"'", b"'\\''") + b"'"
        if not val or re.search(rb'[^-a-zA-Z0-9_]', val) else
        val
        for val in values
    )

class ProcStats:
    total = 0
    succeeded = 0
    finished = 0
    queued = 0

class Logger:
    def __init__(self, id, parent, keys):
        self.parent = parent
        self.keys = keys
        if self.parent.opts.rainbow_rows:
            self.dark_colour = self.parent.get_rgb(id-1, sat=0.5)
            self.light_colour = self.parent.get_rgb(id-1, sat=0.2)

    def log_output(self, values, stderr: bool):
        for v in values:
            if not isinstance(v, bytes):
                v = str(v).encode()
            row = self.keys.copy() if self.parent.opts.tag else []
            if self.parent.opts.rainbow_rows:
                if row:
                    row[0] = self.dark_colour + row[0]
                v = self.light_colour + v + self.parent.RESET_COLOUR
            row.append(v)
            _Base.on_row(self.parent, row, stderr=stderr)
        self.parent.print_progress()

class Verbosity:
    LOW = 0
    EXIT_CODE = 1
    ALL = 2

class xargs(_Base):
    ''' build and execute command lines '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '-j', '--max_procs', '--jobs', help='run up to num processes at a time, default is 1')
    parser.add_argument('--progress-bar', choices=('never', 'always', 'auto'), nargs='?', help='print a trailer')
    parser.add_argument('--terminal-progress-report', choices=('never', 'always', 'auto'), nargs='?', help='enable conemu progress reporting')
    parser.add_argument('-v', '--verbose', default=0, action='count', help='enable verbose logging')
    parser.add_argument('--rainbow-rows', choices=('never', 'always', 'auto'), nargs='?', help='enable rainbow rows')
    parser.add_argument('--dry-run', action='store_true', help='print the job to run but do not run the job')
    parser.add_argument('--no-tag', action='store_false', dest='tag', help="don't tag lines with the input rows")
    parser.add_argument('-k', '--column', type=_utils.utf8_type, default=b'output', help="new header column name")
    parser.add_argument('-I', '--replace-str', default='{}', help='use the replacement string instead of {}')
    parser.add_argument('command', nargs='*', type=_utils.utf8_type, help='command and arguments to run')

    def should_have_progress_bar(self, fd):
        return _utils.is_tty(fd) and ( _utils.is_tty(1) or not stat.S_ISFIFO(os.fstat(1).st_mode))

    def __init__(self, opts):
        opts.command.extend(map(_utils.utf8_type, opts.extras))
        opts.extras = ()
        opts.progress_bar = _utils.resolve_tty_auto(opts.progress_bar or 'auto', fd=2, checker=self.should_have_progress_bar)
        opts.rainbow_rows = opts.colour and _utils.resolve_tty_auto(opts.rainbow_rows or 'auto')
        if opts.rainbow_rows:
            opts.rainbow_columns = 'never'
        if len(opts.replace_str) not in (1, 2):
            opts.parser.error('-I/--replace-str: should be 1-2 chars')
        # ermmm only supported on some terminals
        # for now just check for vte even though kitty supports it too
        opts.terminal_progress_report = _utils.resolve_tty_auto(
            opts.terminal_progress_report or 'auto',
            fd=2,
            checker=lambda fd: _utils.is_tty(fd) and os.environ.get('VTE_VERSION', '').isdigit() and int(os.environ['VTE_VERSION']) >= 7900,
        )

        super().__init__(opts)
        self.header_map = {}
        self.thread = None
        self.queue = Queue()
        self.proc_queue = deque()
        self.stats = ProcStats()
        self.proc_tasks = set()

        l = re.escape(opts.replace_str[0]).encode()
        r = re.escape(opts.replace_str[1:] or opts.replace_str[0]).encode()
        self.placeholder_regex = re.compile(rb"(%s%s)|(%s%s)|%s[^%s]*%s" % (l, l, r, r, l, r, r))

        self.job_limit = 1
        if opts.max_procs:
            if opts.max_procs.isdigit():
                self.job_limit = int(opts.max_procs)
            elif opts.max_procs.endswith('%') and opts.max_procs[:-1].isdigit():
                self.job_limit = max(1, os.cpu_count() * int(opts.max_procs[:-1]) // 100)
            else:
                self.parser.error(f'error: argument --max-procs/--jobs: invalid value {opts.max_procs!r}')

        self.print_progress()

    def start_loop(self):
        if self.thread is None:
            self.thread = threading.Thread(target=lambda: asyncio.run(self.loop()), daemon=True)
            self.thread.start()

    async def get_from_queue(self):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.queue.get)

    async def loop(self):
        try:
            while True:
                row = await self.get_from_queue()
                if row is None:
                    break
                self.stats.total += 1
                if self.job_limit == 0 or len(self.proc_tasks) < self.job_limit:
                    self.proc_tasks.add(asyncio.create_task(self.start_proc(row)))
                else:
                    self.proc_queue.append(row)
                    self.stats.queued = len(self.proc_queue)
                self.print_progress()
            while self.proc_tasks:
                await asyncio.gather(*self.proc_tasks)
        except BrokenPipeError:
            pass

    def get_format_arg_index(self, text):
        if not text:
            return 0
        elif text.isdigit():
            return int(text)
        elif text in self.header_map:
            return self.header_map[text]

    def format_arg(self, match, row):
        if match.group(1) is not None:
            return match.group(1)[:1]
        if match.group(2) is not None:
            return match.group(2)[:1]

        text = match.group(0)[1:-1]
        i = self.get_format_arg_index(text)
        if i is not None:
            return row[i]

        i = self.get_format_arg_index(text[:-1])
        if i is not None:
            if text.endswith(b'.'):
                return os.path.splitext(row[i])[0]
            if text.endswith(b'/'):
                return os.path.basename(row[i])

        i = self.get_format_arg_index(text[1:-3])
        if i is not None:
            if text.endswith(b'//'):
                return os.path.dirname(row[i])
            if text.endswith(b'/.'):
                return os.path.splitext(os.path.basename(row[i]))[0]

        raise FormattingError(f'invalid placeholder: {text!r}')

    async def start_proc(self, row):
        logger = Logger(self.stats.total - self.stats.queued, self, row)
        try:
            if not self.opts.command:
                formatted = ['printf', '%s\n'] + row
            elif not any(self.placeholder_regex.search(c) for c in self.opts.command):
                # no arguments are formatted, append the args at the end
                formatted = self.opts.command + row
            else:
                formatted = [self.placeholder_regex.sub(lambda m: self.format_arg(m, row), c) for c in self.opts.command]

            if len(self.opts.command) == 1 and b' ' in self.opts.command[0]:
                formatted = [b'bash', b'-c', formatted[0]]

            if self.opts.dry_run or self.opts.verbose >= Verbosity.ALL:
                logger.log_output([b'starting process: ' + shell_quote(formatted)], True)

            if self.opts.dry_run:
                self.stats.succeeded += 1

            else:
                proc = await asyncio.create_subprocess_exec(
                    *formatted,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    limit=float('inf'),
                )
                await asyncio.gather(
                    self.read_from_stream(logger, proc.stdout, False),
                    self.read_from_stream(logger, proc.stderr, True),
                )
                code = await proc.wait()
                if self.opts.verbose >= Verbosity.ALL or (self.opts.verbose >= Verbosity.EXIT_CODE and code != 0):
                    logger.log_output([b'exited with %i' % code], True)
                if code == 0:
                    self.stats.succeeded += 1
        except (OSError, IOError, FormattingError) as e:
            logger.log_output([e], True)
        self.stats.finished += 1

        self.proc_tasks.remove(asyncio.current_task())
        # ok we are finished, start the next one
        if self.proc_queue:
            row = self.proc_queue.popleft()
            self.stats.queued = len(self.proc_queue)
            self.proc_tasks.add(asyncio.create_task(self.start_proc(row)))
        self.print_progress()

    async def read_from_stream(self, logger, stream, stderr: bool, bufsize=4096):
        buf = b''
        read = True
        while read:
            read = await stream.read(bufsize)
            buf += read
            if buf:
                lines = buf.split(b'\n')
                buf = lines.pop() if read else b''
                logger.log_output(lines, stderr)

    def on_header(self, header):
        self.header_map = _ColumnSlicer.make_header_map(header)
        if not self.opts.tag:
            header.clear()
        header.append(self.opts.column)
        return super().on_header(header)

    def on_row(self, row):
        self.start_loop()
        self.queue.put(row)

    def on_eof(self):
        self.queue.put(None)
        if self.thread:
            self.thread.join()
        super().on_eof()

    def cleanup(self):
        try:
            self.print_progress(cleanup=True)
        finally:
            super().cleanup()

    def print_progress(self, **kwargs):
        self.print_progress_bar(**kwargs)
        self.print_progress_report(**kwargs)

    def print_progress_bar(self, cleanup=False, width=40, **kwargs):
        if not self.opts.progress_bar:
            return

        stats = self.stats
        failed = stats.finished - stats.succeeded
        running = stats.total - stats.finished - stats.queued
        total = max(1, stats.total)
        bars = [
            divmod(width * stats.succeeded, total),
            divmod(width * failed, total),
            divmod(width * running, total),
            divmod(width * stats.queued, total),
        ]
        for i, b in enumerate(bars):
            if b[0] == 0 and b[1] > 0:
                bars[i] = (1, 0)

        while True:
            current_width = sum(x for x, y in bars)
            if current_width == 0:
                bars[3] = (width, 0)
                break
            if current_width == width:
                break
            fn = min if current_width > width else max
            i, (x, y) = fn(filter(lambda ixy: ixy[1] != (0, 0), enumerate(bars)), key=lambda ixy: (ixy[1][1], -ixy[1][0]))
            if current_width > width:
                # make space and go down
                bars[i] = (x - 1, float('inf'))
            else:
                # round this one up
                bars[i] = (x + 1, 0)

        colour = self.opts.stderr_colour
        vars = dict(
            succeeded_colour =  "\x1b[32m" if colour else "",
            failed_colour =  "\x1b[31m"  if colour else "",
            clear =  "\x1b[0m" if colour else "",
            succeeded_len = bars[0][0],
            failed_len = bars[1][0],
            running_len = bars[2][0],
            queued_len = bars[3][0],
            finished = stats.finished,
            total = stats.total,
            failed = failed,
        )

        bar = ''.join((
            "{clear}[{succeeded_colour}{0:",
            "=",
            ">{succeeded_len}}{failed_colour}{0:",
            "=",
            ">{failed_len}}{clear}{0:",
            "=",
            ">{running_len}}{clear}{0:",
            " ",
            ">{queued_len}}] ({finished} / {total}) ({failed} failed)\r",
        )).format('', **vars)

        if cleanup:
            bar += '\n'
        sys.stderr.buffer.write(bar.encode())
        sys.stderr.buffer.flush()

    def print_progress_report(self, cleanup=False, **kwargs):
        if not self.opts.terminal_progress_report:
            return

        if cleanup:
            data = b"\x1b]9;4;0;\x1b\\"
        else:
            failed = self.stats.finished - self.stats.succeeded
            data = b"\x1b]9;4;%i;%.0f\x1b\\" % (
                3 if self.stats.total == 0 # indeterminate
                else 2 if failed == self.stats.finished # error
                else 4 if failed > 0 # warning
                else 1, # normal
                100. * self.stats.finished / max(1, self.stats.total)
            )
        sys.stderr.buffer.write(data)
        sys.stderr.buffer.flush()
