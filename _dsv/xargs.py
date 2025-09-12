import sys
import os
import subprocess
import re
import asyncio
import argparse
from collections import deque
from queue import Queue
import threading
from . import _utils
from ._column_slicer import _ColumnSlicer
from ._base import _Base

class ProcStats:
    total = 0
    succeeded = 0
    finished = 0
    queued = 0

class Logger:
    def __init__(self, parent, keys):
        self.parent = parent
        self.keys = keys

    def log_output(self, values, stderr: bool):
        for v in values:
            if not isinstance(v, bytes):
                v = str(v).encode()
            _Base.on_row(self.parent, self.keys + [v], stderr=stderr)
        self.print_progress_bar()

    def print_progress_bar(self, newline=False, width=40):
        if not self.parent.opts.progress_bar:
            return

        stats = self.parent.stats
        failed = stats.finished - stats.succeeded
        running = stats.total - stats.finished - stats.queued
        bars = [
            divmod(width * stats.succeeded, stats.total),
            divmod(width * failed, stats.total),
            divmod(width * running, stats.total),
            divmod(width * stats.queued, stats.total),
        ]
        for i, b in enumerate(bars):
            if b[0] == 0 and b[1] > 0:
                bars[i] = (1, 0)

        while True:
            current_width = sum(x for x, y in bars)
            if current_width >= width:
                break
            i, (x, y) = max(enumerate(bars), key=lambda ixy: ixy[1][1])
            if y == 0:
                bars[3] = (width - current_width, 0)
                break
            #  round this one up
            bars[i] = (x + 1, 0)

        colour = self.parent.opts.colour
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

        if newline:
            bar += '\n'
        sys.stderr.buffer.write(bar.encode())
        sys.stderr.buffer.flush()

class xargs(_Base):
    ''' build and execute command lines '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '-j', '--max_procs', '--jobs', help='run up to num processes at a time, default is 1')
    parser.add_argument('--progress-bar', choices=('never', 'always', 'auto'), nargs='?', help='print a trailer')
    parser.add_argument('command', nargs='+', type=_utils.utf8_type, help='command and arguments to run')

    def __init__(self, opts):
        opts.command.extend(map(_utils.utf8_type, opts.extras))
        opts.extras = ()
        opts.progress_bar = _utils.resolve_tty_auto(opts.progress_bar or 'auto')
        super().__init__(opts)
        self.header_map = {}
        self.thread = None
        self.queue = Queue()
        self.proc_queue = deque()
        self.placeholder_regex = re.compile(rb"\{\{|\}\}|\{[^}]*}")
        self.stats = ProcStats()
        self.proc_tasks = set()

        self.job_limit = 1
        if opts.max_procs:
            if opts.max_procs.isdigit():
                self.job_limit = int(opts.max_procs)
            elif opts.max_procs.endswith('%') and opts.max_procs[:-1].isdigit():
                self.job_limit = max(1, os.cpu_count() * int(opts.max_procs[:-1]) // 100)
            else:
                self.parser.error(f'error: argument --max-procs/--jobs: invalid value {opts.max_procs!r}')

    def start_loop(self):
        if self.thread is None:
            self.thread = threading.Thread(target=lambda: asyncio.run(self.loop()), daemon=True)
            self.thread.start()

    async def get_from_queue(self):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.queue.get)

    async def loop(self):
        while True:
            row = await self.get_from_queue()
            if row is None:
                break
            self.stats.total += 1
            if len(self.proc_tasks) < self.job_limit:
                self.proc_tasks.add(asyncio.create_task(self.start_proc(row)))
            else:
                self.proc_queue.append(row)
                self.stats.queued = len(self.proc_queue)
        while self.proc_tasks:
            await asyncio.gather(*self.proc_tasks)

    def format_arg(self, match, row):
        text = match.group(0)
        if text == b'{{':
            return b'{'
        elif text == b'}}':
            return b'}'
        elif text == b'{}':
            return row[0]
        elif text[1:-1].isdigit():
            return row[int(text[1:-1])]
        elif text[1:-1] in self.header_map:
            return row[self.header_map[text[1:-1]]]
        else:
            raise ValueError(f'invalid placeholder: {text!r}')

    async def start_proc(self, row):
        logger = Logger(self, row)
        try:
            command = [self.placeholder_regex.sub(lambda m: self.format_arg(m, row), c) for c in self.opts.command]
            proc = await asyncio.create_subprocess_exec(
                *command,
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
            logger.log_output([b'exited with %i' % code], True)
            if code == 0:
                self.stats.succeeded += 1
        except OSError as e:
            logger.log_output([e], True)
        self.stats.finished += 1

        self.proc_tasks.remove(asyncio.current_task())
        # ok we are finished, start the next one
        if self.proc_queue:
            row = self.proc_queue.popleft()
            self.stats.queued = len(self.proc_queue)
            self.proc_tasks.add(asyncio.create_task(self.start_proc(row)))

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
        return super().on_header(header)

    def on_row(self, row):
        self.start_loop()
        self.queue.put(row)

    def on_eof(self):
        self.queue.put(None)
        if self.thread:
            self.thread.join()
        super().on_eof()
