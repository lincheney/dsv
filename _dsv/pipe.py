import argparse
import itertools
import subprocess
import threading
from collections import deque
from ._base import _Base
from ._utils import utf8_type
from ._column_slicer import _ColumnSlicer

class pipe(_ColumnSlicer):
    ''' pipe rows through a processs '''
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument('-k', '--fields', action='append', default=[], help='pipe only on these fields')
    parser.add_argument('-x', '--complement', action='store_true', help='exclude, rather than include, field names')
    parser.add_argument('-r', '--regex', action='store_true', help='treat fields as regexes')
    parser.add_argument('-a', '--append-columns', action='append', default=[], type=utf8_type, help='append output as extra fields rather than replacing')
    parser.add_argument('-q', '--no-quote-input', action='store_true', help='do not do CSV quoting on the input')
    parser.add_argument('command', nargs='+', help='command to pipe rows through')

    def __init__(self, opts):
        super().__init__(opts)
        self.queue = deque()

    thread = None
    proc = None
    proc_stdout = None
    def start_process(self):
        if not self.proc:
            self.proc = subprocess.Popen(self.opts.command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            self.thread = threading.Thread(target=self.read_from_proc, args=(self.proc,), daemon=True)
            self.thread.start()
        return self.proc

    def stop_process(self):
        if self.proc:
            self.proc.stdin.close()
            self.proc.wait()

    def read_from_proc(self, proc):
        opts = argparse.Namespace(**vars(self.opts))
        opts.header = 'no'
        opts.ors = b'\n'

        for stdout, is_header in _Base(opts).process_file(proc.stdout, do_yield=True, do_callbacks=False):
            if not self.queue:
                continue
            row = self.queue.popleft()

            if self.opts.append_columns:
                left = len(self.header or row)
                extra = len(self.opts.append_columns)

                newrow = row[:min(left, len(row))]
                newrow += [b''] * (left - len(newrow))
                newrow += stdout
                newrow += [b''] * (left + extra - len(newrow))
                newrow += row[left:]
                row = newrow

            else:
                # write the stdout back into the original row
                indices = self.slice(list(range(len(row))), self.opts.complement)
                for k, v in itertools.zip_longest(indices, stdout, fillvalue=b''):
                    row[k] = v

            if super().on_row(row):
                break

        proc.terminate()
        self.stop_process()

    def on_header(self, header):
        return super().on_header(header + self.opts.append_columns)

    def on_row(self, row):
        ofs = b'\t' if self.opts.ofs is self.PRETTY_OUTPUT else self.opts.ofs

        input = self.slice(row, self.opts.complement)
        input = ofs.join(self.format_columns(input, ofs, self.opts.ors, not self.opts.no_quote_input))

        proc = self.start_process()
        try:
            proc.stdin.write(input + self.opts.ors)
            proc.stdin.flush()
        except ValueError as e:
            if e.args[0] == 'write to closed file':
                return True
            raise
        self.queue.append(row)

    def on_eof(self):
        self.stop_process()
        if self.thread:
            self.thread.join()
        super().on_eof()
