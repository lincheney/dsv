import argparse
import subprocess
import threading
from collections import deque
from ._base import _Base
from ._utils import utf8_type
from ._column_slicer import _ColumnSlicer

class pipe(_ColumnSlicer):
    ''' pipe rows through a processs '''
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument('-k', '--fields', action='append', default=[])
    parser.add_argument('-x', '--complement', action='store_true')
    parser.add_argument('-a', '--append-columns', action='append', default=[], type=utf8_type)
    parser.add_argument('command', nargs='+')
    parser.add_argument('-q', '--no-quote-input', action='store_true')

    def __init__(self, opts):
        super().__init__(opts)
        self.queue = deque()

    proc = None
    proc_stdout = None
    def start_process(self):
        if not self.proc:
            self.proc = subprocess.Popen(self.opts.command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            self.thread = threading.Thread(target=self.read_from_proc, args=(self.proc,), daemon=True)
            self.thread.start()
        return self.proc

    def read_from_proc(self, proc):
        opts = argparse.Namespace(**vars(self.opts))
        opts.no_header = True
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
                for k, v in zip(indices, stdout):
                    row[k] = v

            super().on_row(row)

    def on_header(self, header):
        super().on_header(header + self.opts.append_columns)

    def on_row(self, row):
        input = self.slice(row, self.opts.complement)
        input = self.opts.ofs.join(self.format_columns(input, self.opts.ofs, self.opts.ors, not self.opts.no_quote_input))

        proc = self.start_process()
        proc.stdin.write(input + self.opts.ors)
        proc.stdin.flush()
        self.queue.append(row)

    def on_eof(self):
        if self.proc:
            self.proc.stdin.close()
            self.proc.wait()
            self.thread.join()
        super().on_eof()
