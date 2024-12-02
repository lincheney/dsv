import argparse
import subprocess
from ._base import _Base

class sqlite(_Base):
    ''' use sql on the data '''
    parser = argparse.ArgumentParser()
    parser.add_argument('sql', nargs='+', help='sql statements to run')
    parser.add_argument('-t', '--table', default='input', help='name of sql table (default: %(default)s)')

    DELIM = b'\t'

    def __init__(self, opts):
        super().__init__(opts)
        self.proc = None

    def start_proc(self):
        if not self.proc:
            self.proc = subprocess.Popen([
                'sqlite3', '-csv', '-header',
                '-separator', self.DELIM,
                '-cmd', f'.import /dev/stdin {self.opts.table}',
                '-cmd', ' '.join(self.opts.sql),
            ], stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    def on_header(self, header):
        self.start_proc()
        return self.on_row(header)

    def on_row(self, row):
        if not self.proc:
            self.parser.error('Cannot use sqlite without a header')
        self.proc.stdin.write(self.DELIM.join(self.format_columns(row, self.DELIM, b'\n', True)) + b'\n')

    def on_eof(self):
        if self.proc:
            self.proc.stdin.close()
            self.opts.ifs = self.DELIM

            child = _Base(self.opts)
            child.on_row = super().on_row
            child.on_header = super().on_header
            list(child.process_file(self.proc.stdout))

            self.proc.wait()
