import argparse
import itertools
from ._base import _Base
from ._column_slicer import _ColumnSlicer
from .exec_ import exec_

class exec_groupby(exec_, _ColumnSlicer):
    ''' aggregate rows using python '''
    name = None
    parser = argparse.ArgumentParser()
    parser.set_defaults(slurp=True)
    parser.add_argument('fields', nargs='*')
    parser.add_argument('-x', '--complement', action='store_true')

    parser.add_argument('script')
    parser.add_argument('-q', '--quiet', action='store_true')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-I', '--ignore-errors', action='store_true')
    group.add_argument('-E', '--remove-errors', action='store_true')

    def __init__(self, opts):
        opts.script = [opts.script]
        super().__init__(opts)
        self.key = []
        self.group = []

    def exec_on_group(self, group, key):
        if self.header is not None:
            key = dict(zip(self.header, key))

        if rows := group and self.exec_on_all_rows(group, key=key):
            if not self.have_printed_header and self.modifiable_header:
                _Base.on_header(self, self.modifiable_header)
            self.have_printed_header = True
            for row in rows:
                _Base.on_row(self, row)

    def on_row(self, row):
        key = self.slice(row, self.opts.complement)
        if key != self.key:
            self.exec_on_group(self.group, self.key)
            self.group.clear()
            self.key = key
        self.group.append(row)

    def on_eof(self):
        self.exec_on_group(self.group, self.key)
        _Base.on_eof(self)
