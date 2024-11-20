import argparse
from ._column_slicer import _ColumnSlicer
from . import _utils

class uniq(_ColumnSlicer):
    ''' omit repeated lines '''
    parser = argparse.ArgumentParser()
    parser.add_argument('fields', nargs='*', help='check these only fields for uniqueness')
    parser.add_argument('-x', '--complement', action='store_true', help='exclude, rather than include, field names')
    parser.add_argument('-c', '--count', action='store_true', help='prefix lines by the number of occurrences')
    parser.add_argument('-C', '--count-column', type=_utils.utf8_type, help='name of column to put the count in')

    def __init__(self, opts):
        super().__init__(opts)
        opts.count_column = opts.count_column or (opts.count and b'count') or None
        self.uniq = {}
        self.counts = {}

    def on_header(self, header):
        if self.opts.count_column is not None:
            header = [self.opts.count_column] + header
        return super().on_header(header)

    def on_row(self, row, ofs=b'\x00'):
        key = tuple(self.slice(row, self.opts.complement))
        self.uniq.setdefault(key, row)
        count = self.counts[key] = self.counts.get(key, 0) + 1
        if self.opts.count_column is None and count == 1:
            super().on_row(row)

    def on_eof(self):
        if self.opts.count_column is not None:
            for k, row in self.uniq.items():
                if self.opts.count_column:
                    row = [b'%i' % self.counts[k]] + row
                super().on_row(row)
        super().on_eof()
