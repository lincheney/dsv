import argparse
from ._column_slicer import _ColumnSlicer
from . import _utils

class uniq(_ColumnSlicer):
    ''' omit repeated lines '''
    parser = argparse.ArgumentParser()
    parser.add_argument('fields', nargs='*', help='check these only fields for uniqueness')
    parser.add_argument('-x', '--complement', action='store_true', help='exclude, rather than include, field names')
    parser.add_argument('-r', '--regex', action='store_true', help='treat fields as regexes')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-c', '--count', action='store_true', help='prefix lines by the number of occurrences')
    group.add_argument('-C', '--count-column', type=_utils.utf8_type, help='name of column to put the count in')
    group.add_argument('--group', action='store_true', help='show all items, separating groups with an empty line')

    def __init__(self, opts):
        super().__init__(opts)
        opts.count_column = opts.count_column or (opts.count and b'count') or None
        self.uniq = {}
        self.counts = {}
        self.sep = self.get_separator()

    def on_header(self, header):
        if self.opts.count_column is not None:
            header = [self.opts.count_column] + header
        return super().on_header(header)

    def on_row(self, row):
        key = tuple(self.slice(row, self.opts.complement))

        if self.opts.group:
            self.uniq.setdefault(key, []).append(row)

        else:
            self.uniq.setdefault(key, row)
            count = self.counts[key] = self.counts.get(key, 0) + 1
            if self.opts.count_column is None and count == 1:
                return super().on_row(row)

    def print_groups(self):
        for i, (k, rows) in enumerate(self.uniq.items()):
            for row in rows:
                if super().on_row(row):
                    return
            if i != len(self.uniq) - 1 and super().on_row(self.sep):
                return

    def on_eof(self):
        if self.opts.group:
            self.print_groups()

        elif self.opts.count_column is not None:
            for k, row in self.uniq.items():
                if self.opts.count_column:
                    row = [b'%i' % self.counts[k]] + row
                if super().on_row(row):
                    break

        super().on_eof()
