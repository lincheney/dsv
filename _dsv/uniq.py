import argparse
from ._column_slicer import _ColumnSlicer
from . import _utils

class uniq(_ColumnSlicer):
    ''' omit repeated lines '''
    parser = argparse.ArgumentParser()
    parser.add_argument('fields', nargs='*', help='check these only fields for uniqueness')
    parser.add_argument('-x', '--complement', action='store_true', help='exclude, rather than include, field names')
    parser.add_argument('-r', '--regex', action='store_true', help='treat fields as regexes')
    parser.add_argument('-c', '--count', action='store_true', help='prefix lines by the number of occurrences')
    parser.add_argument('-C', '--count-column', type=_utils.utf8_type, help='name of column to put the count in')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--group', action='store_true', help='show all items, separating groups with an empty line')
    group.add_argument('--repeated', action='store_true', help='only print duplicate lines, one for each group')
    group.add_argument('--repeated-all', action='store_true', help='print all duplicate lines')

    def __init__(self, opts):
        super().__init__(opts)
        opts.count_column = opts.count_column or (opts.count and b'count') or None
        self.uniq = {}
        self.counts = {}
        self.gather = opts.group or opts.repeated_all
        self.repeated = opts.repeated or opts.repeated_all
        self.print_early = not self.gather and opts.count_column is None
        self.sep = self.get_separator()

    def on_header(self, header):
        if self.opts.count_column is not None:
            header = [self.opts.count_column] + header
        return super().on_header(header)

    def on_row(self, row):
        key = tuple(self.slice(row, self.opts.complement))

        count = self.counts[key] = self.counts.get(key, 0) + 1
        rows = self.uniq.setdefault(key, [])
        if self.print_early:
            if count == (2 if self.repeated else 1):
                return super().on_row(row)
        elif self.gather or not rows:
            rows.append(row)

    def print_groups(self):
        for i, (k, rows) in enumerate(self.uniq.items()):
            count = self.counts[k]
            if self.repeated and count < 2:
                continue
            if self.opts.group and i > 0 and super().on_row(self.sep):
                return
            for row in rows:
                if self.opts.count_column:
                    row.insert(0, b'%i' % count)
                if super().on_row(row):
                    return

    def on_eof(self):
        if not self.print_early:
            self.print_groups()
        super().on_eof()
