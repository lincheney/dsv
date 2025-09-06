import argparse
import copy
from ._column_slicer import _ColumnSlicer

class reshape_wide(_ColumnSlicer):
    ''' reshape to wide format '''
    name = 'reshape-wide'

    parser = argparse.ArgumentParser()
    parser.add_argument('value', help='value field (timevar/wide variable)')
    parser.add_argument('fields', nargs='+', help='fields to group by (idvar/long variable)')
    parser.add_argument('-f', '--fields', metavar='fields', type=lambda x: x.split(','), dest='old_style_fields', help='select only these fields')
    parser.add_argument('-x', '--complement', action='store_true', help='exclude, rather than include, field names')
    parser.add_argument('-r', '--regex', action='store_true', help='treat fields as regexes')

    def __init__(self, opts):
        fields = list(opts.fields)
        super().__init__(copy.copy(opts))
        self.__rows = []

        opts.fields = [*fields, opts.value]
        opts.complement = False
        opts.regex = False
        self.wide_slicer = _ColumnSlicer(copy.copy(opts))

        opts = copy.copy(opts)
        opts.fields = [opts.value]
        opts.complement = False
        opts.regex = False
        self.long_slicer = _ColumnSlicer(opts)

    def on_header(self, header):
        self.header_map = self.make_header_map(self.header)
        self.wide_slicer.header_map = self.header_map
        self.long_slicer.header_map = self.header_map

    def on_row(self, row):
        self.__rows.append(row)

    def on_eof(self):
        long_values = set()
        groups = {}
        for row in self.__rows:
            long_value = self.long_slicer.slice(row)[0]
            long_values.add(long_value)
            key = tuple(self.slice(row))
            groups.setdefault(key, []).append((long_value, row))

        long_values = list(long_values)
        if self.header is not None:
            header = self.slice(self.header)
            for h in self.wide_slicer.slice(self.header, True):
                header.extend(b'%s_%s' % (h, v) for v in long_values)
            if super().on_header(header):
                return

        long_values = {v: i for i, v in enumerate(long_values)}
        for key, group in groups.items():
            row = list(key)
            num_columns = max(len(r) for l, r in group)

            for i in self.wide_slicer.slice(list(range(num_columns)), True):
                start = len(row)
                row += [b''] * len(long_values)
                for l, r in group:
                    row[start + long_values[l]] = r[i]

            if super().on_row(row):
                break
