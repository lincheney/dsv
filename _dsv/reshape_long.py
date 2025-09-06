import re
import copy
import argparse
from ._column_slicer import _ColumnSlicer
from . import _utils

class reshape_long(_ColumnSlicer):
    ''' reshape to long format '''
    name = 'reshape-long'

    parser = argparse.ArgumentParser()
    parser.add_argument('value', type=_utils.utf8_type, help='value field (timevar/wide variable)')
    parser.add_argument('fields', nargs='*', help='reshape only these fields')
    parser.add_argument('-x', '--complement', action='store_true', help='exclude, rather than include, field names')
    parser.add_argument('-r', '--regex', action='store_true', help='treat fields as regexes')
    parser.add_argument('--format', default=r'^(.*?)_(.*)$', help='regex to split wide columns')

    def __init__(self, opts):
        super().__init__(copy.copy(opts))
        self.pattern = re.compile(opts.format.encode())

        opts.fields = [opts.format]
        opts.complement = False
        opts.regex = True
        self.format_slicer = _ColumnSlicer(copy.copy(opts))

        self.wide_header_matches = []

    def wide_slice(self, row, complement=False):
        all_indices = list(range(len(row)))
        indices = self.slice(all_indices, self.opts.complement)
        format_indices = set(self.format_slicer.slice(all_indices))
        indices = [i for i in indices if i in format_indices]
        if complement:
            indices = [i for i in all_indices if i not in indices]
        return indices

    def on_header(self, header):
        self.header_map = self.make_header_map(self.header)
        self.format_slicer.header_map = self.header_map

        group_header = [header[i] for i in self.wide_slice(header, True)]
        wide_header = [header[i] for i in self.wide_slice(header)]
        self.wide_header_matches = [re.match(self.pattern, x) for x in wide_header]
        self.wide_header_matches = [
            (
                m.groupdict().get('key', m.group(1) if len(m.groups()) > 0 else b''),
                m.groupdict().get('value', m.group(2) if len(m.groups()) > 1 else b''),
            )
            for m in self.wide_header_matches
        ]

        self.wide_header = list(set(k for k, v in self.wide_header_matches))
        header = group_header + [self.opts.value] + self.wide_header
        self.wide_header = {v: i for i, v in enumerate(self.wide_header)}
        return super().on_header(header)

    def on_row(self, row):
        keys = [row[i] for i in self.wide_slice(row, True)]
        wide = [row[i] for i in self.wide_slice(row)]

        groups = {}
        for (k, v), x in zip(self.wide_header_matches, wide):
            groups.setdefault(v, [b''] * len(self.wide_header))[self.wide_header[k]] = x

        for k, v in groups.items():
            row = keys + [k] + v
            if super().on_row(row):
                return True
