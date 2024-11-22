import argparse
from ._column_slicer import _ColumnSlicer

class cut(_ColumnSlicer):
    ''' select columns '''
    parser = argparse.ArgumentParser()
    parser.add_argument('fields', nargs='*', help='select only these fields')
    parser.add_argument('-f', '--fields', metavar='fields', type=lambda x: x.split(','), dest='old_style_fields', help='select only these fields')
    parser.add_argument('-x', '--complement', action='store_true', help='exclude, rather than include, field names')
    parser.add_argument('-r', '--regex', action='store_true', help='treat fields as regexes')

    def __init__(self, opts):
        if not opts.fields and not opts.old_style_fields:
            self.parser.error('error: the following arguments are required: fields')
        opts.fields += opts.old_style_fields or ()

        super().__init__(opts)

    def on_header(self, header):
        self.header_map = self.make_header_map(self.header)
        header = self.slice(header, self.opts.complement)
        return super().on_header(header)

    def on_row(self, row):
        row = self.slice(row, self.opts.complement)
        return super().on_row(row)
