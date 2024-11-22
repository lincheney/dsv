import argparse
from ._column_slicer import _ColumnSlicer
from . import _utils

class reshape_long(_ColumnSlicer):
    ''' reshape to long format '''
    name = 'reshape-long'

    parser = argparse.ArgumentParser()
    parser.add_argument('fields', nargs='+', help='reshape only these fields')
    parser.add_argument('-x', '--complement', action='store_true', help='exclude, rather than include, field names')
    parser.add_argument('-r', '--regex', action='store_true', help='treat fields as regexes')
    parser.add_argument('-k', '--key', default='key', type=_utils.utf8_type, help='name of the key field')
    parser.add_argument('-v', '--value', default='value', type=_utils.utf8_type, help='name of the value field')

    def on_header(self, header):
        self.header_map = self.make_header_map(self.header)
        header = [self.opts.key, self.opts.value] + self.slice(header, not self.opts.complement)
        return super().on_header(header)

    def on_row(self, row):
        values = self.slice(row, self.opts.complement)
        keys = self.slice(self.header or [str(i).encode('utf8') for i in range(len(row))], self.opts.complement)

        row = self.slice(row, not self.opts.complement)
        for kv in zip(keys, values):
            if super().on_row(list(kv) + row):
                return True
