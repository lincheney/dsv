import argparse
from ._column_slicer import _ColumnSlicer

class cut(_ColumnSlicer):
    ''' select columns '''
    parser = argparse.ArgumentParser()
    parser.add_argument('fields', nargs='+')
    parser.add_argument('-x', '--complement', action='store_true')

    header_map = None
    def on_header(self, header):
        self.header_map = self.make_header_map(self.header)
        header = self.slice(header, self.opts.complement)
        return super().on_header(header)
    def on_row(self, row):
        row = self.slice(row, self.opts.complement)
        return super().on_row(row)
