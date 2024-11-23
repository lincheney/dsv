import argparse
from ._base import _Base
from . import _utils

class flip(_Base):
    ''' prints each column on a separate line '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--lines', type=int, metavar='NUM', help='print the first NUM lines')
    parser.add_argument('--row-sep', choices=('never', 'always', 'auto'), default='auto', help='show a separator between the rows')
    parser.set_defaults(ofs=_Base.PRETTY_OUTPUT)

    def __init__(self, opts):
        super().__init__(opts)
        self.opts.row_sep = _utils.resolve_tty_auto(self.opts.row_sep)
        self.count = 0

    def on_header(self, header):
        header = [b'row', b'column']
        if self.header is not None:
            header.append(b'key')
        header.append(b'value')
        return super().on_header(header)

    def on_row(self, row):
        if self.count == 0:
            # first row
            if self.header is None:
                if self.on_header(None):
                    return True

        elif self.opts.row_sep:
            if super().on_row([b'---']):
                return True

        self.count += 1

        for i, value in enumerate(row, 1):
            row = [b'%i' % self.count, b'%i' % i]
            if self.header is not None:
                row.append(self.header[i-1] if i <= len(self.header) else b'')
            row.append(value)
            if super().on_row(row):
                return True

        if self.opts.lines and self.count >= self.opts.lines:
            return True
