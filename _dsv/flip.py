import argparse
from ._base import _Base
from . import _utils

class flip(_Base):
    ''' prints each column on a separate line '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--lines', type=int)
    parser.add_argument('--row-sep', choices=('never', 'always', 'auto'), default='auto')
    parser.set_defaults(ofs=_Base.PRETTY_OUTPUT)

    def __init__(self, opts):
        super().__init__(opts)
        self.opts.row_sep = _utils.resolve_tty_auto(self.opts.row_sep)
        self.count = 0

    def on_header(self, header):
        header = [b'row', b'column']
        if not self.opts.no_header:
            header.append(b'key')
        header.append(b'value')
        super().on_header(header)

    def on_row(self, row):
        if self.count == 0:
            # first row
            if self.header is None:
                self.on_header(None)
                self.header = []

        elif self.opts.row_sep:
            super().on_row([b'---'])

        self.count += 1

        for i, value in enumerate(row, 1):
            row = [b'%i' % self.count, b'%i' % i]
            if not self.opts.no_header:
                row.append(self.header[i-1] if i <= len(self.header) else b'')
            row.append(value)
            super().on_row(row)

        if self.opts.lines and self.count >= self.opts.lines:
            return True
