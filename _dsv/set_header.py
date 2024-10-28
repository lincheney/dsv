import argparse
from ._base import _Base
from . import _utils

class set_header(_Base):
    ''' sets the header labels '''
    parser = argparse.ArgumentParser()
    parser.add_argument('fields', nargs='*', type=_utils.utf8_type, help='new header names')
    parser.add_argument('--only', action='store_true', help='drop all other header names')
    parser.add_argument('-r', '--rename', nargs=2, action='append', type=_utils.utf8_type, metavar=('A', 'B'), help='rename field A to B')
    parser.add_argument('--auto', nargs='?', const='col%i', type=_utils.utf8_type, help='automatically name the headers, only useful if there is no input header')

    set_header = False

    def on_row(self, row):
        if not self.set_header:
            if self.opts.auto:
                header = [self.opts.auto % (i+1) for i in range(len(row))]
            else:
                header = []
            self.on_header(header)

        self.on_row = super().on_row
        self.on_row(row)

    def on_header(self, header):
        self.set_header = True
        header = header.copy()

        for old, new in (self.opts.rename or ()):
            if old.isdigit():
                i = int(old) - 1
            else:
                try:
                    i = header.index(old)
                except ValueError:
                    continue

            if i >= len(header):
                header += [b''] * (i - len(header) + 1)
            header[i] = new

        if self.opts.fields:
            if self.opts.only:
                header = self.opts.fields
            else:
                header[:len(self.opts.fields)] = self.opts.fields

        self.header = header or None
        if self.header:
            super().on_header(header)
