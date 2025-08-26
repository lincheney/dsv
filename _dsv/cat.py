import argparse
from ._base import _Base

class cat(_Base):
    ''' concatenate files by row '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--number', action='store_true', help='number all output lines')
    parser.add_argument('files', type=argparse.FileType('rb'), nargs='*', help='other files to concatenate to stdin')

    def __init__(self, opts):
        self.original_opts = argparse.Namespace(**vars(opts))
        self.original_opts.drop_header = True
        super().__init__(opts)

    def on_header(self, header):
        if self.header is None:
            self.header = header

        if self.opts.number:
            header = [b'n'] + header
        # drop all future headers
        self.on_header = lambda h: 0
        return super().on_header(header)

    def on_row(self, row):
        if self.opts.number:
            row.insert(0, b'%i' % self.row_count)
        return super().on_row(row)

    def on_eof(self):
        for file in self.opts.files:
            if self.header is None and not self.row_count:
                child = _Base(self.opts)
            else:
                child = _Base(self.original_opts)
            child.on_row = self.on_row
            child.on_header = self.on_header
            list(child.process_file(file))

        super().on_eof()
