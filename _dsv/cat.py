import argparse
from ._base import _Base

class cat(_Base):
    ''' concatenate files by row '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--number', action='store_true')
    parser.add_argument('files', type=argparse.FileType('rb'), nargs='*')

    def __init__(self, opts):
        self.original_opts = argparse.Namespace(**vars(opts))
        super().__init__(opts)

    def process_file(self, file):
        for file in [file] + self.opts.files:
            if self.opts.ofs:
                child = _Base(self.original_opts, outfile=self.outfile)
                child.on_row = self.on_row
                child.on_header = self.on_header
                got_row = yield from child.process_file(file)
            else:
                # if no ofs yet (file is empty), keep using this parser
                got_row = yield from super().process_file(file)

            if got_row:
                self.original_opts.drop_header = True

        super().on_eof()

    def on_header(self, header):
        if self.opts.number:
            header = [b'n'] + header
        super().on_header(header)
        # drop all future headers
        self.on_header = lambda h: 0

    def on_row(self, row):
        if self.opts.number:
            row.insert(0, b'%i' % self.row_count)
        super().on_row(row)

    def on_eof(self):
        pass
