import argparse
import itertools
from ._base import _Base
from ._shtab import shtab

class cat(_Base):
    ''' concatenate files by row '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--number', action='store_true', help='number all output lines')
    parser.add_argument('-s', '--slurp', action='store_true', help='determine header after reading all input')
    parser.add_argument('files', type=argparse.FileType('rb'), nargs='*', help='other files to concatenate to stdin').complete = shtab.FILE

    def __init__(self, opts):
        self.original_opts = argparse.Namespace(**vars(opts))
        #  self.original_opts.drop_header = True
        super().__init__(opts)
        self.combined_header = []
        self.children = None
        #  self.children = [Child(self).process_file(file, do_yield=True) for file in self.opts.files]
        self.extra_rows = {}
        self.column_mapping = None

    def get_children(self):
        if self.children is None:
            self.children = [Child(self).process_file(file, do_yield=True) for file in self.opts.files]
        return self.children

    def make_combined_header(self, row):
        headers = self.header or [None for _ in row]
        self.column_mapping = []
        have_any_headers = bool(self.header)
        # i got my header, get everyone elses
        for c in self.get_children():
            value = next(c, None)
            if value is not None:
                row, is_header = value
                if is_header:
                    have_any_headers = True
                    mapping = []
                    for c in row:
                        try:
                            ix = headers.index(c)
                        except ValueError:
                            ix = len(headers)
                            headers.append(c)
                        mapping.append(ix)
                    self.column_mapping.append(mapping)
                else:
                    self.column_mapping.append(range(len(headers), len(headers) + len(row)))
                    headers += [None for _ in row]
                    self.extra_rows[c] = row

        if have_any_headers:
            self.combined_header = [h or b'' for h in headers]
        else:
            self.opts.slurp = False
            self.combined_header = self.header

    def on_header(self, header):
        if self.column_mapping is None and self.opts.slurp:
            self.make_combined_header(header)
            header = self.combined_header

        if self.opts.number:
            header = [b'n'] + header
        return super().on_header(header)

    def on_row(self, row):
        if self.column_mapping is None and self.opts.slurp:
            self.make_combined_header(row)
            if self.combined_header is not None:
                if self.on_header(self.combined_header):
                    return True

        if self.opts.number:
            row.insert(0, b'%i' % self.row_count)
        return super().on_row(row)

    def on_eof(self):
        while self.opts.slurp and self.column_mapping is None and self.opts.files:
            # we got no rows, process the next file
            # dont run eof twice
            self.on_eof = lambda: 0
            file = self.opts.files.pop(0)
            list(self.process_file(file))

        for i, child in enumerate(self.get_children()):
            rows = (row for row, is_header in child)
            if child in self.extra_rows:
                rows = itertools.chain([self.extra_rows[child]], rows)

            if self.opts.slurp:
                mapping = self.column_mapping[i]
                for row in rows:
                    template = [b''] * (max(mapping) + 1)
                    for i, c in zip(mapping, row):
                        template[i] = c
                    self.on_row(template)
            else:
                for row in rows:
                    self.on_row(row)

        super().on_eof()

class Child(_Base):
    def __init__(self, parent):
        self.parent = parent
        opts = argparse.Namespace(**vars(parent.opts))
        super().__init__(opts)

    def on_header(self, header):
        pass

    def on_row(self, row):
        pass
