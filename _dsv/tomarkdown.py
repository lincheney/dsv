from ._base import _Base

class tomarkdown(_Base):
    ''' convert to markdown table '''

    ofs = b' | '

    def __init__(self, opts):
        super().__init__(opts)
        self.rows = []

    def on_header(self, header):
        self.on_row(header)

    def on_row(self, row):
        row = [col.replace(b'\\', b'\\\\').replace(b'|', b'\\|').replace(b'`', b'\\`') for col in row]
        self.rows.append(self.format_columns(row, self.ofs, self.opts.ors, quote_output=self.opts.quote_output) + [b''])

    def on_eof(self):
        if not self.rows:
            return
        if self.opts.no_header:
            self.rows.insert([b''] * len(self.rows[0]))
        padding = self.justify(self.rows)

        # rows are already quoted
        self.opts.quote_output = False

        self.opts.ofs = self.ofs
        for i, (p, row) in enumerate(zip(padding, self.rows)):
            self.outfile.write(b'| ')
            super().on_row(row, p)

            if i == 0:
                self.outfile.write(b'| ')
                row = [b'-'*len(col) for col in row]
                super().on_row(row)
