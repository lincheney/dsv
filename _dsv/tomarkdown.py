from ._base import _Base

class tomarkdown(_Base):
    ''' convert to markdown table '''

    def on_header(self, header):
        self.on_row(header)

    def on_row(self, row):
        row = [col.replace(b'\\', b'\\\\').replace(b'|', b'\\|').replace(b'`', b'\\`') for col in row]
        self.gathered_rows.append(self.format_columns(row, None, None, quote_output=False) + [b''])

    def on_eof(self):
        if not self.gathered_rows:
            return
        if self.opts.no_header:
            self.gathered_rows.insert([b''] * len(self.gathered_rows[0]))
        padding = self.justify(self.gathered_rows)

        self.opts.ofs = b' | '
        for i, (p, row) in enumerate(zip(padding, self.gathered_rows)):
            self.outfile.write(b'| ')
            super().on_row(row, p)

            if i == 0:
                self.outfile.write(b'| ')
                row = [b'-'*len(col) for col in row]
                super().on_row(row)
