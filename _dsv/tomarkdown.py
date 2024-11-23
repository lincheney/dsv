import subprocess
from ._base import _Base

class tomarkdown(_Base):
    ''' convert to markdown table '''

    def __init__(self, opts):
        opts.header_colour = b'\x1b[1m'
        opts.numbered_columns = False
        # rows are already quoted
        opts.quote_output = False

        super().__init__(opts)
        self.rows = []

    def on_header(self, header):
        return self.on_row(header)

    def prepare_row(self, row):
        row = [b' ' + col.replace(b'\\', b'\\\\').replace(b'|', b'\\|').replace(b'`', b'\\`') + b' ' for col in row]
        return [b''] + row + [b'']

    def on_row(self, row):
        self.rows.append(self.prepare_row(row))

    def on_eof(self):
        if not self.rows:
            return

        if self.opts.drop_header:
            self.opts.drop_header = False
            self.opts.header = 'no'
            del self.rows[0]

        # set a blank header
        if self.opts.header == 'no':
            self.rows.insert(0, [b''] * len(self.rows[0]))

        padding = self.justify(self.rows)
        self.opts.ofs = b'|'
        for i, (pad, row) in enumerate(zip(padding, self.rows)):
            row = [col + b' ' * p for col, p in zip(row, pad + [0])]

            if i == 0:
                if super().on_header(row):
                    break
                row = self.prepare_row(b'-' * (len(col) - 2) for col in row[1:-1])

            if super().on_row(row):
                break

        super().on_eof()

    def start_outfile(self):
        if self.opts.page and self.outfile_proc is None:
            cmd = ['less', '-RX', '--header=2']
            self.outfile_proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
            self.outfile = self.outfile_proc.stdin
        else:
            super().start_outfile()
