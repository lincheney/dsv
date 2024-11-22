import subprocess
from ._base import _Base

class tomarkdown(_Base):
    ''' convert to markdown table '''

    ofs = b' | '

    def __init__(self, opts):
        super().__init__(opts)
        self.rows = []

    def on_header(self, header):
        return self.on_row(header)

    def on_row(self, row):
        row = [col.replace(b'\\', b'\\\\').replace(b'|', b'\\|').replace(b'`', b'\\`') for col in row]
        self.rows.append(self.format_columns(row, self.ofs, self.opts.ors, quote_output=False) + [b''])

    def on_eof(self):
        if not self.rows:
            return
        if self.opts.header == 'no':
            self.rows.insert(0, [b''] * len(self.rows[0]))
        padding = self.justify(self.rows)

        # rows are already quoted
        self.opts.quote_output = False

        self.start_outfile()

        self.opts.ofs = self.ofs
        for i, (p, row) in enumerate(zip(padding, self.rows)):
            self.outfile.write(b'| ')
            if super().on_row(row, p):
                break

            if i == 0:
                self.outfile.write(b'| ')
                row = [b'-'*len(col) for col in row]
                if super().on_row(row):
                    break

    def start_outfile(self):
        if self.opts.page and self.outfile_proc is None:
            cmd = ['less', '-RX', '--header=2']
            self.outfile_proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
            self.outfile = self.outfile_proc.stdin
