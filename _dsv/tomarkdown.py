import subprocess
from ._base import _Base
from . import _utils

class tomarkdown(_Base):
    ''' convert to markdown table '''

    PRETTY_OUTPUT_DELIM = b'|'

    def __init__(self, opts):
        opts.header_colour = b'\x1b[1m'
        opts.numbered_columns = False
        opts.ofs = self.PRETTY_OUTPUT
        opts.trailer = 'never'

        super().__init__(opts)
        self.rows = []

    def format_columns(self, row, ofs, ors, quote_output):
        if quote_output:
            row = [row[0]] + [(b' ' + col.replace(b'\\', b'\\\\').replace(b'|', b'\\|').replace(b'`', b'\\`') + b' ').ljust(3) for col in row[1:-1]] + [row[-1]]
        return row

    def on_header(self, header):
        return super().on_header([b''] + header + [b''])

    def on_row(self, row):
        if self.header is None or self.opts.drop_header:
            self.header = [b''] * len(row)
            self.opts.drop_header = False
            if self.on_header(self.header):
                return True

        return super().on_row([b''] + row + [b''])

    def write_output(self, row, padding=None, is_header=False):
        if super().write_output(row, padding, is_header):
            return True

        # print the separator
        if is_header:
            row = [_utils.remove_ansi_colour(c) for c in row]
            sep = self.format_columns([b'-' * (len(c) - 2) for c in row], None, None, True)
            return super().write_output(sep)

    def start_outfile(self):
        if self.opts.page and self.outfile_proc is None:
            cmd = ['less', '-RX', '--header=2']
            self.outfile_proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
            self.outfile = self.outfile_proc.stdin
        else:
            super().start_outfile()
