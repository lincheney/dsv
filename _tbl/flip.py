import argparse
from ._base import _Base

class flip(_Base):
    ''' prints each column on a separate line '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--lines', type=int)

    def __init__(self, opts):
        super().__init__(opts)

    def on_header(self, header):
        pass
    def on_eof(self):
        pass

    def on_row(self, row):
        self.row_count += 1

        header_width = 0
        colour = self.opts.colour
        if colour and self.header:
            header_width = max(map(len, self.header))

        parts = []
        for i, col in enumerate(row):

            if colour:
                parts.append(self.opts.header_colour)
            parts.append(b'%i' % (i+1))
            if colour:
                parts.append(self.RESET_COLOUR)

            parts.append(b'\t')

            header = self.header[i] if i < len(self.header) else b''

            if colour:
                parts.append(self.opts.header_colour)
            parts.append(header)
            if colour:
                parts.append(self.RESET_COLOUR)
            parts.append(b' ' * max(0, header_width - len(header)))

            parts.append(b'\t')
            parts.append(col)
            parts.append(self.opts.ors)

        self.outfile.write(b''.join(parts))
        self.outfile.write(b'---')
        self.outfile.write(self.opts.ors)

        if self.opts.lines and self.row_count >= self.opts.lines:
            return True
