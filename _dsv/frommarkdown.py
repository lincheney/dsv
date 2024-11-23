import sys
import re
from ._base import _Base

class frommarkdown(_Base):
    ''' convert from markdown table '''

    def __init__(self, opts):
        opts.header = 'yes'
        opts.irs = b'\n'
        super().__init__(opts)
        self.looking_for_header_border = True

    def parse_line(self, line, row):
        row = [m.group(0) for m in re.finditer(rb'(\\.|[^|])*', line)]

        if not row or row[0].strip() or row[-1].strip() or row[-2].strip():
            print('invalid markdown table row:', line, file=sys.stderr)
            return (), True

        return [re.sub(rb'\\(.)', rb'\1', x.strip()) for x in row[1:-1:2]], False

    def on_row(self, row):
        if self.looking_for_header_border:
            self.looking_for_header_border = False
            if all(re.fullmatch(br':?-+:?', x) for x in row):
                return

        return super().on_row(row)
