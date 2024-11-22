import argparse
from collections import deque
from ._base import _Base
from . import _utils

class tail(_Base):
    ''' output the last lines '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--lines', type=_utils.regex_arg_type(r'\+?\d+'), default='10', metavar='NUM', help='print the first NUM lines')

    def __init__(self, opts):
        super().__init__(opts)
        self.lines = int(self.opts.lines.group(0))
        self.ring = deque((), self.lines) if not self.opts.lines.group(0).startswith('+') else None
        self.count = 0

    def on_row(self, row):
        if self.ring is None:
            self.count += 1
            # print except for first n-1 lines
            if self.count >= self.lines:
                return super().on_row(row)
        else:
            # print last n lines
            self.ring.append(row)

    def on_eof(self):
        if self.ring is not None:
            for row in self.ring:
                if super().on_row(row):
                    break
        super().on_eof()
