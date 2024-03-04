import argparse
from collections import deque
from ._base import _Base
from . import _utils

class head(_Base):
    ''' output the first lines '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--lines', type=_utils.regex_arg_type(r'-?\d+'), default='10')

    def __init__(self, opts):
        super().__init__(opts)
        self.lines = int(self.opts.lines.group(0))
        self.ring = deque((), -self.lines) if self.opts.lines.group(0).startswith('-') else None

    def on_row(self, row):
        if self.ring is None:
            # print first n lines
            super().on_row(row)
            if self.row_count >= self.lines:
                return True
        else:
            # print except for last n lines
            if self.ring and len(self.ring) >= -self.lines:
                super().on_row(self.ring.popleft())
            self.ring.append(row)
