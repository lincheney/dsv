import argparse
from ._base import _Base
from ._column_slicer import _ColumnSlicer
from .exec_ import exec_

class exec_groupby(exec_, _ColumnSlicer):
    ''' aggregate rows using python '''
    name = None
    parser = argparse.ArgumentParser()
    parser.set_defaults(slurp=True)
    parser.add_argument('fields', nargs='*')
    parser.add_argument('-x', '--complement', action='store_true')

    parser.add_argument('script')
    parser.add_argument('-q', '--quiet', action='store_true')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-I', '--ignore-errors', action='store_true')
    group.add_argument('-E', '--remove-errors', action='store_true')
    parser.add_argument('--var', default='X')

    def __init__(self, opts):
        opts.script = [opts.script]
        super().__init__(opts)
        self.groups = {}

    def on_row(self, row, ofs=b'\x00'):
        key = tuple(self.slice(row, self.opts.complement))
        self.groups.setdefault(key, []).append(row)

    def on_eof(self):
        for key, group in self.groups.items():
            if self.header is not None:
                key = dict(zip(self.slice(self.header), key))
                self.exec_on_all_rows(group, K=key)
        _Base.on_eof(self)
