import argparse
from ._base import _Base
from ._column_slicer import _ColumnSlicer
from .exec_ import exec_

class exec_groupby(_ColumnSlicer, exec_):
    ''' aggregate rows using python '''
    name = None
    parser = argparse.ArgumentParser(parents=[exec_.parent])
    parser.set_defaults(slurp=True)
    parser.add_argument('fields', nargs='*')
    parser.add_argument('-x', '--complement', action='store_true')

    parser.add_argument('script')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-I', '--ignore-errors', action='store_true')
    group.add_argument('-E', '--remove-errors', action='store_true')

    def __init__(self, opts):
        opts.script = [opts.script]
        super().__init__(opts)
        self.groups = {}

    def on_row(self, row, ofs=b'\x00'):
        key = tuple(self.slice(row, self.opts.complement))
        self.groups.setdefault(key, []).append(row)

    def on_eof(self):
        header = None

        for key, group in self.groups.items():

            if header is None:
                if self.header is None:
                    header = [str(x) for x in range(1, len(key)+1)]
                else:
                    header = self.header
                    if not self.opts.no_auto_convert:
                        header = self.parse_value(self.header)
                header = self.slice(header)

            if not self.opts.no_auto_convert:
                key = self.parse_value(key)

            key = dict(zip(header, key))
            self.exec_on_all_rows(group, K=key)
        _Base.on_eof(self)
