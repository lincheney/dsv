import argparse
from ._base import _Base
from ._column_slicer import _ColumnSlicer
from . import _utils
from .py import py
from ._table import BaseTable, Proxy

class py_groupby(_ColumnSlicer, py):
    ''' aggregate rows using python '''
    name = None
    parser = argparse.ArgumentParser(parents=[py.parent])
    parser.set_defaults(slurp=True)
    parser.add_argument('-x', '--complement', action='store_true', help='exclude, rather than include, field names')
    parser.add_argument('-k', '--fields', action='append', default=[], help='search only on these fields')
    parser.add_argument('-r', '--regex', action='store_true', help='treat fields as regexes')
    parser.add_argument('script', nargs='+', help='python statements to run')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-I', '--ignore-errors', action='store_true', help='do not abort on python errors')
    group.add_argument('-E', '--remove-errors', action='store_true', help='remove rows on python errors')

    def __init__(self, opts):
        super().__init__(opts)
        self.groups = {}

    def on_row(self, row, ofs=b'\x00'):
        key = tuple(self.slice(row, self.opts.complement))
        self.groups.setdefault(key, []).append(row)

    def on_eof(self):
        header = None

        for key, group in self.groups.items():

            if header is None:
                header = self.slice(self.header or [], default=lambda i: str(i+1))

            if not self.opts.bytes:
                key = _utils.parse_value(key)

            self.current_key = dict(zip(header, key))
            self.exec_on_all_rows(group, K=self.current_key)
        _Base.on_eof(self)

    def handle_exec_result(self, result, vars, table):
        if self.expr and self.opts.var in vars:
            if not isinstance(result, BaseTable) or (isinstance(result, Proxy) and (result.__is_column__() or result.__is_row__())):
                if not isinstance(result, dict):
                    result = {self.opts.script[-1]: result}
                result = {**self.current_key, **result}

        return super().handle_exec_result(result, vars, table)
