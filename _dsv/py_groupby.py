import argparse
import itertools
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
    parser.add_argument('-k', '--fields', action='append', default=[], help='search only on these fields')
    parser.add_argument('-x', '--complement', action='store_true', help='exclude, rather than include, field names')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-r', '--regex', action='store_true', help='treat fields as regexes')
    group.add_argument('-p', '--python-fields', action='store_true', help='grouping fields are python scripts')
    parser.add_argument('script', nargs='+', help='python statements to run')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-I', '--ignore-errors', action='store_true', help='do not abort on python errors')
    group.add_argument('-E', '--remove-errors', action='store_true', help='remove rows on python errors')

    def __init__(self, opts):
        super().__init__(opts)
        self.rows = []
        if self.opts.python_fields and self.opts.fields:
            self.keys = [compile(script, '<string>', 'eval') for script in self.opts.fields]

    def on_row(self, row, ofs=b'\x00'):
        self.rows.append(row)

    def on_eof(self):

        if self.opts.python_fields and self.opts.fields:
            keys = []
            for script in self.keys:
                result, vars, table = self.do_exec(self.rows, script, expr=True, N=len(self.rows))
                keys.append(result)
            keys = list(itertools.zip_longest(*keys, fillvalue=b''))
            header = self.opts.fields
        else:
            keys = (self.slice(row, self.opts.complement) for row in self.rows)
            if not self.opts.bytes:
                keys = map(_utils.parse_value, keys)
            keys = list(map(tuple, keys))
            header = self.slice(self.header or [], default=lambda i: str(i+1))

        groups = {}
        for k, row in zip(keys, self.rows):
            groups.setdefault(k, []).append(row)

        for key, group in groups.items():
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
