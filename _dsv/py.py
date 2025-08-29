import sys
import argparse
import copy
import linecache
from contextlib import contextmanager
from ._base import _Base
from . import _utils
from ._table import Vec, NoNaVec, Table, convert_to_table

FULL_SLICE = slice(None)
MISSING = b''

class py(_Base):
    ''' run python on each row '''

    parent = argparse.ArgumentParser(add_help=False)
    parent.set_defaults(
        expr=False,
        slurp=True,
        ignore_errors=False,
        remove_errors=False,
    )
    parent.add_argument('-q', '--quiet', action='store_true', help='do not print errors')
    parent.add_argument('--var', default='X', help='python variable to use to refer to the data (default: %(default)s)')
    parent.add_argument('-b', '--bytes', action='store_true', help='do not auto convert data to int, str etc, treat everything as bytes')
    parent.add_argument('--no-na', action='store_true', help='do not auto generate NA for invalid operations')

    parser = argparse.ArgumentParser(parents=[parent])
    parser.add_argument('script', nargs='+', help='python statements to run')
    parser.add_argument('-S', '--no-slurp', action='store_false', dest='slurp', help='run python on one row at a time')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-I', '--ignore-errors', action='store_true', help='do not abort on python errors')
    group.add_argument('-E', '--remove-errors', action='store_true', help='remove rows on python errors')

    def __init__(self, opts, filename='<string>', eval_only=False):
        super().__init__(opts)

        self.prelude = compile('\n'.join(opts.script[:-1]), filename, 'exec')
        # add some newlines so that newlines add up
        script = '\n' * (len(opts.script) - 1) + opts.script[-1]
        # test if it is an expr
        self.code = None
        try:
            self.code = compile(script, filename, 'eval')
            self.expr = True
        except SyntaxError:
            if eval_only:
                raise
        # compile outside the above try-except to avoid a double traceback on error
        if self.code is None:
            self.code = compile(script, filename, 'exec')
            self.expr = False

        # load it into the linecache so it shows up in tracebacks
        script = '\n'.join(opts.script)
        linecache.cache[filename] = (len(script), None, (script + '\n').splitlines(True), filename)

        self.count = 0
        self.have_printed_header = False
        self.rows = []
        self.modifiable_header = []
        self.header_numbers = {}
        if self.opts.remove_errors:
            self.opts.ignore_errors = True

    def on_header(self, header):
        self.modifiable_header = header.copy()
        self.header_numbers = {k: i for i, k in enumerate(header)}

    def on_row(self, row):
        if self.opts.slurp:
            self.rows.append(row)
        else:
            return self.exec_per_row(row)

    def on_eof(self):
        if self.opts.slurp:
            self.exec_on_all_rows(self.rows)
        super().on_eof()

    @contextmanager
    def exec_wrapper(self, vars):
        try:
            yield
        except Exception as e:
            if not self.opts.ignore_errors and not self.opts.quiet:
                raise
            if not self.opts.quiet:
                print(f'{type(e).__name__}: {e}', file=sys.stderr)
            if self.opts.remove_errors or (self.opts.ignore_errors and self.expr):
                vars.pop(self.opts.var, None)

    def do_exec(self, rows, code, expr=None, **vars):
        if expr is None:
            expr = self.expr
        if not self.opts.bytes:
            rows = [_utils.parse_value(row) for row in rows]

        vars['H'] = copy.copy(self.header)
        vars['Vec'] = NoNaVec if self.opts.no_na else Vec
        table = vars[self.opts.var] = Table(rows, self.header_numbers, not self.opts.no_na)

        with self.exec_wrapper(vars):
            exec(self.prelude, vars)
            if expr:
                vars[self.opts.var] = eval(code, vars)
            else:
                exec(code, vars)

        result = vars.get(self.opts.var)
        return result, vars, table

    def handle_exec_result(self, result, vars, table):
        if result is None:
            return

        table = convert_to_table(result, None)
        if table is not None:
            headers = table.__headers__
            rows = table.__data__

            if not self.have_printed_header and headers:
                if super().on_header([_utils.to_bytes(k) for k in headers]):
                    return True
                self.have_printed_header = True

            for row in rows:
                if super().on_row([_utils.to_bytes(x) for x in row]):
                    return True

        elif self.expr:
            print(result)
        else:
            raise ValueError(result)

    def exec_per_row(self, row, **vars):
        self.count = self.count + 1
        result, vars, table = self.do_exec([row], self.code, N=self.count, **vars)
        return self.handle_exec_result(result, vars, table)

    def exec_on_all_rows(self, rows, **vars):
        result, vars, table = self.do_exec(rows, self.code, N=len(rows), **vars)
        return self.handle_exec_result(result, vars, table)
