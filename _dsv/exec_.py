import sys
import argparse
import linecache
import itertools
import operator
from functools import partial
import math
import statistics
from contextlib import contextmanager
from ._base import _Base
from . import _utils

FULL_SLICE = slice(None)

def to_bytes(x):
    if not isinstance(x, bytes):
        x = str(x).encode('utf8')
    return x

def getattr_to_vec(self, key):
    value = [getattr(x, key) for x in self]
    if all(map(callable, value)):
        return (lambda *a, **kw: Vec(fn(*a, **kw) for fn in value))
    return Vec(value)

def is_list_of(value, types):
    return isinstance(value, list) and all(isinstance(x, types) for x in value)

def apply_slice(data, key, flat=False):
    if isinstance(data, slice):
        data = slice_to_list(data)

    if isinstance(key, slice) or (isinstance(key, int) and flat):
        return data[key]
    if isinstance(key, int):
        return (data[key],) if key < len(data) else ()
    else:
        while key and key[-1] >= len(data):
            key = key[:-1]
        return [data[k] if k < len(data) else b'' for k in key]

def slice_to_list(key):
    return list(range(*key.indices(key.stop)))

class BaseTable:
    def __setattr__(self, key, value):
        self[key] = value
    def __delattr__(self, key):
        del self[key]
    def __getattr__(self, key):
        return self[key]

    def __len__(self):
        return len(self.__data__)

    def __add_col__(self, name):
        self.__headers__[name] = len(self.__headers__)
        return self.__headers__[name]

    def __get_col__(self, col, new=False):
        col = to_bytes(col)
        if new and col not in self.__headers__:
            self.__add_col__(col)
        return self.__headers__[col]

    def __parse_key__(self, key, new=False):
        if isinstance(key, (int, slice)) or is_list_of(key, int) or (is_list_of(key, bool) and len(key) == len(self)):
            k = (key, FULL_SLICE)
        elif isinstance(key, (str, bytes)) or is_list_of(key, (str, bytes, int)):
            k = (FULL_SLICE, key)
        elif not isinstance(key, tuple) or len(key) != 2:
            raise IndexError(key)
        else:
            k = key

        real_rows, real_cols = k

        length = len(self)
        indices = range(length)
        if is_list_of(real_rows, bool) and len(real_rows) == length:
            real_rows = rows = [i for i, x in enumerate(real_rows) if x]
        elif is_list_of(real_rows, int):
            real_rows = rows = [indices[x] for x in real_rows]
        elif isinstance(real_rows, int):
            real_rows = rows = indices[real_rows]
        elif isinstance(real_rows, slice):
            rows = slice(*real_rows.indices(length))
        else:
            raise IndexError(key)

        length = len(self.__headers__ or self.__data__[0])
        indices = range(length)
        if is_list_of(real_cols, (str, bytes, int)):
            real_cols = cols = [indices[x] if isinstance(x, int) else self.__get_col__(x, new) for x in real_cols]
        elif isinstance(real_cols, (str, bytes)):
            real_cols = cols = self.__get_col__(real_cols, new)
        elif isinstance(real_cols, int):
            real_cols = cols = indices[real_cols]
        elif isinstance(real_cols, slice):
            cols = slice(*real_cols.indices(length))
        else:
            raise IndexError(key)

        return real_rows, real_cols, rows, cols

    def __flat__(self):
        if self.__is_row__() or self.__is_column__():
            return self
        return itertools.chain.from_iterable(self)

    def map(self, fn):
        if self.__is_row__() or self.__is_column__():
            return Vec(self).map(fn)
        return Vec(Vec(row).map(fn) for row in self)

    def as_float(self):
        return self.map(_utils.as_float)

    def sum(self):
        return sum(self.__flat__())


class Table(BaseTable):
    def __init__(self, data, headers):
        self.__dict__.update(
            __headers__=headers,
            __data__=data,
        )

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    def __getitem__(self, key):
        _, _, rows, cols = self.__parse_key__(key)

        # get a specific cell
        if isinstance(rows, int) and isinstance(cols, int):
            if cols >= len(self.__data__[rows]):
                return b''
            return self.__data__[rows][cols]

        return Proxy(self, rows, cols)

    def __setitem__(self, key, value):
        real_rows, real_cols, rows, cols = self.__parse_key__(key, new=True)
        # non scalar if value is non scalar and exactly one of rows/cols is non scalar
        non_scalar = isinstance(value, (list, tuple)) and isinstance(rows, int) != isinstance(cols, int)
        rows = apply_slice(self.__data__, rows)

        if real_cols == FULL_SLICE:
            # replace whole rows
            for row in rows:
                row[:] = value if non_scalar else [value] * len(row)
            return

        if isinstance(cols, int):
            cols = (cols,)
        elif isinstance(cols, slice):
            cols = slice_to_list(cols)

        if not non_scalar:
            value = itertools.repeat(value)
        value = iter(value)

        for row in rows:
            for col, v in zip(cols, value):
                if col >= len(row):
                    row += [b''] * (col + 1 - len(row))
                row[col] = v

    def __delitem__(self, key):
        real_rows, real_cols, rows, cols = self.__parse_key__(key, new=True)

        delete_cols = real_rows == FULL_SLICE
        delete_rows = real_cols == FULL_SLICE
        if not delete_cols and not delete_rows:
            raise IndexError(key)

        if delete_rows:
            if isinstance(rows, (int, slice)):
                rows = [rows]
            else:
                rows = set(rows)

            for r in sorted(rows, reverse=True):
                del self.__data__[r]

        if delete_cols:
            if isinstance(cols, (int, slice)):
                cols = [cols]
            else:
                cols = set(cols)

            header = list(self.__headers__.keys())
            for c in sorted(cols, reverse=True):
                if isinstance(c, int):
                    c = slice(c, c+1)

                for row in self.__data__:
                    del row[c]
                del header[c]

            self.__dict__['__headers__'] = {k: i for i, k in enumerate(header)}


class Proxy(BaseTable):
    def __init__(self, parent, rows, cols):
        self.__dict__.update(
            __parent__=parent,
            __rows__=rows,
            __cols__=cols,
            __headers__={k: i for i, k in enumerate(apply_slice(list(parent.__headers__), cols))},
        )

    def __is_row__(self):
        return isinstance(self.__rows__, int)

    def __is_column__(self):
        return isinstance(self.__cols__, int)

    def __add_col__(self, name):
        assert not self.__is_row__() and not self.__is_column__()

        # add it to the parent as well
        num = self.__parent__.__add_col__(name)
        if isinstance(self.__cols__, slice):
            self.__cols__ = slice_to_list(self.__cols__)
        self.__cols__.append(num)

        return super().__add_col__(name)

    @property
    def __data__(self):
        data = self.__parent__.__data__
        data = apply_slice(data, self.__rows__)
        data = [apply_slice(row, self.__cols__, flat=True) for row in data]

        if self.__is_row__():
            return data[0]
        return data

    def __iter__(self):
        return iter(self.__data__)

    def __repr__(self):
        return repr(self.__data__)

    def __getattr__(self, key):
        if self.__is_column__():
            return getattr_to_vec(self, key)
        return super().__getattr__(key)

    def __parse_key__(self, key, new=False):
        real_rows, real_cols, rows, cols = super().__parse_key__(key, new)

        if self.__is_column__():
            if isinstance(key, tuple) or real_cols != FULL_SLICE:
                raise IndexError(key)
            return (apply_slice(self.__rows__, rows, flat=True), self.__cols__)

        if self.__is_row__():
            if isinstance(key, tuple):
                raise IndexError(key)
            elif real_rows != FULL_SLICE:
                cols = rows
            return (self.__rows__, apply_slice(self.__cols__, cols, flat=True))

        return (apply_slice(self.__rows__, rows), apply_slice(self.__cols__, cols))

    def __getitem__(self, key):
        key = self.__parse_key__(key)
        return self.__parent__[key]

    def __setitem__(self, key, value):
        key = self.__parse_key__(key, True)
        self.__parent__[key] = value


class Vec(list):
    def __getattr__(self, key):
        return getattr_to_vec(self, key)

    def map(self, fn):
        return Vec(map(fn, self))

    def as_float(self):
        return self.map(_utils.as_float)

    def sum(self):
        return sum(self)

for fn in ('round', 'floor', 'ceil', 'lt', 'gt', 'le', 'ge', 'eq', 'ne', 'neg', 'pos', 'invert', 'add', 'sub', 'mul', 'matmul', 'truediv', 'floordiv', 'mod', 'divmod', 'lshift', 'rshift', 'and', 'xor', 'or', 'pow', 'index'):
    key = f'__{fn}__'

    if op := getattr(operator, key, None):
        def fn(self, *args, op=op):
            if args and isinstance(args[0], (Vec, Proxy)):
                return Vec(map(op, self, args[0]))
            return Vec(op(x, *args) for x in self)
    else:
        def fn(self, *args, key=key):
            return getattr_to_vec(self, key)(*args)

    setattr(Proxy, key, fn)
    setattr(Vec, key, fn)

for fn in ('truediv', 'floordiv', 'add', 'sub', 'mul', 'mod', 'divmod', 'pow', 'lshift', 'rshift', 'and', 'xor', 'or'):
    key = f'__r{fn}__'
    def fn(self, other, key=f'__{fn}__'):
        return getattr(Vec([other] * len(self)), key)(self)
    setattr(Proxy, key, fn)
    setattr(Vec, key, fn)

for key in ('mean', 'fmean', 'geometric_mean', 'harmonic_mean', 'median', 'median_low', 'median_high', 'median_grouped', 'mode', 'multimode', 'quantiles', 'pstdev', 'pvariance', 'stdev', 'variance'):

    def fn(self, *args, _fn=getattr(statistics, key), **kwargs):
        return _fn(self.__flat__(), *args, **kwargs)

    setattr(Proxy, key, fn)
    setattr(Vec, key, getattr(statistics, key))

for key in ('ceil', 'fabs', 'floor', 'isfinite', 'isinf', 'isnan', 'isqrt', 'prod', 'trunc', 'exp', 'log', 'log2', 'log10', 'sqrt'):

    def fn(self, *args, _fn=getattr(math, key), **kwargs):
        return self.map(partial(_fn, *args, **kwargs))

    setattr(Proxy, key, fn)
    setattr(Vec, key, fn)


class exec_(_Base):
    ''' run python on each row '''
    name = 'exec'

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
        try:
            self.code = compile(script, filename, 'eval')
            self.expr = True
        except SyntaxError:
            if eval_only:
                raise
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
            self.exec_per_row(row)

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

    def parse_value(self, value):
        if isinstance(value, (list, tuple)):
            return [self.parse_value(x) for x in value]

        if value.isdigit():
            return int(value)

        try:
            try:
                value = value.decode('utf8')
            except UnicodeDecodeError:
                return value
            return float(value)
        except ValueError:
            return value

    def do_exec(self, rows, **vars):
        if not self.opts.bytes:
            rows = [self.parse_value(row) for row in rows]

        table = vars[self.opts.var] = Table(rows, self.header_numbers)

        with self.exec_wrapper(vars):
            exec(self.prelude, vars)
            if self.expr:
                vars[self.opts.var] = eval(self.code, vars)
            else:
                exec(self.code, vars)

        result = vars.get(self.opts.var)
        self.handle_exec_result(result, vars, table)

    def convert_to_table(self, value):
        if isinstance(value, Proxy) and not value.__is_row__() and not value.__is_column__():
            data = list(value)
            headers = apply_slice(list(value.__parent__.__headers__), value.__cols__)
            return Table(data, headers)

        if isinstance(value, Table):
            return value

        if isinstance(value, dict):
            columns = [list(v) if isinstance(v, (list, tuple, Proxy)) else [v] for v in value.values()]
            max_rows = max(len(col) for col in columns)
            if any(col and max_rows % len(col) != 0 for col in columns):
                raise ValueError(f'mismatched rows: {value}')
            columns = [col * (max_rows // len(col)) if col else [b''] * max_rows for col in columns]
            data = list(zip(*columns))
            headers = value.keys()
            return Table(data, headers)

    def handle_exec_result(self, result, vars, table):
        table = self.convert_to_table(result)

        if table is not None:
            headers = table.__headers__
            rows = table.__data__

            if not self.have_printed_header and headers:
                super().on_header([to_bytes(k) for k in headers])
                self.have_printed_header = True

            for row in rows:
                super().on_row([to_bytes(x) for x in row])

        elif result is None:
            return
        elif self.expr:
            print(result)
        else:
            raise ValueError(result)

    def exec_per_row(self, row, **vars):
        self.count = self.count + 1
        self.do_exec([row], N=self.count, **vars)

    def exec_on_all_rows(self, rows, **vars):
        self.do_exec(rows, N=len(rows), **vars)
