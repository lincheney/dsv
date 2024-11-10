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
        return (lambda *a, **kw: vec(fn(*a, **kw) for fn in value))
    return vec(value)

def is_list_of(value, types):
    return isinstance(value, (list, tuple)) and all(isinstance(x, types) for x in value)

def apply_slice(data, key, flat=False):
    if isinstance(data, slice):
        data = slice_to_list(data)

    if isinstance(key, slice) or (isinstance(key, int) and flat):
        return data[key]
    if isinstance(key, int):
        return (data[key],)
    else:
        return [data[k] for k in key if k < len(data)]

def slice_to_list(key):
    return list(range(*key.indices(key.stop)))

class Table:
    def __init__(self, data, headers):
        self.__dict__.update(
            __headers__=headers,
            __data__=data,
        )

    def __setattr__(self, key, value):
        self[key] = value
    def __delattr__(self, key):
        del self[key]
    def __getattr__(self, key):
        return self[key]

    def __len__(self):
        return len(self.__data__)

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    def __add_col__(self, name):
        self.__headers__[name] = len(self.__headers__)
        return self.__headers__[name]

    def __get_col__(self, col, new=False):
        if isinstance(col, str):
            col = col.encode('utf8')
        if new and col not in self.__headers__:
            self.__add_col__(col)
        return self.__headers__[col]

    def __parse_key__(self, key, new=False):
        if isinstance(key, (str, bytes)) or is_list_of(key, (str, bytes)):
            key = (FULL_SLICE, key)
        elif isinstance(key, (int, slice)) or (is_list_of(key, bool) and len(key) == len(self)):
            key = (key, FULL_SLICE)
        elif not isinstance(key, tuple) or len(key) != 2:
            raise IndexError(key)

        rows, cols = key

        if is_list_of(rows, bool) and len(rows) == len(self):
            rows = [i for i, x in enumerate(rows) if x]
        elif isinstance(rows, slice):
            rows = slice(*rows.indices(len(self)))

        if is_list_of(cols, (str, bytes)):
            cols = [self.__get_col__(x, new) for x in cols]
        elif isinstance(cols, (str, bytes)):
            cols = self.__get_col__(cols, new)
        elif isinstance(cols, slice):
            cols = slice(*cols.indices(len(self.__headers__)))

        return rows, cols

    def __getitem__(self, key):
        rows, cols = self.__parse_key__(key)

        # get a specific cell
        if isinstance(rows, int) and isinstance(cols, int):
            if cols >= len(self.__data__[rows]):
                return b''
            return self.__data__[rows][cols]

        return proxy(self, rows, cols)

    def __setitem__(self, key, value):
        rows, cols = self.__parse_key__(key, new=True)

        if isinstance(value, (list, tuple)) and isinstance(cols, int) and isinstance(rows, (slice, list, tuple)):
            # zip the value over the rows
            value = iter(value)
        else:
            value = itertools.repeat(value)

        if isinstance(cols, int):
            cols = (cols,)
        elif isinstance(cols, slice):
            cols = slice_to_list(cols)
        rows = apply_slice(self.__data__, rows)

        # set a specific column
        for row in rows:
            for col in cols:
                if col >= len(row):
                    row += [b''] * (col - len(row))
                    row.append(next(value))
                else:
                    row[col] = next(value)

    def __delitem__(self, key):
        rows, cols = self.__parse_key__(key, new=True)

        delete_cols = rows == slice(*FULL_SLICE.indices(len(self.__data__))) and isinstance(key, tuple) and len(key) > 1
        delete_rows = cols == slice(*FULL_SLICE.indices(len(self.__headers__)))
        if not delete_cols and not delete_rows:
            raise IndexError(key)

        if delete_rows:
            # delete rows
            if isinstance(rows, (int, slice)):
                rows = [rows]
            else:
                rows = set(rows)

            for r in sorted(rows, reverse=True):
                del self.__data__[r]

        if delete_cols:
            # delete columns
            header = list(self.__headers__.keys())
            if isinstance(cols, (int, slice)):
                cols = [cols]
            else:
                cols = set(cols)

            for c in sorted(cols, reverse=True):
                for row in self.__data__:
                    del row[c]
                del header[c]

            self.__dict__['__headers__'] = {k: i for i, k in enumerate(header)}

    def __flat__(self):
        if self.__is_row__() or self.__is_column__():
            return self
        return itertools.chain.from_iterable(self)

    def map(self, fn):
        if self.__is_row__() or self.__is_column__():
            return vec(self).map(fn)
        return vec(vec(row).map(fn) for row in self)

    def as_float(self):
        return self.map(_utils.as_float)

    def sum(self):
        return sum(self.__flat__())


class proxy(Table):
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
        rows, cols = super().__parse_key__(key, new)

        if self.__is_column__():
            if isinstance(key, tuple) or cols != FULL_SLICE:
                raise IndexError(key)
            return (apply_slice(self.__rows__, rows), self.__cols__)

        if self.__is_row__():
            if isinstance(key, tuple) or rows != FULL_SLICE:
                raise IndexError(key)
            return (self.__rows__, apply_slice(self.__cols__, cols))

        return (apply_slice(self.__rows__, rows), apply_slice(self.__cols__, cols))

    def __getitem__(self, key):
        key = self.__parse_key__(key)
        return self.__parent__[key]

    def __setitem__(self, key, value):
        key = self.__parse_key__(key, True)
        self.__parent__[key] = value

    def __delitem__(self, key):
        raise TypeError(f"{type(self).__name__!r} object doesn't support item deletion")

class vec(list):
    def __getattr__(self, key):
        return getattr_to_vec(self, key)

    def map(self, fn):
        return vec(map(fn, self))

    def as_float(self):
        return self.map(_utils.as_float)

    def sum(self):
        return sum(self)

for fn in ('round', 'floor', 'ceil', 'lt', 'gt', 'le', 'ge', 'eq', 'ne', 'neg', 'pos', 'invert', 'add', 'sub', 'mul', 'matmul', 'truediv', 'floordiv', 'mod', 'divmod', 'lshift', 'rshift', 'and', 'xor', 'or', 'pow', 'index', 'rtruediv', 'rfloordiv', 'radd', 'rsub', 'rmul', 'rmod', 'rdivmod', 'rpow', 'rlshift', 'rrshift', 'rand', 'rxor', 'ror'):
    key = f'__{fn}__'

    if op := getattr(operator, key, None):
        def fn(self, *args, op=op):
            if args and isinstance(args[0], (vec, proxy)):
                return vec(map(op, self, args[0]))
            return vec(op(x, *args) for x in self)
    else:
        def fn(self, *args, key=key):
            return getattr_to_vec(self, key)(*args)

    setattr(proxy, key, fn)
    setattr(vec, key, fn)

for key in ('mean', 'fmean', 'geometric_mean', 'harmonic_mean', 'median', 'median_low', 'median_high', 'median_grouped', 'mode', 'multimode', 'quantiles', 'pstdev', 'pvariance', 'stdev', 'variance'):

    def fn(self, *args, _fn=getattr(statistics, key), **kwargs):
        return _fn(self.__flat__(), *args, **kwargs)

    setattr(proxy, key, fn)
    setattr(vec, key, getattr(statistics, key))

for key in ('ceil', 'fabs', 'floor', 'isfinite', 'isinf', 'isnan', 'isqrt', 'prod', 'trunc', 'exp', 'log', 'log2', 'log10', 'sqrt'):

    def fn(self, *args, _fn=getattr(math, key), **kwargs):
        return self.map(partial(_fn, *args, **kwargs))

    setattr(proxy, key, fn)
    setattr(vec, key, fn)


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
    parser.add_argument('-e', '--expr', action='store_true', help='print the last python expression given')
    parser.add_argument('-S', '--no-slurp', action='store_false', dest='slurp', help='run python on one row at a time')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-I', '--ignore-errors', action='store_true', help='do not abort on python errors')
    group.add_argument('-E', '--remove-errors', action='store_true', help='remove rows on python errors')

    def __init__(self, opts, mode='exec'):
        super().__init__(opts)

        if opts.expr:
            opts.script[-1] = f'{opts.var} = ({opts.script[-1]})'
        script = '\n'.join(opts.script)

        self.code = compile(script, '<string>', mode)
        # load it into the linecache so it shows up in tracebacks
        linecache.cache['<string>'] = (len(script), None, (script + '\n').splitlines(True), 'asd')

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
            if self.opts.remove_errors or (self.opts.ignore_errors and self.opts.expr):
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

        vars[self.opts.var] = Table(rows, self.header_numbers)

        with self.exec_wrapper(vars):
            exec(self.code, vars)

        self.handle_exec_result(vars)

    def convert_to_table(self, value):
        if isinstance(value, proxy) and not value.__is_row__() and not value.__is_column__():
            data = list(value)
            headers = apply_slice(list(value.__parent__.__headers__), value.__cols__)
            return Table(data, headers)

        if isinstance(value, Table):
            return value

        if isinstance(value, dict):
            columns = [list(v) if isinstance(v, (list, tuple, proxy)) else [v] for v in value.values()]
            max_rows = max(len(col) for col in columns)
            if any(col and max_rows % len(col) != 0 for col in columns):
                raise ValueError(f'mismatched rows: {value}')
            columns = [col * (max_rows // len(col)) if col else [b''] * max_rows for col in columns]
            data = list(zip(*columns))
            headers = value.keys()
            return Table(data, headers)

    def handle_exec_result(self, vars):
        result = vars.get(self.opts.var)
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
        elif self.opts.expr:
            print(result)
        else:
            raise ValueError(result)

    def exec_per_row(self, row, **vars):
        self.count = self.count + 1
        self.do_exec([row], N=self.count, **vars)

    def exec_on_all_rows(self, rows, **vars):
        self.do_exec(rows, N=len(rows), **vars)
