import sys
import argparse
import linecache
import itertools
import operator
from contextlib import contextmanager
from ._base import _Base
from . import _utils

def to_bytes(x):
    if not isinstance(x, bytes):
        x = str(x).encode('utf8')
    return x

def getattr_to_vec(self, key):
    value = [getattr(x, key) for x in self]
    if all(map(callable, value)):
        return (lambda *a, **kw: vec(fn(*a, **kw) for fn in value))
    return vec(value)

class Table:
    def __init__(self, data, headers):
        super().__setattr__('__headers__', headers)
        super().__setattr__('__data__', data)

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

    def __parse_key__(self, key, new=False):
        if isinstance(key, tuple) and len(key) == 1:
            key = key[0]

        if isinstance(key, str):
            key = (slice(None), key)
        elif isinstance(key, (int, slice)):
            key = (key, slice(None))
        elif not isinstance(key, tuple) or len(key) != 2:
            raise IndexError(key)

        rows, cols = key

        if isinstance(cols, str):
            cols = cols.encode('utf8')
            if new and cols not in self.__headers__:
                self.__headers__[cols] = len(self.__headers__)
            cols = self.__headers__[cols]

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

        if isinstance(value, (list, tuple)) and isinstance(cols, int) and isinstance(rows, slice):
            # zip the value over the rows
            value = iter(value)
        else:
            value = itertools.repeat(value)

        if isinstance(rows, int):
            rows = [self.__data__[rows]]
        else:
            rows = self.__data__[rows]

        # set a specific column
        for row in rows:
            if isinstance(cols, int) and cols >= len(row):
                row += [b''] * (cols - len(row) - 1)
                row.append(next(value))
            else:
                row[cols] = next(value)

    def __delitem__(self, key):
        rows, cols = self.__parse_key__(key, new=True)
        full_slice = slice(None, None, None)

        if rows == full_slice:
            # delete columns
            for row in self.__data__:
                del row[cols]
            header = list(self.__headers__.keys())
            del header[cols]
            super().__setattr__('__headers__', {k: i for i, k in enumerate(header)})

        elif cols == full_slice:
            # delete rows
            del self.__data__[rows]

        else:
            raise IndexError(key)

class proxy:
    def __init__(self, parent, rows, cols):
        self.__parent__ = parent
        self.__rows__ = rows
        self.__cols__ = cols

    def __is_row__(self):
        return isinstance(self.__rows__, int)

    def __is_column__(self):
        return isinstance(self.__cols__, int)

    def __inner__(self):
        if self.__is_row__():
            return self.__parent__.__data__[self.__rows__][self.__cols__]

        if self.__is_column__():
            return [r[self.__cols__] for r in self.__parent__.__data__[self.__rows__]]

        return [r[self.__cols__] for r in self.__parent__.__data__[self.__rows__]]

    def __len__(self):
        return len(self.__inner__())

    def __iter__(self):
        return iter(self.__inner__())

    def __repr__(self):
        return repr(self.__inner__())

    def __getattr__(self, key):
        if self.__is_column__():
            return getattr_to_vec(self, key)
        return self[key]

    def __parse_key__(self, key):
        if isinstance(key, tuple):
            if self.__is_column__() or self.__is_row__():
                raise IndexError(key)
            return (self.__rows__[key[0]], self.__cols__[key[1]])

        if isinstance(key, str):
            if self.__is_column__():
                raise IndexError(key)
            _, key = self.__parent__.__parse_key__(key)
            return (self.__rows__, key)

        if isinstance(key, (int, slice)):

            if self.__is_row__():
                return (self.__rows__, key)

            if self.__is_column__():
                return (key, self.__cols__)

            # get a specific row(s)
            return (self.__rows__[key], self.__cols__)

        raise IndexError(key)

    def __getitem__(self, key):
        key = self.__parse_key__(key)
        return self.__parent__[key]

    def __setitem__(self, key, value):
        key = self.__parse_key__(key)
        self.__parent__[key] = value

    def as_float(self):
        if not self.__is_row__() and not self.__is_column__():
            return vec(vec(row).as_float() for row in self)
        return vec(self).as_float()

class vec(list):
    def __getattr__(self, key):
        return getattr_to_vec(self, key)

    def as_float(self):
        result = vec()
        for i in self:
            result.append(_utils.as_float(i))
        return result

for fn in ('round', 'floor', 'ceil', 'lt', 'gt', 'le', 'ge', 'eq', 'ne', 'neg', 'pos', 'invert', 'add', 'sub', 'mul', 'matmul', 'truediv', 'floordiv', 'mod', 'divmod', 'lshift', 'rshift', 'and', 'xor', 'or', 'pow', 'index'):
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

class exec_(_Base):
    ''' run python on each row '''
    name = 'exec'

    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument('-q', '--quiet', action='store_true', help='do not print errors')
    parent.add_argument('--var', default='X', help='python variable to use to refer to the data (default: %(default)s)')
    parent.add_argument('-b', '--bytes', action='store_true', help='do not auto convert data to int, str etc, treat everything as bytes')

    parser = argparse.ArgumentParser(parents=[parent])
    parser.add_argument('script', nargs='+', help='python statements to run')
    parser.add_argument('-e', '--expr', action='store_true', help='print the last python expression given')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-I', '--ignore-errors', action='store_true', help='do not abort on python errors')
    group.add_argument('-E', '--remove-errors', action='store_true', help='remove rows on python errors')
    group.add_argument('-S', '--no-slurp', action='store_false', dest='slurp', help='run python on one row at a time')

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
            if self.opts.remove_errors:
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
        if isinstance(value, Table):
            return value

        elif isinstance(value, proxy) and not value.__is_row__() and not value.__is_column__():
            data = list(value)
            headers = list(value.__parent__.__headers__)[value.__cols__]
            return Table(data, headers)

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
