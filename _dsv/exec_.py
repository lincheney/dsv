import sys
import math
import argparse
from contextlib import contextmanager
from functools import partial
from ._base import _Base

def to_bytes(x):
    if not isinstance(x, bytes):
        x = str(x).encode('utf8')
    return x

class string_like(bytes):
    @staticmethod
    def wrapper_fn(self, other, *args, _fn, **kwargs):
        if isinstance(other, str):
            other = other.encode('utf8')
        return _fn(self, other, *args, **kwargs)

    def __getattribute__(self, key):
        if key == 'decode':
            return super().decode
        return partial(string_like.wrapper_fn, self, _fn=getattr(bytes, key))

    def __add__(self, other):
        return string_like.wrapper_fn(self, other, _fn=bytes.__add__)

    def __radd__(self, other):
        if isinstance(other, str):
            other = string_like(other.encode('utf8'))
        if isinstance(other, bytes):
            return string_like(other + self)
        raise TypeError(other)

    def __eq__(self, other):
        return string_like.wrapper_fn(self, other, _fn=bytes.__eq__)

    def __hash__(self):
        return super().__hash__()

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
            if cols >= len(self.__data__):
                return string_like()
            return string_like(self.__data__[rows][cols])

        return proxy(self, rows, cols)

    def __setitem__(self, key, value):
        rows, cols = self.__parse_key__(key, new=True)

        if isinstance(rows, int):
            rows = [self.__data__[rows]]
        else:
            rows = self.__data__[rows]

        # set a specific column
        for row in rows:
            if isinstance(cols, int) and cols >= len(row):
                row += [b''] * (cols - len(row) - 1)
                row.append(value)
            else:
                row[cols] = value

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
            return self.__parent__.__data__[self.__rows__]

        if self.__is_column__():
            return [r[self.__cols__] for r in self.__parent__.__data__]

        return [r[self.__cols__] for r in self.__parent__.__data__[self.__rows__]]

    def __repr__(self):
        return repr(self.__inner__())

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

    def float(self):
        if not self.__is_row__() and not self.__is_column__():
            raise TypeError(self)

        result = []
        for i in self.__inner__():
            try:
                result.append(float(i))
            except ValueError as e:
                print(e, file=sys.stderr)
                result.append(math.nan)
        return result

class exec_(_Base):
    ''' run python on each row '''
    name = 'exec'
    parser = argparse.ArgumentParser()
    parser.add_argument('script', nargs='+')
    parser.add_argument('-q', '--quiet', action='store_true')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-I', '--ignore-errors', action='store_true')
    group.add_argument('-E', '--remove-errors', action='store_true')
    group.add_argument('-s', '--slurp', action='store_true')
    parser.add_argument('--var', default='X')

    def __init__(self, opts, mode='exec'):
        super().__init__(opts)

        script = '\n'.join(opts.script)
        self.code = compile(script, '<string>', mode)
        self.count = 0
        self.have_printed_header = False
        self.rows = []
        self.modifiable_header = []
        self.header_map = {}

    def on_header(self, header):
        self.modifiable_header = header.copy()
        self.header_map = {k: i for i, k in enumerate(header)}

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
            if not self.opts.quiet:
                print(f'{type(e).__name__}: {e}', file=sys.stderr)
            if self.opts.remove_errors:
                vars.pop(self.opts.var, None)
                return
            if not self.opts.ignore_errors and not self.opts.quiet:
                raise

    def do_exec(self, rows, **vars):
        vars[self.opts.var] = table = Table(rows, self.header_map)

        with self.exec_wrapper(vars):
            exec(self.code, vars)

        if vars.get(self.opts.var) is table:
            headers = table.__headers__
            rows = table.__data__

        elif self.opts.var in vars:
            if isinstance(vars[self.opts.var], dict):
                vars[self.opts.var] = [vars[self.opts.var]]
            if isinstance(vars[self.opts.var], list) and all(isinstance(row, dict) for row in vars[self.opts.var]):
                headers = {}
                for row in vars[self.opts.var]:
                    headers.update(dict.fromkeys(row))
                rows = [row.values() for row in vars[self.opts.var]]
            else:
                raise ValueError(vars[self.opts.var])

        else:
            return

        if not self.have_printed_header and headers:
            super().on_header([to_bytes(k) for k in headers])
            self.have_printed_header = True

        for row in rows:
            super().on_row([to_bytes(x) for x in row])

    def exec_per_row(self, row, **vars):
        self.count = self.count + 1
        self.do_exec([row], N=self.count, **vars)

    def exec_on_all_rows(self, rows, **vars):
        self.do_exec(rows, N=len(rows), **vars)
