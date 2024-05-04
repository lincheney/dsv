import sys
import argparse
from contextlib import contextmanager
from functools import partial
from ._base import _Base

def to_bytes(x):
    if not isinstance(x, bytes):
        x = str(x).encode('utf8')
    return x

class mixin:
    def __setattr__(self, key, value):
        if key in self.__slots__:
            super().__setattr__(key, value)
        else:
            self[key] = value
    def __delattr__(self, key):
        del self[key]
    def __getattr__(self, key):
        return self[key]

    def __get_column__(self, key, new=False):
        if isinstance(key, str):
            key = key.encode('utf8')
            if new and key not in self.__header_map__:
                self.__header_map__[key] = len(self.__header__)
                self.__header__.append(key)
            key = self.__header_map__[key]
        return key

    def __remake_header_map__(self):
        self.__header_map__.clear()
        self.__header_map__.update({k: i for i, k in enumerate(self.__header__)})

class string_like(bytes):
    @staticmethod
    def wrapper_fn(self, other, *args, _fn, **kwargs):
        if isinstance(other, str):
            other = other.encode('utf8')
        return _fn(self, other, *args, **kwargs)

    def __getattribute__(self, key):
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

class Row(list, mixin):
    __slots__ = ('__header__', '__header_map__')
    def __init__(self, row, header, header_map):
        super().__init__(row)
        self.__header__ = header
        self.__header_map__ = header_map

    def __getitem__(self, key):
        key = self.__get_column__(key)
        if isinstance(key, int) and key >= len(self):
            return string_like()
        return string_like(super().__getitem__(key))

    def __setitem__(self, key, value):
        key = self.__get_column__(key, True)
        if isinstance(key, int) and key >= len(self):
            self += [b''] * (key - len(self) - 1)
            self.append(value)
        else:
            return super().__setitem__(key, value)

class Column(mixin):
    __slots__ = ('__index__', '__rows_ref__', '__header__', '__header_map__')
    def __init__(self, index, rows_ref, header, header_map):
        self.__index__ = index
        self.__rows_ref__ = rows_ref
        self.__header__ = header
        self.__header_map__ = header_map

    def __getitem__(self, key):
        if isinstance(key, slice):
            return [r[self.__index__] for r in self.__rows_ref__['rows'][key]]
        else:
            return self.__rows_ref__['rows'][key][self.__index__]

    def __setitem__(self, key, value):
        rows = self.__rows_ref__['rows'][key]
        if not isinstance(key, slice):
            rows = [rows]

        if isinstance(value, Column):
            value = value[:]
        elif not isinstance(value, (list, tuple)):
            # broadcast
            value = [value] * len(rows)

        for row, val in zip(rows, value):
            row[self.__index__] = val

class Columns(mixin):
    __slots__ = ('__rows_ref__', '__header__', '__header_map__')
    def __init__(self, rows_ref, header, header_map):
        self.__rows_ref__ = rows_ref
        self.__header__ = header
        self.__header_map__ = {}
        self.__remake_header_map__()

    def __getitem__(self, key, new=False):
        key = self.__get_column__(key, new)
        return Column(key, self.__rows_ref__, self.__header__, self.__header_map__)

    def __setitem__(self, key, value):
        self.__getitem__(key, new=True)[:] = value

    def __delitem__(self, key):
        key = self.__get_column__(key)
        # remove from header as well
        for row in self.__rows_ref__['rows'] + [self.__header__]:
            if key < len(row):
                del row[key]
        self.__remake_header_map__()

class exec_(_Base):
    ''' run python on each row '''
    name = 'exec'
    parser = argparse.ArgumentParser()
    parser.add_argument('script', nargs='+')
    parser.add_argument('-q', '--quiet', action='store_true')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-I', '--ignore-errors', action='store_true')
    group.add_argument('-E', '--remove-errors', action='store_true')
    parser.add_argument('-s', '--slurp', action='store_true')

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
    def exec_wrapper(self):
        try:
            yield
        except Exception as e:
            if not self.opts.quiet:
                print(f'{type(e).__name__}: {e}', file=sys.stderr)
            if not self.opts.ignore_errors and not self.opts.quiet:
                raise

    def exec_per_row(self, row):
        self.count = self.count + 1
        row = Row(row, self.modifiable_header, self.header_map)
        vars = {'row': row, 'N': self.count, 'header': self.modifiable_header}

        try:
            with self.exec_wrapper():
                exec(self.code, vars)
        except:
            if self.opts.remove_errors:
                return
            raise

        if 'row' in vars:
            header = {}
            row = self.parse_row(vars['row'], header)
            header = [to_bytes(k) for k in header]

            if not self.have_printed_header and header:
                super().on_header([to_bytes(k) for k in header])
                self.have_printed_header = True

            super().on_row(row)
        return row

    def exec_on_all_rows(self, rows, **vars):
        vars['N'] = len(rows)
        vars['header'] = self.modifiable_header
        vars['rows'] = [Row(row, self.modifiable_header, self.header_map) for row in rows]
        vars['columns'] = Columns(vars, self.modifiable_header, self.header_map)

        with self.exec_wrapper():
            exec(self.code, globals=vars)

        rows = []
        header = {}
        for row in vars['rows']:
            rows.append(self.parse_row(row, header))

        if not self.have_printed_header and header:
            super().on_header([to_bytes(k) for k in header])
            self.have_printed_header = True

        for row in rows:
            super().on_row(row)
        return rows

    def parse_row(self, row, header):
        if isinstance(row, Row):
            for k in row.__header__:
                header[k] = True
        elif isinstance(row, dict):
            for k in row:
                header[k] = True
            row = [row[k] for k in header]
        return [to_bytes(col) for col in row]
