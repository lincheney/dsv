import sys
import re
import datetime
import itertools
import operator
import math
import statistics

FULL_SLICE = slice(None)
MISSING = b''

def to_bytes(x):
    if not isinstance(x, bytes):
        x = str(x).encode('utf8')
    return x

def as_float(value, warn=True):
    try:
        return float(value)
    except ValueError as e:
        if warn:
            print(e, file=sys.stderr)
        return math.nan

def diff(value):
    result = []
    prev = 0
    for v in value:
        result.append(v - prev)
        prev = v
    return result[1:]

def parse_datetime(
    value,
    formats=(
        '%Y-%m-%dT%H:%M:%S.%f%z',
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S%z',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d %H:%M:%S',
        '%Y/%m/%d %H:%M:%S',
        '%d/%m/%y %H:%M:%S',
    ),
    date_yardstick=datetime.datetime(2000, 1, 1),
):
    if isinstance(value, (list, tuple)):
        return [parse_datetime(x) for x in value]

    if isinstance(value, datetime.datetime):
        return value

    elif isinstance(value, (str, bytes)) and value:
        val = value
        if isinstance(val, bytes):
            val = val.decode('utf8')
        val = re.sub('(\\.[0-9]{6})[0-9]*', '\\1', val)
        for fmt in formats:
            try:
                return datetime.datetime.strptime(val, fmt)
            except ValueError:
                pass

    elif isinstance(value, (int, float)) and value >= date_yardstick.timestamp():
        if value > date_yardstick.timestamp() * 1000:
            # this is in milliseconds
            value /= 1000.0
        return datetime.datetime.fromtimestamp(value)

    return value

def is_list_of(value, types):
    return isinstance(value, list) and all(isinstance(x, types) for x in value)

def apply_slice(data, key, flat=False):
    if isinstance(data, slice):
        data = slice_to_list(data)

    try:
        if isinstance(key, slice) or (isinstance(key, int) and flat):
            return data[key]
    except IndexError:
        if isinstance(key, slice):
            return list(itertools.islice(data, key.start, key.stop, key.step))
        else:
            return list(itertools.islice(data, key, key+1))
    if isinstance(key, int):
        return (data[key],) if key < len(data) else ()
    else:
        while key and key[-1] >= len(data):
            key = key[:-1]
        return [data[k] if k < len(data) else MISSING for k in key]

def slice_to_list(key, stop=None):
    return list(range(*key.indices(key.stop if stop is None else stop)))

def convert_to_table(value):
    if isinstance(value, Proxy) and not value.__is_row__() and not value.__is_column__():
        return Table(list(value), value.__headers__)

    if isinstance(value, Table):
        return value

    if isinstance(value, dict):
        columns = [list(v) if isinstance(v, (list, tuple, Proxy)) else [v] for v in value.values()]
        max_rows = max(len(col) for col in columns)
        if any(col and max_rows % len(col) != 0 for col in columns):
            raise ValueError(f'mismatched rows: {value}')
        columns = [col * (max_rows // len(col)) if col else [MISSING] * max_rows for col in columns]
        data = list(zip(*columns))
        headers = value.keys()
        return Table(data, headers)

class Vectorised:
    def __getattr__(self, key):
        value = self.map(lambda x: getattr(x, key))
        if all(map(callable, value.__flat__())):
            return (lambda *a, **kw: value.map(lambda fn: fn(*a, **kw)))
        return value

class BaseTable(Vectorised):
    def __setattr__(self, key, value):
        self[key] = value
    def __delattr__(self, key):
        del self[key]
    def __getattr__(self, key):
        if to_bytes(key) in self.__headers__:
            return self[key]
        return super().__getattr__(key)

    def __len__(self):
        return len(self.__data__)

    def __numrows__(self):
        return len(self)

    def __numcols__(self):
        return len(self.__headers__ or (self.__data__ and self.__data__[0]))

    def __add_col__(self, name):
        # add missing headers
        if not self.__headers__ and (cols := self.__numcols__()):
            for i in range(1, cols+1):
                self.__headers__[str(i)] = i

        self.__headers__[name] = len(self.__headers__)
        return self.__headers__[name]

    def __get_col__(self, col, new=False):
        col = to_bytes(col)
        if new and col not in self.__headers__:
            self.__add_col__(col)
        return self.__headers__[col]

    def __parse_key__(self, key, new=False):
        if isinstance(key, (int, slice)) or is_list_of(key, int):
            k = (key, FULL_SLICE)
        elif isinstance(key, (str, bytes)) or is_list_of(key, (str, bytes, int)):
            k = (FULL_SLICE, key)
        elif not isinstance(key, tuple) or len(key) != 2:
            raise IndexError(key)
        else:
            k = key

        real_rows, real_cols = k

        length = self.__numrows__()
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

        length = self.__numcols__()
        indices = range(length)
        if is_list_of(real_cols, (str, bytes, int)):
            real_cols = cols = [indices[x] if isinstance(x, int) else self.__get_col__(x, new) for x in real_cols]
        elif isinstance(real_cols, (str, bytes)):
            real_cols = cols = self.__get_col__(real_cols, new)
        elif isinstance(real_cols, int):
            real_cols = cols = len(indices) if real_cols >= len(indices) and new else indices[real_cols]
        elif isinstance(real_cols, slice):
            cols = slice(*real_cols.indices(length))
        else:
            raise IndexError(key)

        return real_rows, real_cols, rows, cols

    def __flat__(self):
        return itertools.chain.from_iterable(self.__data__)

    def map(self, fn, col=False):
        if col:
            cols = [fn(Vec(col)) for col in zip(*self)]
            return convert_to_table(dict(zip(self.__headers__, cols)))
        else:
            return Table([Vec(row).map(fn) for row in self], self.__headers__.copy())


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

        value = iter(value) if non_scalar else itertools.repeat(value)
        for row in rows:
            if isinstance(cols, int):
                colindex = (cols,)
            elif isinstance(cols, slice):
                colindex = slice_to_list(cols, len(row))
            else:
                colindex = cols

            for col in colindex:
                if col >= len(row):
                    row += [MISSING] * (col + 1 - len(row))
                row[col] = next(value, MISSING)

    def __delitem__(self, key):
        real_rows, real_cols, rows, cols = self.__parse_key__(key, new=True)

        # delete every row in specific columns
        if real_rows == FULL_SLICE:
            cols = [cols] if isinstance(cols, (int, slice)) else set(cols)

            header = list(self.__headers__.keys())
            for c in sorted(cols, reverse=True):
                if isinstance(c, int):
                    c = slice(c, c+1)
                for row in self.__data__:
                    del row[c]
                del header[c]

            self.__dict__['__headers__'] = {k: i for i, k in enumerate(header)}

        # delete every column in specific rows
        elif real_cols == FULL_SLICE:
            rows = [rows] if isinstance(rows, (int, slice)) else set(rows)
            for r in sorted(rows, reverse=True):
                del self.__data__[r]

        else:
            raise IndexError(key)

    def append(self, value):
        self.insert(len(self), value)

    def insert(self, index, value):
        if not isinstance(value, (list, tuple)):
            value = [value] * self.__numcols__()
        self.__data__.insert(index, value)

class Proxy(BaseTable):
    def __init__(self, parent, rows, cols):
        self.__dict__.update(
            __parent__=parent,
            __rows__=rows,
            __cols__=cols,
            __headers__={k: i for i, k in enumerate(apply_slice(list(parent.__headers__), cols))},
        )

    def __numrows__(self):
        if self.__is_row__():
            return 1
        return super().__numrows__()

    def __is_row__(self):
        return isinstance(self.__rows__, int)

    def __is_column__(self):
        return isinstance(self.__cols__, int)

    def __add_col__(self, name):
        assert not self.__is_row__() and not self.__is_column__()

        # add it to the parent as well
        num = self.__parent__.__add_col__(name)
        if isinstance(self.__cols__, slice):
            self.__dict__['__cols__'] = slice_to_list(self.__cols__)
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

    def __parse_key__(self, key, new=False):
        if isinstance(key, tuple) and (self.__is_row__() or self.__is_column__()):
            raise IndexError(key)
        if self.__is_row__():
            key = (FULL_SLICE, key)

        _, _, rows, cols = super().__parse_key__(key, new)
        rows = self.__rows__ if self.__is_row__()    else apply_slice(self.__rows__, rows, flat=True)
        cols = self.__cols__ if self.__is_column__() else apply_slice(self.__cols__, cols, flat=True)
        return (rows, cols)

    def __getitem__(self, key):
        key = self.__parse_key__(key)
        return self.__parent__[key]

    def __setitem__(self, key, value):
        key = self.__parse_key__(key, True)
        self.__parent__[key] = value

    def __flat__(self):
        if self.__is_row__() or self.__is_column__():
            return self
        return super().__flat__()

    def map(self, fn, col=False):
        if col and self.__is_column__():
            return fn(self)
        if self.__is_row__() or self.__is_column__():
            return Vec(self).map(fn)
        return super().map(fn, col)


class Vec(Vectorised, list):
    def __flat__(self):
        return self

    def map(self, fn):
        return Vec(map(fn, self))


for arity, scalar, functions in [
    (1, False, (
        '__round__', '__floor__', '__ceil__', '__neg__', '__pos__', '__invert__', '__index__',
        math.ceil, math.fabs, math.floor, math.isfinite, math.isinf, math.isnan, math.isqrt, math.prod, math.trunc, math.exp, math.log, math.log2, math.log10, math.sqrt,
        as_float, parse_datetime,
    )),
    (1, True, (
        sum,
        statistics.mean, statistics.fmean, statistics.geometric_mean, statistics.harmonic_mean, statistics.median, statistics.median_low, statistics.median_high, statistics.median_grouped, statistics.mode, statistics.multimode, statistics.quantiles, statistics.pstdev, statistics.pvariance, statistics.stdev, statistics.variance,
        diff,
    )),
    (2, True, (
        '__lt__', '__gt__', '__le__', '__ge__', '__eq__', '__ne__', '__add__', '__sub__', '__mul__', '__matmul__', '__truediv__', '__floordiv__', '__mod__', '__lshift__', '__rshift__', '__and__', '__xor__', '__or__', '__pow__', '__divmod__',
    )),
    (-2, True, (
        '__rlt__', '__rgt__', '__rle__', '__rge__', '__req__', '__rne__', '__radd__', '__rsub__', '__rmul__', '__rmatmul__', '__rtruediv__', '__rfloordiv__', '__rmod__', '__rlshift__', '__rrshift__', '__rand__', '__rxor__', '__ror__', '__rpow__', '__rdivmod__',
    )),
]:
    for fn in functions:
        name = fn if isinstance(fn, str) else fn.__name__

        if isinstance(fn, str):
            fnname = name
            if arity < 0:
               fnname = fnname.replace('__r', '__', 1)
            if not (fn := getattr(operator, fnname, None)):
                def fn(x, *args, fn=fnname, **kwargs):
                    return getattr(x, fn)(*args, **kwargs)

        if arity == 1 and scalar:
            def method(self, *args, fn=fn, **kwargs):
                return fn(self.__flat__(), *args, **kwargs)

        elif arity == 1:
            def method(self, *args, fn=fn, **kwargs):
                return self.map(lambda x: fn(x, *args, **kwargs))

        elif arity == 2:
            def method(self, other, *args, fn=fn, **kwargs):
                if isinstance(other, Vectorised):
                    return Vec(fn(x, y, *args, **kwargs) for x, y in zip(self, other))
                return self.map(lambda x: fn(x, other, *args, **kwargs))

        elif arity == -2:
            def method(self, other, *args, fn=fn, **kwargs):
                return self.map(lambda x: fn(other, x, *args, **kwargs))

        else:
            raise NotImplementedError(arity, scalar)

        setattr(Vectorised, name, method)
