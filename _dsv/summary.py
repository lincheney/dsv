import re
import argparse
import itertools
import datetime
import math
import statistics
from collections import Counter
from ._base import _Base
from . import _utils
from ._column_slicer import _ColumnSlicer

class summary(_ColumnSlicer):
    ''' produce automatic summaries of the data '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--col-sep', choices=('never', 'always', 'auto'), default='auto', help='show a separator between the columns')
    parser.add_argument('fields', nargs='*', help='select only these fields')
    parser.add_argument('-x', '--complement', action='store_true', help='exclude, rather than include, field names')
    parser.add_argument('-r', '--regex', action='store_true', help='treat fields as regexes')
    parser.set_defaults(ofs=_Base.PRETTY_OUTPUT)

    SIZE_SUFFIXES = {
        '': 1,
        'b': 1,
        'k': 10**3,
        'kb': 10**3,
        'kib': 2**10,
        'm': 10**6,
        'mb': 10**6,
        'mib': 2**20,
        'g': 10**9,
        'gb': 10**9,
        'gib': 2**30,
        't': 10**12,
        'tb': 10**12,
        'tib': 2**40,
        'p': 10**15,
        'pb': 10**15,
        'pib': 2**50,
    }
    SIZE_REGEX = re.compile(fr'''(?i)(\d+(?:\.\d+)?)\s?((?:{'|'.join(SIZE_SUFFIXES)})?)'''.encode('utf8'))

    def __init__(self, opts):
        super().__init__(opts)
        self.opts.col_sep = _utils.resolve_tty_auto(self.opts.col_sep)
        self.sep = self.get_separator()
        self.rows = []

    def on_header(self, header):
        self.header_map = self.make_header_map(self.header)

    def on_row(self, row):
        row = self.slice(row, self.opts.complement)
        self.rows.append(row)

    def on_eof(self, cutoff=0.8):
        if not super().on_header([b'column', b'type', b'key', b'value']):

            missing = b''
            header = self.header or []
            num_cols = max(len(header), max(map(len, self.rows), default=0))

            if len(header) < num_cols:
                header += [_utils.to_bytes(i+1) for i in range(len(header), num_cols)]

            header = self.slice(header, self.opts.complement)
            columns = list(itertools.zip_longest(*self.rows, fillvalue=missing))

            for h, col in zip(header, columns):
                # what is it

                if self.is_enum(col) >= cutoff:
                    if self.display_enum(h, col):
                        break

                elif self.is_date(dates := _utils.parse_datetime(col)) >= cutoff:
                    if self.display_date(h, dates):
                        break

                elif self.is_numeric(numbers := _utils.parse_value(col)) >= cutoff:
                    if self.display_numeric(h, numbers):
                        break

                elif self.is_numeric(numbers := _utils.parse_value([c.strip().removesuffix(b'%') for c in col])) >= cutoff:
                    if self.display_numeric(h, numbers, formatter=self.format_percentage):
                        break

                elif self.is_size(col) >= cutoff:
                    if self.display_size(h, col):
                        break

                else:
                    if self.display_enum(h, col):
                        break

            for header in header[len(columns):]:
                if super().on_row([header, b'(empty)']):
                    break
                if self.opts.col_sep:
                    if super().on_row(self.sep):
                        break

        return super().on_eof()

    def display_stats(self, header, type, stats):
        type = _utils.to_bytes(type)
        for k, v in stats.items():
            if super().on_row([header, type, _utils.to_bytes(k), _utils.to_bytes(v)]):
                return True

    def is_enum(self, col, n=5):
        counts = Counter(col)
        return sum(v for k, v in counts.most_common(n)) / len(col)

    def display_enum(self, header, col, n=5):
        type = 'enum'

        non_blank = [x for x in col if x]
        counts = Counter(non_blank)

        common = {k: v for k, v in counts.most_common(n) if v > 1}
        if len(counts) <= n + 1:
            common = counts

        stats = common.copy()
        if len(non_blank) != len(col):
            stats['[empty string]'] = len(col) - len(non_blank)

        if stats:
            if len(non_blank) != sum(common.values()):
                stats[f'[{len(counts) - len(common)} other values]'] = len(non_blank) - sum(common.values())

            for k, v in stats.items():
                stats[k] = f'{v} ({v / len(col) * 100:.3g}%)'

        else:
            # no common strings, do some word stats etc instead
            type = 'string'
            stats['min length'] = min(map(len, col))
            stats['max length'] = max(map(len, col))
            stats['words'] = sum(len(x.split()) for x in col)
            stats['[example]'] = next(x for x in col if x)

        return self.display_stats(header, type, stats)

    def is_date(self, col):
        num_dates = sum(isinstance(x, datetime.datetime) for x in col)
        return num_dates / len(col)

    def display_date(self, header, col):
        stats = self.get_numeric_stats(
            [x.timestamp() if isinstance(x, datetime.datetime) else None for x in col],
            formatter=datetime.datetime.fromtimestamp,
        )
        return self.display_stats(header, 'date', stats)

    def is_numeric(self, col):
        num_floats = sum(isinstance(x, (float, int)) for x in col)
        return num_floats / len(col)

    def get_numeric_stats(self, col, formatter=None):
        numeric = [x for x in col if isinstance(x, (float, int))]
        non_nan = [x for x in numeric if x is not math.isnan(x)]
        finite = [x for x in non_nan if not math.isinf(x)]

        first_quartile, median, third_quartile = statistics.quantiles(non_nan, n=4)

        stats = {
            'min': min(non_nan),
            'first quartile': first_quartile,
            'mean': statistics.mean(finite or non_nan),
            'median': median,
            'third quartile': third_quartile,
            'max': max(non_nan),
        }
        if formatter is not None:
            for k, v in stats.items():
                stats[k] = formatter(v)
        if len(non_nan) != len(numeric):
            stats['nan'] = len(numeric) - len(non_nan)
        if len(numeric) != len(col):
            stats['non numeric'] = len(col) - len(numeric)

        return stats

    def display_numeric(self, header, col, formatter=None):
        return self.display_stats(header, 'numeric', self.get_numeric_stats(col, formatter))

    def format_percentage(self, value):
        return f'{value:.3g}%'

    def is_size(self, col):
        num_sizes = sum(bool(self.SIZE_REGEX.fullmatch(c)) for c in col)
        return num_sizes / len(col)

    def format_size(self, size):
        suffixes = ('b', 'kb', 'mb', 'gb', 'tb', 'pb')
        for exp, suffix in enumerate(suffixes, 1):
            if exp == len(suffixes) or size < 1_000**exp:
                return '{:.3g} {}'.format(size / 1_000**(exp-1), suffix)
        raise Exception()

    def display_size(self, header, col):
        matches = [self.SIZE_REGEX.fullmatch(c) for c in col]
        values = [float(m.group(1)) * self.SIZE_SUFFIXES[m.group(2).lower().decode('utf8')] for m in matches]
        stats = self.get_numeric_stats(values, formatter=self.format_size)
        return self.display_stats(header, 'size', stats)
