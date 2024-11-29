import argparse
import itertools
import datetime
import math
import statistics
from collections import Counter
from ._base import _Base
from . import _utils

class summary(_Base):
    ''' produce automatic summaries of the data '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--col-sep', choices=('never', 'always', 'auto'), default='auto', help='show a separator between the columns')
    parser.set_defaults(ofs=_Base.PRETTY_OUTPUT)

    def __init__(self, opts):
        super().__init__(opts)
        self.opts.col_sep = _utils.resolve_tty_auto(self.opts.col_sep)
        self.rows = []

    def on_header(self, header):
        pass

    def on_row(self, row):
        self.rows.append(row)

    def on_eof(self, cutoff=0.8):
        missing = b''
        header = self.header or []
        num_cols = max(len(header), max(map(len, self.rows)))

        if len(header) < num_cols:
            header += [_utils.to_bytes(i+1) for i in range(len(header), num_cols)]

        if super().on_header([b'column', b'type', b'key', b'value']):
            return

        columns = list(itertools.zip_longest(*self.rows, fillvalue=missing))
        for header, col in zip(header, columns):
            parsed = _utils.parse_value(col)

            # what is it

            if self.is_enum(col) >= cutoff:
                if self.display_enum(header, col):
                    break

            elif self.is_date(dates := _utils.parse_datetime(col)) >= cutoff:
                if self.display_date(header, dates):
                    break

            elif self.is_numeric(numbers := _utils.parse_value(col)) >= cutoff:
                if self.display_numeric(header, numbers):
                    break

            else:
                if self.display_enum(header, col):
                    break

            if self.opts.col_sep:
                if super().on_row([b'---']):
                    break

            # import sys;print(f'''DEBUG(trauma)\t{header = }''', file=sys.__stderr__)
            #  import sys;print(f'''DEBUG(unclog)\t{col = }''', file=sys.__stderr__)

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

        stats = {k: v for k, v in counts.most_common(n) if v > 1}
        if len(non_blank) != len(col):
            stats['[empty string]'] = len(col) - len(non_blank)

        if not stats:
            # no common strings, do some word stats etc instead
            type = 'string'
            stats['min length'] = min(map(len, col))
            stats['max length'] = max(map(len, col))
            stats['words'] = sum(len(x.split()) for x in col)

        elif len(col) != sum(stats.values()):
            stats[f'[{len(counts) - len(stats)} other values]'] = len(col) - sum(stats.values())

        return self.display_stats(header, type, stats)

    def is_date(self, col):
        num_dates = sum(isinstance(x, datetime.datetime) for x in col)
        return num_dates / len(col)

    def display_date(self, header, col):
        stats = self.get_numeric_stats([x.timestamp() if isinstance(x, datetime.datetime) else None for x in col])
        for k, v in stats.items():
            stats[k] = datetime.datetime.fromtimestamp(v)
        return self.display_stats(header, 'date', stats)

    def is_numeric(self, col):
        num_floats = sum(isinstance(x, (float, int)) for x in col)
        return num_floats / len(col)

    def get_numeric_stats(self, col):
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
        if len(non_nan) != len(numeric):
            stats['nan'] = len(numeric) - len(non_nan)
        if len(numeric) != len(col):
            stats['numeric'] = len(col) - len(numeric)

        return stats

    def display_numeric(self, header, col):
        return self.display_stats(header, 'numeric', self.get_numeric_stats(col))
