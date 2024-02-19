import sys
import argparse
from ._base import _Base

def to_bytes(x):
    if not isinstance(x, bytes):
        x = str(x).encode('utf8')
    return x

def make_row_class(header):
    header_map = {k: i for i, k in enumerate(header)}

    class cls(list):
        def __setattr__(self, key, value):
            self[key] = value
        def __getattr__(self, key):
            return self[key]

        def __getitem__(self, key):
            if isinstance(key, str):
                key = key.encode('utf8')
                key = header_map[key] + 1
            if key > len(self):
                return ''
            return super().__getitem__(key if key < 0 else key-1).decode('utf8')

        def __setitem__(self, key, value):
            value = to_bytes(value)

            if isinstance(key, str):
                key = key.encode('utf8')
                if key not in header_map:
                    header_map[key] = len(header)
                    header.append(key)

                key = header_map[key]
                if key >= len(self):
                    for i in range(key - len(self) - 1):
                        self.append(b'')
                    self.append(value)
                    return
                key += 1

            return super().__setitem__(key if key < 0 else key-1, value)

        def __delitem__(self, key):
            if isinstance(key, str):
                key = header_map.pop(key)
            return super().__delitem__(key)

    return cls

class exec_(_Base):
    ''' run python on each row '''
    name = 'exec'
    parser = argparse.ArgumentParser()
    parser.add_argument('script', nargs='+')
    parser.add_argument('-q', '--quiet', action='store_true')
    parser.add_argument('-I', '--ignore-errors', action='store_true')
    parser.add_argument('-E', '--remove-errors', action='store_true')

    def __init__(self, opts):
        super().__init__(opts)

        script = '\n'.join(opts.script)
        self.code = compile(script, '<string>', 'exec')
        self.printed_header = False
        self.count = 0
        self.row_cls = None

    def on_header(self, header):
        self.modifiable_header = header.copy()

    def on_row(self, row):
        self.row_cls = self.row_cls or make_row_class(self.modifiable_header or [])

        self.count = self.count + 1
        row = self.row_cls(row)
        locals = {'row': row, 'N': self.count, 'header': self.header}

        try:
            exec(self.code, locals=locals)
        except Exception as e:
            if not self.opts.quiet:
                print(f'{type(e).__name__}: {e}', file=sys.stderr)
            if self.opts.remove_errors:
                return
            if not self.opts.ignore_errors and not self.opts.quiet:
                raise

        if not self.printed_header:
            super().on_header(self.modifiable_header)
            self.printed_header = True

        if locals.get('row') is not None:
            row = [col if isinstance(col, bytes) else str(col).encode('utf8') for col in locals['row']]
            super().on_row(row)

    def on_eof(self):
        if not self.printed_header:
            super().on_header(self.modifiable_header)
            self.printed_header = True
        super().on_eof()
