import sys
import argparse
from ._base import _Base

class Row(list):
    def __init__(self, values, header):
        super().__init__(values)
        private = self.__dict__['__private'] = {}
        private['header'] = header
        private['header_map'] = {k: i for i, k in enumerate(header)}

    def __setattr__(self, key, value):
        self[key] = value
    def __getattr__(self, key):
        return self[key]

    def __getitem__(self, key):
        if isinstance(key, str):
            key = key.encode('utf8')
            key = self.__dict__['__private']['header_map'][key] + 1
        if key > len(self):
            return ''
        return super().__getitem__(key if key < 0 else key-1).decode('utf8')

    def __setitem__(self, key, value):
        if not isinstance(value, bytes):
            value = str(value).encode('utf8')

        if isinstance(key, str):
            key = key.encode('utf8')
            private = self.__dict__['__private']
            if key not in private['header_map']:
                private['header_map'][key] = len(private['header'])
                private['header'].append(key)

            key = private['header_map'][key]
            if key >= len(self):
                for i in range(key - len(self) - 1):
                    self.append(b'')
                self.append(value)
                return
            key += 1

        return super().__setitem__(key if key < 0 else key-1, value)

    def __delitem__(self, key):
        if isinstance(key, str):
            key = self.__header_map.pop(key)
        return super().__delitem__(key)

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

    def on_header(self, header):
        self.modifiable_header = header.copy()

    def on_row(self, row):
        self.count = self.count + 1
        row = Row(row, self.modifiable_header or [])
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
