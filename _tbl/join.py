import argparse
import threading
from queue import Queue
from ._column_slicer import _ColumnSlicer

class join(_ColumnSlicer):
    ''' join lines of two files on a common field '''
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=argparse.FileType('rb'))
    parser.add_argument('fields', nargs='*')
    parser.add_argument('-1', dest='left_fields', action='append')
    parser.add_argument('-2', dest='right_fields', action='append')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-a', dest='show_all', choices=('1', '2'), action='append')
    group.add_argument('--join', choices=('inner', 'left', 'right', 'outer'), default='inner')

    def __init__(self, opts):
        opts.fields.extend(opts.extras)
        opts.extras = ()

        if opts.fields and (opts.left_fields or opts.right_fields):
            self.parser.error('Cannot set common fields and use -1 or -2')

        right_opts = argparse.Namespace(**vars(opts))
        right_opts.fields = right_opts.fields.copy() or right_opts.right_fields
        opts.fields = opts.fields or opts.left_fields

        if opts.show_all:
            if '1' in opts.show_all and '2' in opts.show_all:
                opts.join = 'outer'
            elif '1' in opts.show_all:
                opts.join = 'left'
            elif '2' in opts.show_all:
                opts.join = 'right'

        super().__init__(opts)
        self.left = {}
        self.right = Queue()
        self.header_lock = threading.Semaphore(1)

        self.collector = _ColumnSlicer(right_opts)
        self.collector.on_header = self.on_collector_header
        self.collector.on_row = self.right.put_nowait
        self.collector.on_eof = lambda: 0 # nop

        self.thread = threading.Thread(target=self.run_collector, daemon=True)
        self.thread.start()

    def run_collector(self):
        try:
            list(self.collector.process_file(self.opts.file))
        finally:
            self.right.put_nowait(None)

    def on_header(self, header):
        self.header_map = self.make_header_map(self.header)
        self.print_header()

    def on_collector_header(self, header):
        self.collector.header_map = self.make_header_map(self.collector.header)
        self.print_header()

    def print_header(self):
        # the semaphore only has 1, so the second time it is called it will return false and go through
        if not self.header_lock.acquire(blocking=False):
            header = self.paste_row(self.header, self.collector.header)
            super().on_header(header)

    def paste_row(self, left, right):
        return left + self.collector.slice(right, True)

    def on_row(self, row, ofs=b'\x00'):
        key = self.slice(row, False)
        key = ofs.join(self.format_columns(key, ofs, ofs, True))
        self.left.setdefault(key, []).append(row)

    def on_eof(self, ofs=b'\x00'):
        matched = set()

        # left has finished, now read off the right
        while (right := self.right.get()) is not None:
            key = self.collector.slice(right, False)
            key = ofs.join(self.format_columns(key, None, None, False))
            matched.add(key)

            lefts = self.left.get(key, ())
            for left in lefts:
                row = self.paste_row(left, right)
                super().on_row(row)

            if not lefts and self.opts.join in ('right', 'outer'):
                row = self.paste_row([b''] * self.header, right)
                super().on_row(row)

        if self.opts.join in ('left', 'outer'):
            for key in self.left.keys() - matched:
                for left in self.left[key]:
                    super().on_row(left)

        super().on_eof()
        self.thread.join()
