import argparse
import threading
from queue import Queue
from ._column_slicer import _ColumnSlicer
from . import _utils

class join(_ColumnSlicer):
    ''' join lines of two files on a common field '''
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=argparse.FileType('rb'), metavar='FILE', help='join stdin with FILE')
    parser.add_argument('fields', nargs='*', help='join on these fields from stdin and FILE')
    parser.add_argument('-1', dest='left_fields', action='append', help='join on these fields from stdin')
    parser.add_argument('-2', dest='right_fields', action='append', help='join on these fields from FILE')
    parser.add_argument('-e', dest='empty_value', type=_utils.utf8_type, default='', metavar='STRING', help='replace missing input fields with STRING')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-a', dest='show_all', choices=('1', '2'), action='append', help='also print unpairable lines from the given file')
    group.add_argument('--join', choices=('inner', 'left', 'right', 'outer'), default='inner', help='type of join to perform')

    def __init__(self, opts):
        opts.fields.extend(opts.extras)
        opts.extras = ()

        if opts.fields and (opts.left_fields or opts.right_fields):
            self.parser.error('Cannot set common fields and use -1 or -2')

        right_opts = argparse.Namespace(**vars(opts))
        right_opts.fields = right_opts.fields.copy() or right_opts.right_fields or []
        opts.fields = opts.fields or opts.left_fields or []

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
        self.header_event = threading.Event()

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
        self.got_header()

    def on_collector_header(self, header):
        self.collector.header_map = self.make_header_map(self.collector.header)
        self.got_header()

    def got_header(self):
        no_join_fields = not self.opts.fields and not self.collector.opts.fields

        # the semaphore only has 1, so the second time it is called it will return false and go through
        if not self.header_lock.acquire(blocking=False):
            if no_join_fields:
                self.opts.fields = self.collector.opts.fields = list(set(self.header) & set(self.collector.header))
            header = self.paste_row(self.header, self.collector.header)
            super().on_header(header)
            self.header_event.set()

        if no_join_fields:
            # wait for the other thread to set the join fields
            self.header_event.wait()

    def paste_row(self, left, right):
        return self.slice(left) + self.slice(left, True) + self.collector.slice(right, True)

    def on_row(self, row, ofs=b'\x00'):
        key = tuple(self.slice(row, False))
        self.left.setdefault(key, []).append(row)

    def on_eof(self, ofs=b'\x00'):
        matched = set()

        # left has finished, now read off the right
        while (right := self.right.get()) is not None:
            key = tuple(self.collector.slice(right, False))
            right = self.collector.slice(right, True)
            matched.add(key)

            lefts = self.left.get(key, ())
            for left in lefts:
                super().on_row(list(key) + self.slice(left, True) + right)

            if not lefts and self.opts.join in ('right', 'outer'):
                padding = [self.opts.empty_value] * (len(self.header) - len(key))
                super().on_row(list(key) + padding + right)

        if self.opts.join in ('left', 'outer'):
            for key in self.left.keys() - matched:
                padding = [self.opts.empty_value] * (len(self.collector.header) - len(key))
                for left in self.left[key]:
                    super().on_row(list(key) + self.slice(left, True) + padding)

        super().on_eof()
        self.thread.join()
