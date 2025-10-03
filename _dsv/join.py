import argparse
import threading
from queue import Queue
from ._column_slicer import _ColumnSlicer
from . import _utils
from ._shtab import shtab

class join(_ColumnSlicer):
    ''' join lines of two files on a common field '''
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=argparse.FileType('rb'), metavar='FILE', help='join stdin with FILE').complete = shtab.FILE
    parser.add_argument('fields', nargs='*', help='join on these fields from stdin and FILE')
    parser.add_argument('-1', dest='left_fields', action='append', help='join on these fields from stdin')
    parser.add_argument('-2', dest='right_fields', action='append', help='join on these fields from FILE')
    parser.add_argument('-e', dest='empty_value', type=_utils.utf8_type, default='', metavar='STRING', help='replace missing input fields with STRING')
    parser.add_argument('-r', '--regex', action='store_true', help='treat fields as regexes')
    parser.add_argument('--rename-1', type=_utils.utf8_type, help='rename header from stdin according to this %%-format string')
    parser.add_argument('--rename-2', type=_utils.utf8_type, help='rename header from FILE according to this %%-format string')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-a', dest='show_all', choices=('1', '2'), action='append', help='also print unpairable lines from the given file')
    group.add_argument('--join', choices=('inner', 'left', 'right', 'outer'), default='inner', help='type of join to perform')
    group.add_argument('--inner', action='store_const', dest='join', const='inner', help='do a inner join')
    group.add_argument('--left', action='store_const', dest='join', const='left', help='do a left join')
    group.add_argument('--right', action='store_const', dest='join', const='right', help='do a right join')
    group.add_argument('--outer', action='store_const', dest='join', const='outer', help='do a outer join')

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
        self.joined_header = None

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
        return self.got_header()

    def on_collector_header(self, header):
        self.collector.header_map = self.make_header_map(self.collector.header)
        return self.got_header()

    def got_header(self):
        no_join_fields = not self.opts.fields and not self.collector.opts.fields

        # the semaphore only has 1, so the second time it is called it will return false and go through
        if not self.header_lock.acquire(blocking=False):
            if no_join_fields:
                self.opts.fields = self.collector.opts.fields = list(set(self.header) & set(self.collector.header))

            left = self.header
            right = self.collector.header

            if self.opts.rename_1:
                left = [self.opts.rename_1 % h for h in left]
            if self.opts.rename_2:
                right = [self.opts.rename_2 % h for h in right]

            self.joined_header = self.paste_row(left, right)
            try:
                if super().on_header(self.joined_header):
                    return True
            finally:
                self.header_event.set()

        if no_join_fields:
            # wait for the other thread to set the join fields
            self.header_event.wait()

    def paste_row(self, left, right):
        return self.slice(left) + self.slice(left, True) + self.collector.slice(right, True)

    def on_row(self, row):
        key = tuple(self.slice(row, False))
        self.left.setdefault(key, []).append(row)

    def join_left_with_right(self):
        first_left = self.header or list(self.left.values() or [[]])[0]
        key_len = len(self.slice(first_left))
        left_len = len(self.slice(first_left, True))
        right_len = self.collector.header and len(self.collector.slice(self.collector.header, True))

        matched = set()

        # left has finished, now read off the right
        while (right := self.right.get()) is not None:

            if right_len is None:
                right_len = len(self.collector.slice(right, True))

            key = tuple(self.collector.slice(right, False))
            matched.add(key)
            lefts = self.left.get(key, ())

            key = list(key)
            if len(key) < key_len:
                key += [b''] * (key_len - len(key))

            # inner joins
            right = self.collector.slice(right, True)
            for left in lefts:
                row = self.slice(left, True)
                if len(row) < left_len:
                    row += [b''] * (left_len - len(row))

                if super().on_row(key + row + right):
                    return True

            # right joins
            if not lefts and self.opts.join in ('right', 'outer'):
                padding = [self.opts.empty_value] * left_len
                if super().on_row(key + padding + right):
                    return True

        if right_len is None:
            right_len = 0

        # left joins
        if self.opts.join in ('left', 'outer'):
            for key in self.left.keys() - matched:
                lefts = self.left[key]

                key = list(key)
                if len(key) < key_len:
                    key += [b''] * (key_len - len(key_len))

                padding = [self.opts.empty_value] * right_len
                for left in lefts:
                    row = key + self.slice(left, True)
                    if len(row) < left_len:
                        row += [b''] * (left_len - len(row))

                    if super().on_row(row + padding):
                        return True

    def on_eof(self):
        self.join_left_with_right()
        super().on_eof()
        self.thread.join()
