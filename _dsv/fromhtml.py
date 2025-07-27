import sys
from html.parser import HTMLParser
import html
import argparse
import threading
from queue import Queue
from concurrent.futures import Future
from functools import partial
from ._base import _Base

class Parser(HTMLParser):
    def __init__(self, callback, strict=False, inner_html=False):
        super().__init__()
        self.callback = callback
        self.state = []
        self.got_header = False
        self.strict = strict
        self.inner_html = inner_html
        self.rowspans = {}

    def apply_rowspans(self):
        i = len(self.current_row) + 1
        while value := self.rowspans.get(i):
            self.current_row.append(value[1])
            i += 1

    def decrement_rowspans(self):
        for k, v in list(self.rowspans.items()):
            if v[0] <= 1:
                self.rowspans.pop(k)
            else:
                v[0] -= 1

    def handle_starttag(self, tag, attrs):
        if self.state and self.state[-1] in {'th', 'td'}:
            if self.inner_html:
                self.current_row[-1] += f'<{tag} {' '.join(f'{k}="{html.escape(v)}"' for k, v in attrs)}>'
            return

        self.state.append(tag)
        match self.state[-2:]:
            case \
                  ['table' | 'thead' | 'tbody'] \
                | ['table', 'thead' | 'tbody' | 'tr'] \
                | ['thead' | 'tbody', 'tr'] \
                | ['tr', 'th' | 'td']:
                # good

                if tag == 'tr':
                    # new row
                    self.current_row = []
                    self.decrement_rowspans()
                    self.apply_rowspans()

                elif tag in {'td', 'th'}:
                    self.apply_rowspans()
                    # new column
                    self.current_row.append('')

                    if rowspan := dict(attrs).get('rowspan'):
                        if rowspan.isnumeric() and (rowspan := int(rowspan)) and rowspan > 0:
                            self.rowspans[len(self.current_row)] = [rowspan, '']
                        else:
                            print(f'invalid rowspan {rowspan!r}', file=sys.stderr)

                else:
                    # new table
                    self.rowspans.clear()

            case _:
                # bad
                if self.strict:
                    raise ValueError(f'invalid tags {self.state}')
                else:
                    self.state.pop()

    def handle_endtag(self, tag):
        old_state = self.state

        if self.state and self.state[-1] in {'th', 'td'} and tag != self.state[-1]:
            if self.inner_html:
                self.current_row[-1] += f'</{tag}>'

        if tag in self.state:
            ix = list(reversed(self.state)).index(tag) + 1
            self.state = self.state[:-ix]

        if 'tr' in old_state and 'tr' not in self.state:
            is_header = 'thead' in old_state
            if is_header and self.got_header:
                print('got duplicate html table header', file=sys.stderr)
            else:
                self.apply_rowspans()
                self.callback((self.current_row, is_header))
                self.got_header = is_header

    def handle_data(self, data):
        if self.state and self.state[-1] in {'td', 'th'}:
            self.current_row[-1] += data

            if rowspan := self.rowspans.get(len(self.current_row)):
                rowspan[1] = self.current_row[-1]

class fromhtml(_Base):
    ''' convert from html table '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--strict', action='store_true', help='only allow valid table')
    parser.add_argument('--inner-html', action='store_true', help='output the innerHTML of table cells, not the innerText')

    def process_file(self, file, do_callbacks=True, do_yield=False):
        self.determine_delimiters(b'')

        got_row = False
        queue = Queue()
        thread = threading.Thread(target=self.parse, args=[queue, file])
        thread.start()

        while True:
            fut = queue.get()
            if do_callbacks and (fut is None or fut.exception() is not None):
                self.on_eof()
            if fut is None:
                break

            got_row = True
            item = fut.result()
            row, is_header = item
            row = [x.encode('utf8').strip() for x in row]
            if do_callbacks and (self.on_header(row) if is_header else self.on_row(row)):
                break
            if do_yield:
                yield item

        thread.join()
        return got_row

    def parse(self, queue, *args, **kwargs):
        try:
            self._parse(queue, *args, **kwargs)
        except Exception as e:
            fut = Future()
            fut.set_exception(e)
            queue.put_nowait(fut)
        finally:
            queue.put_nowait(None)

    def parser_callback(self, queue, result):
        fut = Future()
        fut.set_result(result)
        queue.put_nowait(fut)

    def _parse(self, queue, file, chunk=8192):
        parser = Parser(partial(self.parser_callback, queue), self.opts.strict, self.opts.inner_html)
        remainder = b''
        while buf := file.read1(chunk):
            buf = remainder + buf

            try:
                utf8_buf = buf.decode('utf8')
            except UnicodeDecodeError as e:
                utf8_buf = buf[:e.start].decode('utf8')
                if not utf8_buf:
                    raise
                remainder = buf[e.start:]
            else:
                remainder = b''

            parser.feed(utf8_buf)

        if remainder:
            # probably raises a decode error
            remainder.decode('utf8')

        parser.close()
        queue.put_nowait(None)
