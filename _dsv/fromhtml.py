import sys
from html.parser import HTMLParser
import threading
from queue import Queue
from concurrent.futures import Future
from functools import partial
from ._base import _Base

class Parser(HTMLParser):
    def __init__(self, callback):
        super().__init__()
        self.callback = callback
        self.state = []
        self.got_header = False

    def handle_starttag(self, tag, attrs):
        if self.state and self.state[-1] in {'th', 'td'}:
            return

        if tag == 'tr' and tag not in self.state:
            self.current_row = []
        elif tag in {'td', 'th'} and tag not in self.state:
            self.current_row.append('')

        self.state.append(tag)
        if self.state not in [
            ['table'],
            ['table', 'thead'],
            ['table', 'thead', 'tr'],
            ['table', 'thead', 'tr', 'th'],
            ['table', 'tbody'],
            ['table', 'tbody', 'tr'],
            ['table', 'tbody', 'tr', 'td'],
        ]:
            raise ValueError(f'invalid tags {self.state}')

    def handle_endtag(self, tag):
        old_state = self.state

        if tag in self.state:
            ix = list(reversed(self.state)).index(tag) + 1
            self.state = self.state[:-ix]

        if 'tr' in old_state and 'tr' not in self.state:
            is_header = 'thead' in old_state
            if is_header and self.got_header:
                print('got duplicate html table header', file=sys.stderr)
            else:
                self.callback((self.current_row, is_header))
                self.got_header = is_header

    def handle_data(self, data):
        if self.state and self.state[-1] in {'td', 'th'}:
            self.current_row[-1] += data

class fromhtml(_Base):
    ''' convert from html table '''

    def process_file(self, file, do_callbacks=True, do_yield=False):
        self.determine_delimiters(b'')

        got_row = False
        queue = Queue()
        thread = threading.Thread(target=self.parse, args=[queue, file])
        thread.start()

        while (fut := queue.get()) is not None:
            item = fut.result()
            got_row = True
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
        parser = Parser(partial(self.parser_callback, queue))
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
