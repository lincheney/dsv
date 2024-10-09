import sys
import json
from ._base import _Base

class fromjson(_Base):
    ''' convert from json '''

    def parse_json(self, buffer, json_decoder=json.JSONDecoder()):
        try:
            utf8_buf = buffer.decode('utf8')
        except UnicodeDecodeError as e:
            utf8_buf = buffer[:e.start].decode('utf8')
            remainder = buffer[e.start:]
        else:
            remainder = b''

        value, index = json_decoder.raw_decode(utf8_buf)
        return value, utf8_buf[index:].lstrip().encode('utf8') + remainder

    def read_json(self, file, bufsize=4096):
        buffer = b''
        while True:
            data = file.read(bufsize)

            if not data and not buffer:
                # no more data
                return

            buffer += data
            while True:
                try:
                    value, buffer = self.parse_json(buffer)
                except json.JSONDecodeError:
                    if not data:
                        print('invalid json:', buffer, file=sys.stderr)
                        return
                    break
                yield value

    def process_file(self, file):
        self.determine_delimiters(b'')

        for row in self.read_json(file):
            if not isinstance(row, dict):
                print('not a json object:', row, file=sys.stderr)
                continue

            if self.header is None:
                self.header = [x.encode('utf8') for x in row.keys()]
                if self.on_header(self.header):
                    break
            row = [row.get(k.decode('utf8'), '') for k in self.header]
            row = [(x if isinstance(x, str) else json.dumps(x)).encode('utf8') for x in row]
            if self.on_row(row):
                break

        self.on_eof()
        return ()
