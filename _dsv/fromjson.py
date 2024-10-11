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
            if not utf8_buf:
                raise
            remainder = buffer[e.start:]
        else:
            remainder = b''

        value, index = json_decoder.raw_decode(utf8_buf)
        return value, utf8_buf[index:].lstrip().encode('utf8') + remainder

    def iter_json(self, file, chunk=8192):
        rest = b''
        while buf := file.read1(chunk):
            rest += buf
            try:
                while rest:
                    try:
                        value, rest = self.parse_json(rest)
                    except json.JSONDecodeError:
                        break
                    yield value
            except UnicodeDecodeError:
                break

        if rest:
            print('invalid json:', rest, file=sys.stderr)

    def process_file(self, file):
        self.determine_delimiters(b'')

        for row in self.iter_json(file):
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
