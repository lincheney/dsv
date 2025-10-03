import sys
import json
import argparse
from ._base import _Base

def flatten(d, sep='.', parent_key=None):
    data = {}
    if isinstance(d, dict):
        items = d.items()
    elif isinstance(d, list):
        items = enumerate(d)
    else:
        data[parent_key] = d
        return data

    for i, v in items:
        key = f"{parent_key}{sep}{i}" if parent_key else str(i)
        data.update(flatten(v, sep=sep, parent_key=key))
    return data

class fromjson(_Base):
    ''' convert from json '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--flatten', nargs='?', const='.', help='flatten objects and arrays. (default seperator: %(const)s)')
    parser.add_argument('-s', '--slurp', action='store_true', help='determine header after reading all input')

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
        utf8_buf = utf8_buf.lstrip()

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
                    if isinstance(value, dict):
                        if self.opts.flatten:
                            value = flatten(value, self.opts.flatten)
                        yield value
                    else:
                        print('not a json object:', value, file=sys.stderr)
            except UnicodeDecodeError:
                break

        if rest:
            print('invalid json:', rest, file=sys.stderr)

    def calc_header(self, rows: list[dict]):
        self.header = list({key.encode('utf8'): None for row in rows for key in row})

    def process_file(self, file):
        self.determine_delimiters(b'')
        rows = self.iter_json(file)

        if self.opts.slurp:
            rows = list(rows)
            self.calc_header(rows)
            if super().on_header(self.header):
                return True

        for row in rows:
            if self.on_row(row):
                break

        self.on_eof()
        return ()

    def on_header(self, header):
        return self.on_row(header)

    def on_row(self, row):
        if self.header is None:
            self.calc_header([row])
            if super().on_header(self.header):
                return True

        row = [row.get(k.decode('utf8'), '') for k in self.header]
        row = [(x if isinstance(x, str) else json.dumps(x)).encode('utf8') for x in row]
        return super().on_row(row)
