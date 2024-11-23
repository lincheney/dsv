import json
import argparse
from ._base import _Base

class tojson(_Base):
    ''' convert to json '''
    parser = argparse.ArgumentParser()
    parser.set_defaults(drop_header=True, ofs=b'')

    def on_header(self, header):
        pass

    def on_row(self, row):
        values = {}
        for i, col in enumerate(row):
            key = i
            if self.header and i < len(self.header):
                key = self.header[i].decode('utf8')
            values[key] = col.decode('utf8')
        return super().on_row(values)

    def format_row(self, data, *args, **kwargs):
        return json.dumps(data).encode('utf8')
