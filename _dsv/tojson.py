import json
import argparse
from ._base import _Base

class tojson(_Base):
    ''' convert to json '''
    parser = argparse.ArgumentParser()
    parser.set_defaults(drop_header=True, ofs=b',')

    def format_row(self, data, *args, **kwargs):
        values = {}
        for i, col in enumerate(data):
            key = i
            if self.header and i < len(self.header):
                key = self.header[i].decode('utf8')
            values[key] = col.decode('utf8')
        return json.dumps(values).encode('utf8')
