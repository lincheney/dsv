import json
from ._base import _Base

class tojson(_Base):
    ''' convert to json '''

    def on_header(self, header):
        pass

    def on_eof(self):
        pass

    def on_row(self, row):
        values = {}
        for i, col in enumerate(row):
            key = i
            if self.header and i < len(self.header):
                key = self.header[i].decode('utf8')
            values[key] = col.decode('utf8')
        self.outfile.write(json.dumps(values).encode('utf8') + self.opts.ors)
