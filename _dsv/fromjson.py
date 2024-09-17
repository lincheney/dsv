import json
import argparse
from ._base import _Base

class fromjson(_Base):
    ''' convert from json '''
    parser = argparse.ArgumentParser()
    parser.set_defaults(ofs=_Base.PRETTY_OUTPUT)

    def process_file(self, file):
        for line in file:
            row = json.loads(line)
            if isinstance(row, dict):
                if self.header is None:
                    self.header = [x.encode('utf8') for x in row.keys()]
                    if self.on_header(self.header):
                        break
                row = [(x if isinstance(x, str) else json.dumps(x)).encode('utf8') for x in row.values()]
                if self.on_row(row):
                    break
        self.on_eof()
        return ()
