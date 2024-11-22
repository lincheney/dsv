import re
from ._base import _Base

class _ColumnSlicer(_Base):
    def __init__(self, opts):
        super().__init__(opts)
        self.header_map = {}

        for i, f in enumerate(opts.fields):
            if f != '-' and (match := re.fullmatch(r'(\d*)-(\d*)', f)):
                s, e = match.groups()
                opts.fields[i] = (int(s)-1 if s else 0, int(e)-1 if e else float('inf'))
            elif f.isdigit():
                opts.fields[i] = int(f) - 1
            elif isinstance(f, str):
                opts.fields[i] = f.encode('utf8')

    def make_header_map(self, header):
        return {k: i for i, k in enumerate(header)}

    def on_header(self, header):
        self.header_map = self.make_header_map(self.header)
        return super().on_header(header)

    def slice(self, row, complement=False, allow_empty=True, default=None):
        if not self.opts.fields:
            return row

        newrow = complement and row.copy() or []

        for f in self.opts.fields:
            if isinstance(f, tuple):
                # add/remove all fields in the range
                for i in range(f[0], min(f[1]+1, len(row))):
                    if complement:
                        newrow[i] = None
                    else:
                        newrow.append(row[i])
            else:
                i = f if isinstance(f, int) else self.header_map.get(f)
                if i is not None and i < len(row):
                    if complement:
                        newrow[i] = None
                    else:
                        newrow.append(row[i])
                elif not complement and allow_empty and i is not None:
                    # add blank if column exists but just not for this row
                    # to make sure all columns align
                    if default is None:
                        newrow.append(b'')
                    else:
                        newrow.append(default(i))

        if complement:
            newrow = [x for x in newrow if x is not None]
        return newrow
