import argparse
from ._column_slicer import _ColumnSlicer

class reshape_wide(_ColumnSlicer):
    ''' reshape to wide format '''
    name = 'reshape-wide'

    parser = argparse.ArgumentParser()
    parser.add_argument('key', help='key field')
    parser.add_argument('value', help='value field')

    def __init__(self, opts):
        opts.fields = [opts.key, opts.value]
        opts.regex = False
        super().__init__(opts)
        self.__rows = []

    def on_header(self, header):
        self.header_map = self.make_header_map(self.header)

    def on_row(self, row):
        self.__rows.append(row)

    def on_eof(self):
        new_headers = list({self.slice(row)[0] for row in self.__rows})
        if super().on_header(new_headers + self.slice(self.header, True)):
            return
        new_headers = {h: i for i, h in enumerate(new_headers)}

        groups = {}
        for row in self.__rows:
            key, value = self.slice(row)
            row = self.slice(row, True)

            k = tuple(row)
            if k not in groups:
                groups[k] = [None] * len(new_headers) + row

            groups[k][new_headers[key]] = value

            if None not in groups[k]:
                if super().on_row(groups.pop(k)):
                    break

        # print the remaining unmatched ones
        for row in groups.values():
            row = [x or b'' for x in row]
            if super().on_row(row):
                break
