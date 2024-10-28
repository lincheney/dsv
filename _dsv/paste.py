import argparse
import itertools
from ._base import _Base

class paste(_Base):
    ''' concatenate files by column '''
    parser = argparse.ArgumentParser()
    parser.add_argument('files', type=argparse.FileType('rb'), nargs='*', help='other files to concatenate to stdin')

    empty_rows = None

    def __init__(self, opts):
        self.original_opts = argparse.Namespace(**vars(opts))
        super().__init__(opts)

    def process_file(self, file):
        generators = []
        generators.append(super().process_file(file, do_yield=True, do_callbacks=False))
        for file in self.opts.files:
            generators.append(_Base(self.original_opts).process_file(file, do_yield=True, do_callbacks=False))

        for values in itertools.zip_longest(*generators, fillvalue=(None, False)):
            rows, is_header = zip(*values)

            if self.empty_rows is None:
                rows = [r for r in rows if r is not None]
                self.empty_rows = [[b''] * len(h) for h in rows]

            if None in rows:
                rows = list(rows)
                # pad rows that are missing
                for i, r in enumerate(rows):
                    if rows[i] is None:
                        rows[i] = self.empty_rows[i]

            row = sum(rows, start=[])
            if is_header[0]:
                self.header = row
                self.on_header(row)
            else:
                self.on_row(row)

        yield
        self.on_eof()
