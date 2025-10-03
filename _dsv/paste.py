import argparse
import itertools
from ._base import _Base
from ._utils import shtab

class paste(_Base):
    ''' concatenate files by column '''
    parser = argparse.ArgumentParser()
    parser.add_argument('files', type=argparse.FileType('rb'), nargs='*', help='other files to concatenate to stdin').complete = shtab.FILE

    empty_rows = None

    def __init__(self, opts):
        self.original_opts = argparse.Namespace(**vars(opts))
        super().__init__(opts)

        default = (None, False)
        generators = [_Base(self.original_opts).process_file(file, do_yield=True, do_callbacks=False) for file in self.opts.files]
        self.generator = itertools.chain(itertools.zip_longest(*generators, fillvalue=default), itertools.repeat(default))

    def on_header(self, header):
        return super().on_header(self.paste_row(header))

    def on_row(self, row):
        return super().on_row(self.paste_row(row))

    def paste_row(self, row):
        rows, is_header = zip((row, False), *next(self.generator))

        if self.empty_rows is None:
            rows = [r for r in rows if r is not None]
            self.empty_rows = [[b''] * len(r) for r in rows]

        if None in rows:
            rows = list(rows)
            # pad rows that are missing
            for i, r in enumerate(rows):
                if rows[i] is None:
                    rows[i] = self.empty_rows[i]

        return sum(rows, start=[])
