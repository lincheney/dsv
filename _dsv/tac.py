from ._base import _Base

class tac(_Base):
    ''' print the file in reverse '''

    def __init__(self, opts):
        super().__init__(opts)
        self.rows = []

    def on_row(self, row):
        self.rows.append(row)

    def on_eof(self):
        for row in reversed(self.rows):
            if super().on_row(row):
                break
        super().on_eof()
