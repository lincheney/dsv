import argparse
import copy
from ._base import _Base

class _Pipeline(_Base):
    def __init__(self, opts, pipeline: list[_Base]):
        super().__init__(opts)
        self.pipeline = pipeline

        self.process_file = self.pipeline[0].process_file

        original = self.pipeline[0].determine_delimiters
        def determine_delimiters(*args, original=original, **kwargs):
            original(*args, **kwargs)
            self.pipeline[-1].opts.ofs = self.pipeline[0].opts.ofs
            # disable colour and stuff
            for p in self.pipeline[:-1]:
                p.opts.ofs = b'\t'
                p.opts.trailer = False
                p.opts.colour = False
                p.opts.numbered_columns = False
                p.opts.rainbow_columns = False
        self.pipeline[0].determine_delimiters = determine_delimiters

        for src, dst in zip(self.pipeline[:-1], self.pipeline[1:]):

            def print_row(row, padding=None, is_header=False, dst=dst):
                if is_header:
                    dst.header = row.copy()
                    return dst.on_header(row)
                else:
                    return dst.on_row(row)
            src.print_row = print_row

            original = src.on_eof
            def on_eof(dst=dst, original=original):
                original()
                dst.on_eof()
            src.on_eof = on_eof

    def copy_opts(self, opts, **kwargs):
        opts = copy.deepcopy(opts)
        if kwargs:
            opts = argparse.Namespace(**{**vars(opts), **kwargs})
        return opts
