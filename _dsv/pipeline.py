import argparse
import copy
from ._base import _Base, get_all_handlers

class pipeline(_Base):
    ''' pipe multiple dsv commands together '''
    name = '!'

    def __init__(self, opts, pipeline=None):
        self.pipeline = pipeline

        if self.pipeline is None:
            self.pipeline = [[]]
            for arg in opts.extras:
                if arg == '!':
                    self.pipeline.append([])
                else:
                    self.pipeline[-1].append(arg)
            opts.extras = ()
            self.pipeline = [self.action(*a, **vars(opts)) for a in self.pipeline]

        # input goes to first action
        self.process_file = self.pipeline[0].process_file

        # apply guessed ofs on first action to last action
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

        # pipe from left to right
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

    def action(self, name, *args, **kwargs):
        handler = next(h for h in get_all_handlers() if h.get_name() == name)
        return handler.from_args(args, **kwargs)
