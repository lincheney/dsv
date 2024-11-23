import argparse
import copy
from ._base import _Base, get_all_handlers, make_main_parser

class pipeline(_Base):
    ''' pipe multiple dsv commands together '''
    name = '!'

    def __init__(self, opts, pipeline=None):
        self.pipeline = pipeline
        if self.pipeline is None:

            # what options were actually set
            parser = make_main_parser(argument_default=argparse.SUPPRESS)
            kwargs = vars(parser.parse_known_args(opts.args)[0])
            kwargs.pop('handler', None)
            kwargs.pop('command', None)

            self.pipeline = [[]]
            for arg in opts.extras:
                if arg == '!':
                    self.pipeline.append([])
                else:
                    self.pipeline[-1].append(arg)
            opts.extras = ()
            self.pipeline = [self.action(*a, **kwargs) for a in self.pipeline]

        super().__init__(opts)

        first = self.pipeline[0]
        last = self.pipeline[-1]
        # input goes to first action
        self.process_file = first.process_file

        # apply guessed ofs on first action to last action
        original = first.determine_delimiters
        def determine_delimiters(*args, original=original, **kwargs):
            original(*args, **kwargs)

            if last.opts.ofs is None:
                last.opts.ofs = first.opts.ofs

            # disable colour and stuff
            for p in self.pipeline[:-1]:
                p.opts.ofs = b'\t'
                p.opts.trailer = False
                p.opts.colour = False
                p.opts.numbered_columns = False
                p.opts.rainbow_columns = False
                p.opts.drop_header = False
                p.opts.page = False
        first.determine_delimiters = determine_delimiters

        # pipe from left to right
        for src, dst in zip(self.pipeline[:-1], self.pipeline[1:]):

            def write_output(row, padding=None, is_header=False, dst=dst):
                if is_header:
                    dst.header = row.copy()
                    return dst.on_header(row)
                else:
                    return dst.on_row(row)
            src.write_output = write_output

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
        if handler := next((h for h in get_all_handlers() if h.get_name() == name), None):
            return handler.from_args(args, **kwargs)
        raise ValueError(f'cannot find handler named {name}')
