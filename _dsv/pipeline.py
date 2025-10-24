import argparse
from ._base import _Base, get_all_handlers, make_main_parser
from ._shtab import _quote

completion = {
    'zsh': _quote('''{() {
        local _drop=${words[(I)!]}
        local words=( dsv "${words[@]:$_drop}" )
        local CURRENT=$(( CURRENT+1-_drop ))
        _normal
    } }'''),
    'bash': '''
    _shtab__dsv_pipeline_dsv_custom_complete
    _shtab__dsv_original_ifs="$IFS"
    _shtab__dsv_pipeline_dsv_custom_complete() {
        local IFS="$_shtab__dsv_original_ifs"
        local _drop=${#COMP_WORDS[@]}
        while [[ $_drop > 0 && "${COMP_WORDS[$_drop-1]}" != '!' ]]; do (( _drop -- )); done
        local COMP_WORDS=( dsv "${COMP_WORDS[@]:$_drop}" )
        local COMP_CWORD=$(( COMP_CWORD+1-_drop ))
        _shtab__dsv
        printf '%s\\n' "${COMPREPLY[@]}"
    }
    '''.strip(),
}

class pipeline(_Base):
    ''' pipe multiple dsv commands together '''
    name = '!'

    parser = argparse.ArgumentParser()
    parser.add_argument('command', nargs=argparse.REMAINDER).complete = completion

    DEFAULTS = dict(
        #  ofs = b'\t',
        trailer = False,
        colour = False,
        numbered_columns = False,
        rainbow_columns = False,
        hyperlink_columns = False,
        drop_header = False,
        page = False,
    )

    def __init__(self, opts, pipeline=None):
        self.pipeline = pipeline
        if self.pipeline is None:

            # what options were actually set
            parser = make_main_parser(argument_default=argparse.SUPPRESS)
            kwargs = vars(parser.parse_known_args(opts.args)[0])
            kwargs.pop('handler', None)
            kwargs.pop('command', None)

            self.pipeline = [[]]
            for arg in opts.command:
                if arg == '!':
                    self.pipeline.append([])
                else:
                    self.pipeline[-1].append(arg)
            opts.extras = ()
            self.pipeline = [self.action(i==len(self.pipeline)-1, *a, **kwargs) for i, a in enumerate(self.pipeline)]

        super().__init__(opts)

        first = self.pipeline[0]
        last = self.pipeline[-1]
        # input goes to first action
        self.process_file = first.process_file

        if first is not last:
            # apply guessed ofs on first action to last action
            def determine_ofs(*args, **kwargs):
                last.determine_ofs(*args, **kwargs)
                ofs = b'\t' if last.opts.ofs is self.PRETTY_OUTPUT else last.opts.ofs
                for p in self.pipeline[:-1]:
                    if p.opts.ofs is None:
                        p.opts.ofs = ofs
            first.determine_ofs = determine_ofs

        # pipe from left to right
        for src, dst in zip(self.pipeline[:-1], self.pipeline[1:]):
            src.opts.ofs = b'\t'

            def write_output(row, padding=None, is_header=False, stderr=False, dst=dst):
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

    def action(self, last: bool, name, *args, **kwargs):
        if handler := next((h for h in get_all_handlers() if h.get_name() == name), None):
            if not last:
                if kwargs.get('ofs') == self.PRETTY_OUTPUT:
                    del kwargs['ofs']
                kwargs = {**self.DEFAULTS, **kwargs}
            return handler.from_args(args, **kwargs)
        raise ValueError(f'cannot find handler named {name}')

    def _cleanup(self, subject, *rest):
        try:
            subject.cleanup()
        finally:
            if rest:
                self._cleanup(*rest)

    def cleanup(self):
        self._cleanup(*self.pipeline, super())
