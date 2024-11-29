import os
import re
import sys
import shutil
import pkgutil
import argparse
import colorsys
import subprocess
from functools import cache
from . import _utils
import _dsv

UTF8_BOM = '\ufeff'.encode('utf8')

class Separator(tuple[bytes]):
    pass

def interpret_c_escapes(x: str):
    return x.encode('utf8').decode('unicode_escape').encode('utf8')

@cache
def get_all_handlers():
    modules = [sub.name for sub in pkgutil.iter_modules(_dsv.__path__) if not sub.name.startswith('_')]
    return [getattr(__import__('_dsv.'+name, fromlist=[name]), name) for name in modules]

def make_parser(**kwargs):
    parser = argparse.ArgumentParser(allow_abbrev=False, **kwargs)
    group = parser.add_argument_group('common options')
    header_group = group.add_mutually_exclusive_group()
    header_group.add_argument('-H', '--header', const='yes', action='store_const', help='treat first row as a header')
    header_group.add_argument('-N', '--no-header', dest='header', const='no', action='store_const', help='do not treat first row as header')
    group.add_argument('--drop-header', action='store_true', help='do not print the header')
    group.add_argument('--trailer', choices=('never', 'always', 'auto'), nargs='?', help='print a trailer')
    group.add_argument('--numbered-columns', choices=('never', 'always', 'auto'), nargs='?', help='number the columns in the header')
    group.add_argument('-d', '--ifs', type=interpret_c_escapes, help='input field separator')
    group.add_argument('--plain-ifs', action='store_true', help='treat input field separator as a literal not a regex')
    group.add_argument('-D', '--ofs', type=interpret_c_escapes, help='output field separator')
    group.add_argument('--irs', type=interpret_c_escapes, help='input row separator')
    group.add_argument('--ors', type=interpret_c_escapes, help='output row separator')
    group.add_argument('--csv', dest='ifs', action='store_const', const=b',', help='treat input as csv')
    group.add_argument('--tsv', dest='ifs', action='store_const', const=b'\t', help='treat input as tsv')
    group.add_argument('--ssv', dest='ifs', action='store_const', const=br'\s+', help='treat input as whitespace separated')
    group.add_argument('--combine-trailing-columns', action='store_true', help='if a row has more columns than the header, combine the last ones into one, useful with --ssv')
    group.add_argument('-P', '--pretty', dest='ofs', action='store_const', const=_Base.PRETTY_OUTPUT, help='prettified output')
    group.add_argument('--page', action='store_true', help='show output in a pager (less)')
    group.add_argument('--colour', '--color', choices=('never', 'always', 'auto'), nargs='?', help='enable colour')
    group.add_argument('--header-colour', type=_utils.utf8_type, help='ansi escape code for the header')
    group.add_argument('--header-bg-colour', type=_utils.utf8_type, help='ansi escape code for the header background')
    group.add_argument('--rainbow-columns', choices=('never', 'always', 'auto'), nargs='?', help='enable rainbow columns')
    group.add_argument('-Q', '--no-quoting', action='store_true', help='do not handle quotes from input')
    return parser

def make_main_parser(sub_mapping={}, handlers=None, help=None, argument_default=None):
    parent = make_parser(add_help=False, argument_default=argparse.SUPPRESS)
    parser = make_parser(formatter_class=argparse.RawTextHelpFormatter, argument_default=argument_default)
    parser.set_defaults(handler=None)

    if handlers is None:
        handlers = get_all_handlers()

    descr = '\n'.join(sorted(f'{h.get_name().ljust(20)}{h.__doc__ or ""}' for h in handlers))
    subparsers = parser.add_subparsers(dest='command', title='Commands', help=help, description=descr)

    for h in sorted(handlers, key=lambda h: h.get_name()):
        parents = [parent]
        if h.parser:
            parents.insert(0, h.parser)
            h.parser.prog = parser.prog + ' ' + h.get_name()
        sub = subparsers.add_parser(h.get_name(), parents=parents, description=h.__doc__, add_help=not h.parser, help=None)
        sub.set_defaults(handler=h)
        sub_mapping[h] = sub

    return parser

class _Base:
    SPACE = re.compile(br'\s+')
    PPRINT = re.compile(br'\s\s+')
    PRETTY_OUTPUT = object()
    PRETTY_OUTPUT_DELIM = b'  '
    RESET_COLOUR = b'\x1b[0m'

    name = None
    parser = None
    header = None
    row_count = 0
    outfile = None
    outfile_proc = None
    out_header = None

    def __init__(self, opts, outfile=None):
        self.opts = opts
        self.outfile = outfile

        # setup subprocess outfiles later
        if not self.outfile and not _utils.stdout_is_tty():
            self.outfile = sys.stdout.buffer

        if self.opts.extras:
            (self.parser or opts.parser).error('unrecognized arguments: ' + " ".join(self.opts.extras))

        # private variables
        self.__numcols = None
        self.__rgb_map = []
        self.__gathered_rows = []

    @classmethod
    def get_name(cls):
        return (cls.name or cls.__name__).replace('_', '-')

    @classmethod
    def from_args(cls, args, **kwargs):
        parser = make_main_parser(handlers=[cls])
        args = [cls.get_name(), *args]
        opts, extras = parser.parse_known_args(args)
        kwargs.setdefault('extras', extras)
        kwargs.setdefault('parser', parser)
        return cls.from_opts(args, opts, **kwargs)

    @classmethod
    def from_opts(cls, args, opts, extras, parser, **kwargs):
        opts.args = args
        opts.extras = extras
        opts.parser = parser
        if opts.irs is None:
            opts.irs = b'\n'
        if opts.ors is None:
            opts.ors = opts.irs
        if not hasattr(opts, 'quote_output'):
            opts.quote_output = True

        opts.trailer = opts.trailer or 'auto'
        opts.colour = os.environ.get('NO_COLOR', '') == '' and _utils.resolve_tty_auto(opts.colour or 'auto')
        opts.numbered_columns = _utils.resolve_tty_auto(opts.numbered_columns or 'auto')
        opts.rainbow_columns = opts.colour and _utils.resolve_tty_auto(opts.rainbow_columns or 'auto')
        opts.header_colour = opts.header_colour or b'\x1b[1;4m'
        opts.header_bg_colour = opts.header_bg_colour or b'\x1b[48;5;237m'

        for k, v in kwargs.items():
            setattr(opts, k, v)

        return cls(opts)

    @classmethod
    def guess_delimiter(cls, line, default):
        good_delims = (b'\t', b',')
        other_delims = (b'  ', b' ', b'|', b';')

        delims = {k: line.count(k) for k in good_delims}
        if not any(delims.values()):
            delims = {k: line.count(k) for k in other_delims}
        if not any(delims.values()):
            # no idea
            return default

        best_delim = max(delims, key=delims.get)
        if best_delim == b' ' and 2*delims.get(b'  ', 0) >= delims[b' ']:
            best_delim = b'  '

        if best_delim == b' ':
            if re.search(rb'\S \S', line):
                return cls.SPACE
            else:
                return cls.PPRINT
        elif best_delim == b'  ':
            return cls.PPRINT
        else:
            return best_delim

    def determine_delimiters(self, line):
        self.determine_ifs(line)
        self.determine_ofs(self.opts.ifs)

    def determine_ifs(self, line):
        opts = self.opts
        if opts.ifs:
            if isinstance(opts.ifs, bytes) and re.escape(opts.ifs) != opts.ifs and not opts.plain_ifs:
                opts.ifs = re.compile(opts.ifs)

        else:
            # guess delimiter if not specified
            opts.ifs = self.guess_delimiter(line, b'\t')
            if opts.ifs == self.SPACE or opts.ifs == self.PPRINT:
                opts.combine_trailing_columns = True
                # opts.no_quoting = True

        if isinstance(opts.ifs, bytes):
            def next_ifs(line, start, ifs=opts.ifs):
                i = line.find(ifs, start)
                if i == -1:
                    return None, None
                return i, i + len(ifs)
        else:
            def next_ifs(line, start, ifs=opts.ifs):
                if match := ifs.search(line, start):
                    return match.span()
                return None, None
        self.next_ifs = next_ifs

    def determine_ofs(self, ifs):
        opts = self.opts
        if not opts.ofs:
            if ifs == self.SPACE or ifs == self.PPRINT:
                if opts.colour:
                    opts.ofs = self.PRETTY_OUTPUT
                else:
                    opts.ofs = b' '*4
            elif isinstance(ifs, bytes):
                opts.ofs = ifs
            else:
                opts.ofs = b'\t'

    def iter_lines(self, file, sep, chunk=8192):
        rest = b''
        while buf := file.read1(chunk):
            rest += buf
            *lines, rest = rest.split(sep)
            yield from lines
        if rest:
            yield rest

    def process_file(self, file, do_callbacks=True, do_yield=False):
        row = []
        first = True
        got_row = False
        if self.opts.irs == b'\n':
            lines = file
        else:
            lines = self.iter_lines(file, self.opts.irs)

        sentinel = object()

        line = next(lines, sentinel)
        while line is not sentinel:

            line = line.removesuffix(self.opts.irs)
            if self.opts.irs == b'\n':
                line = line.removesuffix(b'\r')

            if first:
                first = False
                line = line.removeprefix(UTF8_BOM)
                self.determine_delimiters(line)

            row, incomplete = self.parse_line(line, row)
            line = next(lines, sentinel)

            if not incomplete or line is sentinel:
                got_row = True

                if self.header is None and self.opts.header is None:
                    self.opts.header = 'yes' if all(re.match(rb'[_a-zA-Z]', c) for c in row) else 'no'

                is_header = self.header is None and self.opts.header == 'yes'

                if is_header:
                    self.header = row
                    if do_callbacks and self.on_header(self.header):
                        break

                elif do_callbacks and self.on_row(row):
                    break

                if do_yield:
                    yield (row, is_header)

                row = []

        if do_callbacks:
            self.on_eof()

        return got_row

    def extract_column(self, line: bytes, start: int, line_len: int, quote=ord(b'"')):
        # quoted; find closing quote, skip over repeating ones
        i = line.find(b'"', start)
        value = line[start : None if i == -1 else i]

        # next char is another quote
        while i != -1 and i+1 < line_len and line[i+1] == quote:
            j = line.find(b'"', i+2)
            value += line[i+1 : None if j == -1 else j]
            i = j

        return value, i

    def parse_line(self, line: bytes, row: list, quote=ord(b'"')):
        allow_quoted = not self.opts.no_quoting
        maxcols = len(self.header) if self.opts.combine_trailing_columns and self.header is not None else None

        if not allow_quoted or b'"' not in line:
            if row:
                # complete the previously incomplete row
                row[-1] += line
                return row, True
            elif isinstance(self.opts.ifs, bytes):
                return line.split(self.opts.ifs, (maxcols or 0) -1), False
            else:
                return self.opts.ifs.split(line, (maxcols or 1) - 1), False

        start = 0
        line_len = len(line)

        if row:
            # complete the previously incomplete row
            value, i = self.extract_column(line, 0, line_len)
            row[-1] += self.opts.irs + value
            if i == -1:
                return row, True
            start = self.next_ifs(line, i+1)[1] or line_len

        while start < line_len:

            if allow_quoted and line[start] == quote:

                value, i = self.extract_column(line, start+1, line_len)

                if maxcols is not None and len(row) >= maxcols:
                    row[-1] += value
                else:
                    row.append(value)

                if i == -1:
                    # no quote; append the rest of the line, but this is incomplete
                    return row, True

                start = self.next_ifs(line, i+1)[1] or line_len + 1

            else:
                # not quoted
                s, e = self.next_ifs(line, start)
                if maxcols is not None and len(row) >= maxcols:
                    row[-1] += line[start : e]
                else:
                    row.append(line[start : s])
                if not s:
                    break
                start = max(e, s+1)

        # add last trailing blank space
        if start == len(line):
            row.append(b'')

        return row, False

    def get_rgb(self, i, step=0.647): # cycle every 17 columns
        r, g, b = colorsys.hsv_to_rgb(step * i % 1, 0.3, 1)
        return b'\x1b[38;2;%i;%i;%im' % (r*255, g*255, b*255)

    @staticmethod
    def needs_quoting(value, ofs, ors):
        # this is faster than using a [character-class]
        return b'"' in value or ors in value or ofs in value

    def format_columns(self, row, ofs, ors, quote_output):
        if quote_output:
            must_quote = False
            # if pretty output, don't allow >1 space, no matter how long the ofs is
            pretty_output = ofs == b' ' * len(ofs)
            if pretty_output:
                ofs = b'  '
                must_quote = not all(row)

            if must_quote or self.needs_quoting(b''.join(row), ofs, ors):
                row = row.copy()
                for i, col in enumerate(row):
                    if (pretty_output and not col) or self.needs_quoting(col, ofs, ors):
                        row[i] = b'"' + col.replace(b'"', b'""') + b'"'
        return row

    def format_row(self, row, padding=None):
        ofs = self.opts.ofs
        row = self.format_columns(row, ofs, self.opts.ors, self.opts.quote_output)

        if padding:
            # add padding e.g. for pretty printing
            for i, (col, p) in enumerate(zip(row, padding)):
                if p > 0:
                    row[i] += b' ' * p

        if self.opts.colour and self.opts.rainbow_columns:
            # colour each column differently

            if len(row) > len(self.__rgb_map):
                for i in range(len(self.__rgb_map), len(row)):
                    self.__rgb_map.append(self.get_rgb(i))

            parts = []
            ofs = b'\x1b[39m' + ofs
            for rgb, col in zip(self.__rgb_map, row):
                parts.append(rgb)
                parts.append(col)
                parts.append(ofs)
            # drop the last ofs and reset colour instead
            if parts:
                parts[-1] = self.RESET_COLOUR
            return b''.join(parts)

        return ofs.join(row)

    def start_outfile(self):
        if self.outfile_proc is None:
            if self.opts.page:
                cmd = ['less', '-RX']
                if self.header is not None and not self.opts.drop_header:
                    cmd.append('--header=1')
                self.outfile_proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
                self.outfile = self.outfile_proc.stdin
            else:
                self.outfile_proc = subprocess.Popen(['cat'], stdin=subprocess.PIPE) # faster to print through cat??
                self.outfile = self.outfile_proc.stdin

    def write_output(self, row, padding=None, is_header=False):
        self.start_outfile()
        self.outfile.write(self.format_row(row, padding) + self.opts.ors)

    def on_header(self, header, padding=None) -> bool:
        if not self.opts.drop_header:
            self.out_header = header
            if self.opts.numbered_columns:
                numbered_header = []
                # if the header starts with whitespace, use it up
                for i, h in enumerate(header, 1):
                    n = b'%i ' % i
                    if h.startswith(b' ' * len(n)):
                        h = h[len(n):]
                    else:
                        h = h.lstrip(b' ')
                    numbered_header.append(n + h)
                header = numbered_header
            if self.opts.colour and self.opts.ofs is not self.PRETTY_OUTPUT and header:
                header = [b''.join((self.opts.header_colour, self.opts.header_bg_colour, h, self.RESET_COLOUR, self.opts.header_bg_colour)) for h in header]
                header[-1] += self.RESET_COLOUR
            return _Base.on_row(self, header, padding, is_header=True)

    def on_row(self, row, padding=None, is_header=False) -> bool:
        if self.__numcols is None:
            self.__numcols = len(row)
            self.__rgb_map = [self.get_rgb(i) for i in range(self.__numcols)]

        self.row_count += 1
        if self.opts.ofs is self.PRETTY_OUTPUT:
            self.__gathered_rows.append(self.format_columns(row, self.PRETTY_OUTPUT_DELIM, self.opts.ors, quote_output=self.opts.quote_output))
        else:
            return self.write_output(row, padding, is_header)

    def justify(self, rows: list[bytes]):
        # get width of each column
        widths = {}
        maxwidths = {}
        for i, row in enumerate(rows):
            if isinstance(row, Separator):
                continue

            for j, col in enumerate(row):
                if not isinstance(col, bytes):
                    col = str(col).encode('utf8')
                col = _utils.remove_ansi_colour(col)

                widths.setdefault(j, {})[i] = len(col)
                maxwidths[j] = max(maxwidths.get(j, 0), len(col))

        padding = []
        for i, row in enumerate(rows):
            # don't pad the last column
            padding.append([maxwidths[j] - widths[j][i] for j in range(len(row)-1)])
        return padding

    def on_eof(self):
        # pretty print
        header_padding = None

        if self.__gathered_rows:
            padding = self.justify(self.__gathered_rows)

            self.opts.ofs = self.PRETTY_OUTPUT_DELIM
            # rows are already quoted
            self.opts.quote_output = False
            self.opts.numbered_columns = False

            # adjust width of each column and print
            for i, (p, row) in enumerate(zip(padding, self.__gathered_rows)):
                if i == 0 and self.out_header:
                    header_padding = p
                    if _Base.on_header(self, row, p):
                        break
                else:
                    if _Base.on_row(self, row, p):
                        break

        # show a trailer if too much data
        if self.out_header and (self.opts.trailer == 'always' or (_utils.stdout_is_tty() and self.opts.trailer == 'auto' and self.row_count > shutil.get_terminal_size().lines)):
            _Base.on_header(self, self.out_header, header_padding)

    def get_separator(self):
        if _utils.stdout_is_tty():
            return Separator((b'\x1b[2m' + b'-' * shutil.get_terminal_size().columns,))
        else:
            return Separator((b'---',))
