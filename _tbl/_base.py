import re
import sys
import shutil
import subprocess
from . import _utils

def get_rgb(hue, sat, val):
    red = 1 + sat * (1 - abs((hue + 180) % 360 - 180) / 60)
    green = 1 + sat * (1 - abs((hue + 60) % 360 - 180) / 60)
    blue = 1 + sat * (1 - abs((hue + 300) % 360 - 180) / 60)
    red = val * max(1-sat, min(red, 1))
    green = val * max(1-sat, min(green, 1))
    blue = val * max(1-sat, min(blue, 1))
    return int(red), int(green), int(blue)

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

    def __init__(self, opts):
        self.opts = opts

        if not self.outfile:
            if not _utils.stdout_is_tty():
                self.outfile = sys.stdout.buffer
            elif self.opts.page:
                cmd = ['less', '-RX']
                if not opts.no_header:
                    cmd.append('--header=1')
                self.outfile_proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
                self.outfile = self.outfile_proc.stdin
            else:
                self.outfile_proc = subprocess.Popen(['cat'], stdin=subprocess.PIPE) # faster to print through cat??
                self.outfile = self.outfile_proc.stdin

        self.numcols = None
        self.rgb_map = []
        if self.opts.extras:
            self.opts.parser.error('unrecognized arguments: ' + " ".join(self.opts.extras))
        self.gathered_rows = []

    @classmethod
    def get_name(cls):
        return (cls.name or cls.__name__).replace('_', '-')

    @classmethod
    def guess_delimiter(cls, line, default):
        good_delims = (b',', b'\t')
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
        opts = self.opts
        if opts.ifs:
            if re.escape(opts.ifs) != opts.ifs and not opts.plain_ifs:
                opts.ifs = re.compile(opts.ifs)

        else:
            # guess delimiter if not specified
            opts.ifs = self.guess_delimiter(line, b'\t')
            if opts.ifs == self.SPACE or opts.ifs == self.PPRINT:
                opts.combine_trailing_columns = True
                opts.no_quoting = True

        if not opts.ofs:
            if (opts.ifs == self.SPACE or opts.ifs == self.PPRINT or opts.ifs.isspace()) and opts.colour:
                opts.ofs = self.PRETTY_OUTPUT
            elif isinstance(opts.ifs, bytes):
                opts.ofs = opts.ifs
            else:
                opts.ofs = b'\t'

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

    def iter_lines(self, file, sep, chunk=8192):
        rest = b''
        while buf := file.read1(chunk):
            rest += buf
            *lines, rest = rest.split(sep)
            yield from lines
        if rest:
            yield rest

    def process_file(self, file, do_yield=False):
        row = []
        first = True
        if self.opts.irs == b'\n':
            lines = file
        else:
            lines = self.iter_lines(file, self.opts.irs)

        for line in lines:
            line = line.removesuffix(self.opts.irs)

            if first:
                first = False
                self.determine_delimiters(line)

            row, incomplete = self.parse_line(line, row)
            if not incomplete:
                is_header = self.header is None and not self.opts.no_header
                if is_header:
                    self.header = row
                    if self.on_header(self.header):
                        break

                elif self.on_row(row):
                    break

                if do_yield:
                    yield (row, is_header)

                row = []

        self.on_eof()

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

                start = self.next_ifs(line, i+1)[1] or line_len

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

        return row, False

    def get_rgb(self, i):
        return b'\x1b[38;2;%i;%i;%im' % get_rgb(180 * i * (0.7 + 1 / self.numcols), 0.3, 255)

    @staticmethod
    def needs_quoting(value, ofs, ors):
        # this is faster than using a [character-class]
        return b'"' in value or ors in value or ofs in value

    def format_columns(self, row, ofs, ors, quote_output):
        if quote_output and self.needs_quoting(b''.join(row), ofs, ors):
            row = row.copy()
            for i, col in enumerate(row):
                if self.needs_quoting(col, ofs, ors):
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

            if len(row) > len(self.rgb_map):
                for i in range(len(self.rgb_map), len(row)):
                    self.rgb_map.append(self.get_rgb(i))

            parts = []
            ofs = b'\x1b[39m' + ofs
            for rgb, col in zip(self.rgb_map, row):
                parts.append(rgb)
                parts.append(col)
                parts.append(ofs)
            # drop the last ofs and reset colour instead
            if parts:
                parts[-1] = self.RESET_COLOUR
            return b''.join(parts)

        return ofs.join(row)

    def print_row(self, row, padding=None):
        self.outfile.write(self.format_row(row, padding) + self.opts.ors)

    def on_header(self, header, padding=None):
        if not self.opts.drop_header:
            self.out_header = header
            if self.opts.numbered_columns:
                header = [b'%i %s' % x for x in enumerate(header, 1)]
            if self.opts.colour and self.opts.ofs is not self.PRETTY_OUTPUT and header:
                header = [b''.join((self.opts.header_colour, self.opts.header_bg_colour, h, self.RESET_COLOUR, self.opts.header_bg_colour)) for h in header]
                header[-1] += self.RESET_COLOUR
            return _Base.on_row(self, header, padding)

    def on_row(self, row, padding=None):
        if self.numcols is None:
            self.numcols = len(row)
            self.rgb_map = [self.get_rgb(i) for i in range(self.numcols)]

        self.row_count += 1
        if self.opts.ofs is self.PRETTY_OUTPUT:
            self.gathered_rows.append(self.format_columns(row, self.PRETTY_OUTPUT_DELIM, self.opts.ors, quote_output=self.opts.quote_output))
        else:
            self.print_row(row, padding)

    def justify(self, rows: list[bytes]):
        # get width of each column
        widths = {}
        maxwidths = {}
        for i, row in enumerate(rows):
            for j, col in enumerate(row):
                if not isinstance(col, bytes):
                    col = str(col).encode('utf8')

                if b'\x1b[' in col:
                    # remove colour escapes
                    col = re.sub(br'\x1b\[[0-9;:]*m', b'', col)

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

        if self.gathered_rows:
            padding = self.justify(self.gathered_rows)

            self.opts.ofs = self.PRETTY_OUTPUT_DELIM
            # rows are already quoted
            self.opts.quote_output = False
            self.opts.numbered_columns = False

            # adjust width of each column and print
            for i, (p, row) in enumerate(zip(padding, self.gathered_rows)):
                if i == 0 and self.out_header:
                    header_padding = p
                    _Base.on_header(self, row, p)
                else:
                    _Base.on_row(self, row, p)

        # show a trailer if too much data
        if self.out_header and (self.opts.trailer == 'always' or (_utils.stdout_is_tty() and self.opts.trailer == 'auto' and self.row_count > shutil.get_terminal_size().lines)):
            _Base.on_header(self, self.out_header, header_padding)
