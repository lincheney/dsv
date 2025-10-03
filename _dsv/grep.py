import re
import argparse
from collections import deque
from ._column_slicer import _ColumnSlicer
from . import _utils
from ._shtab import shtab

class grep(_ColumnSlicer):
    ''' print lines that match patterns '''

    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument('-e', '--regexp', dest='patterns', action='append', help='pattern to search for')
    parent.add_argument('-F', '--fixed-strings', action='store_true', help='treat all patterns as literals instead of as regular expressions')
    parent.add_argument('-f', '--file', action='append', help='obtain patterns from FILE, one per line').complete = shtab.FILE
    parent.add_argument('-w', '--word-regexp', action='store_true', help='select only those matches surrounded by word boundaries')
    parent.add_argument('-x', '--field-regexp', action='store_true', help='select only those matches that exactly match the column')
    parent.add_argument('-s', '--case-sensitive', action='store_true', help='search case sensitively')
    parent.add_argument('-m', '--max-count', type=int, default=float('inf'), metavar='NUM', help='show only the first NUM matching rows')
    parent.add_argument('-o', '--only-matching', action='store_true', help='print only the matched (non-empty) parts of a matching column')
    parent.add_argument('-k', '--fields', action='append', default=[], help='search only on these fields')
    parent.add_argument('-r', '--regex', action='store_true', help='treat fields as regexes')
    parent.add_argument('--complement', action='store_true', help='exclude, rather than include, field names')
    parent.set_defaults(
        replace=None,
        line_number=False,
        passthru=False,
        after_context=0,
        before_context=0,
        context=0,
        count=False,
        invert_match=False,
    )

    parser = argparse.ArgumentParser(parents=[parent])
    parser.add_argument('patterns', nargs='*', action='extend', help='pattern to search for')
    parser.add_argument('--replace', type=_utils.utf8_type, help='replaces every match with the given text')
    parser.add_argument('-n', '--line-number', action='store_true', help='show line numbers')
    parser.add_argument('--passthru', action='store_true', help='print both matching and non-matching lines')
    parser.add_argument('-A', '--after-context', type=int, default=None, metavar='NUM', help='show NUM lines after each match')
    parser.add_argument('-B', '--before-context', type=int, default=None, metavar='NUM', help='show NUM lines before each match')
    parser.add_argument('-C', '--context', type=int, default=None, metavar='NUM', help='show NUM lines before and after each match')
    parser.add_argument('-c', '--count', action='store_true', help='print only the count of matching rows')
    parser.add_argument('-v', '--invert-match', action='store_true', help='select non-matching lines')

    MATCH_COLOUR = b'\x1b[1;31m'

    def __init__(self, opts):
        super().__init__(opts)
        if not opts.patterns and not opts.file:
            self.parser.error('error: the following arguments are required: patterns')

        self.matched_count = 0
        # field overrides word
        opts.word_regexp = opts.word_regexp and not opts.field_regexp
        # no need to colour if invert and not passthru
        self.grep_colour = opts.colour and not (opts.invert_match and not opts.passthru)

        self.patterns = opts.patterns = [p.encode('utf8') for p in opts.patterns if p is not None]
        for file in opts.file or ():
            with open(file, 'rb') as file:
                self.patterns.extend(line.rstrip(b'\r\n') for line in file)

        # case sensitive if pattern is not lowercase
        opts.case_sensitive = opts.case_sensitive or any(p != p.lower() for p in opts.patterns)

        if not opts.fixed_strings:
            # validate each pattern
            for p in self.patterns:
                re.compile(p)

        if opts.word_regexp:
            # convert to regex
            if opts.fixed_strings:
                self.patterns = [re.escape(p) for p in self.patterns]
                opts.fixed_strings = False
            self.patterns = [rb'\b' + p + rb'\b' for p in self.patterns]

        # compile the patterns
        if not opts.fixed_strings:
            # but if it is just a fixed string leave as is
            self.patterns = [p if p == re.escape(p) else re.compile(p, 0 if opts.case_sensitive else re.IGNORECASE) for p in self.patterns]

        if self.opts.passthru:
            self.opts.before_context = 0
            self.opts.after_context = 0
        self.before = deque((), (self.opts.context if self.opts.before_context is None else self.opts.before_context) or 0)
        self.after = (self.opts.context if self.opts.after_context is None else self.opts.after_context) or 0
        self.last_matched = None
        self.row_num = 0

        if self.opts.count:
            # don't print any rows and disable the pretty formatting
            self.write_output = lambda *a, **kwa: False
            self.opts.ofs = ','

    def on_header(self, header):
        if self.opts.line_number:
            header = [b'n'] + header
        return super().on_header(header)

    def on_eof(self):
        super().on_eof()
        if self.opts.count:
            print(self.matched_count)

    def do_replace(self, match: re.Match, text: bytes):
        if match:
            return match.expand(self.opts.replace)
        elif b'\\' not in self.opts.replace:
            return self.opts.replace
        else:
            # replacement may have groups, so make a fake match
            return re.fullmatch(b'.*', text).expand(self.opts.replace)

    def grep(self, row):
        matched = False

        allowed_fields = None
        if self.opts.fields:
            allowed_fields = set(self.slice(list(range(len(row))), self.opts.complement, False))

        for i, col in enumerate(row):
            if allowed_fields is not None and i not in allowed_fields:
                continue

            parts = []

            if not self.opts.case_sensitive:
                col = col.lower()

            if self.opts.field_regexp:
                for pat in self.patterns:
                    # do a direct string comparison
                    match = None
                    if pat == col or (isinstance(pat, re.Pattern) and (match := pat.fullmatch(row[i]))):

                        if not self.grep_colour and self.opts.replace is None:
                            # quit early if we don't need to add colour
                            return None if self.opts.invert_match else row

                        if self.opts.replace is not None:
                            row[i] = self.do_replace(match, row[i])

                        if self.grep_colour:
                            parts.append(self.MATCH_COLOUR)
                            parts.append(row[i])
                            parts.append(self.RESET_COLOUR)

                        # we've matched the whole string, no need to check other patterns
                        break

            else:
                start = 0
                # always check at least once, in case both the column and pattern are empty
                while True:

                    # find the pattern that matches first
                    best = None
                    for pat in self.patterns:
                        if isinstance(pat, bytes):
                            match = col.find(pat, start)
                            if match != -1 and (not best or match < best[0]):
                                best = [match, match + len(pat), None]

                        elif match := pat.search(row[i], pos=start):
                            if not best or match.start() < best[0]:
                                best = [*match.span(), match]

                    if not best:
                        # no matches
                        if parts and not self.opts.only_matching:
                            parts.append(row[i][start:])
                        break

                    if not self.grep_colour and self.opts.replace is None and not self.opts.only_matching:
                        # quit early if we don't need to add colour
                        return None if self.opts.invert_match else row

                    # prefix
                    if not self.opts.only_matching:
                        parts.append(row[i][start : best[0]])

                    if best[0] == best[1]:
                        # empty match
                        if self.opts.replace is not None:
                            # start colour
                            if self.grep_colour:
                                parts.append(self.MATCH_COLOUR)
                            parts.append(self.do_replace(best[2], b''))
                            # end colour
                            if self.grep_colour:
                                parts.append(self.RESET_COLOUR)
                        parts.append(row[i][best[0] : best[1] + 1])
                        start = best[1] + 1

                    else:
                        # start colour
                        if self.grep_colour:
                            parts.append(self.MATCH_COLOUR)
                        # matched text / replacement
                        parts.append(row[i][best[0] : best[1]])
                        if self.opts.replace is not None:
                            parts[-1] = self.do_replace(best[2], parts[-1])
                        # end colour
                        if self.grep_colour:
                            parts.append(self.RESET_COLOUR)
                        start = best[1]

                    if start >= len(col):
                        break

            if parts or self.opts.only_matching:
                row[i] = b''.join(parts)
                matched = True

        matched = matched ^ self.opts.invert_match
        return matched and row

    def on_row(self, row):
        self.row_num += 1

        if matched := self.grep(row):
            # matched this line
            if self.matched_count < self.opts.max_count:
                self.last_matched = self.row_num
            self.matched_count += 1

            # print the lines before
            for i, r in enumerate(self.before, self.row_num - len(self.before)):
                if self.opts.line_number:
                    r.insert(0, b'%i' % i)
                if super().on_row(r):
                    return True
            self.before.clear()

        # print this line if matched or it is in after or we are doing passthru
        if matched or self.opts.passthru or (self.last_matched is not None and self.last_matched + self.after >= self.row_num):
            if self.opts.line_number:
                row.insert(0, b'%i' % self.row_num)
            if super().on_row(row):
                return True

        else:
            # this line might be a before
            self.before.append(row)
            return

        # quit if reached max count
        if self.matched_count >= self.opts.max_count and self.last_matched + self.after <= self.row_num:
            return True

    def on_eof(self):
        if not self.matched_count:
            self.exit_code = 1
        super().on_eof()
