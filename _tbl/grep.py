import re
import argparse
from collections import deque
from ._column_slicer import _ColumnSlicer
from . import _utils

class grep(_ColumnSlicer):
    ''' print lines that match patterns '''

    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument('-e', '--regexp', dest='patterns', action='append')
    parent.add_argument('-F', '--fixed-strings', action='store_true')
    parent.add_argument('-w', '--word-regexp', action='store_true')
    parent.add_argument('-x', '--field-regexp', action='store_true')
    parent.add_argument('-v', '--invert-match', action='store_true')
    parent.add_argument('-s', '--case-sensitive', action='store_true')
    parent.add_argument('-m', '--max-count', type=int)
    parent.add_argument('-k', '--fields', nargs='+', default=())
    parent.add_argument('--complement', action='store_true')
    parent.set_defaults(
        replace=None,
        line_number=False,
        passthru=False,
        after_context=0,
        before_context=0,
        context=0
    )

    parser = argparse.ArgumentParser(parents=[parent])
    parser.add_argument('patterns', nargs='*', action='extend')
    parser.add_argument('--replace', type=_utils.utf8_type)
    parser.add_argument('-n', '--line-number', action='store_true')
    parser.add_argument('--passthru', action='store_true')
    parser.add_argument('-A', '--after-context', type=int, default=None)
    parser.add_argument('-B', '--before-context', type=int, default=None)
    parser.add_argument('-C', '--context', type=int, default=None)

    MATCH_COLOUR = b'\x1b[1;31m'

    def __init__(self, opts):
        super().__init__(opts)
        self.matched_count = 0
        # field overrides word
        opts.word_regexp = opts.word_regexp and not opts.field_regexp
        # no need to colour if invert and not passthru
        self.grep_colour = opts.colour and not (opts.invert_match and not opts.passthru)

        self.patterns = opts.patterns = [p.encode('utf8') for p in opts.patterns]

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
            self.patterns = [p if p == re.escape(p) else re.compile(p, re.I if opts.case_sensitive else 0) for p in self.patterns]

        if self.opts.passthru:
            self.opts.before_context = 0
            self.opts.after_context = 0
        self.before = deque((), (self.opts.context if self.opts.before_context is None else self.opts.before_context) or 0)
        self.after = (self.opts.context if self.opts.after_context is None else self.opts.after_context)
        self.last_matched = None
        self.count = 0

    def on_header(self, header):
        if self.opts.line_number:
            header = [b'n'] + header
        super().on_header(header)

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
            allowed_fields = set(self.slice(list(range(len(row))), self.opts.complement))

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
                            return row

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
                while start < len(col):

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
                        if parts:
                            parts.append(row[i][start:])
                        break

                    if not self.grep_colour and self.opts.replace is None:
                        # quit early if we don't need to add colour
                        return row

                    # prefix
                    parts.append(row[i][start : best[0]])
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

            if parts:
                row[i] = b''.join(parts)
                matched = True

        matched = matched ^ self.opts.invert_match
        return matched and row

    def on_row(self, row):
        self.count += 1

        reached_maxcount = self.opts.max_count and self.matched_count >= self.opts.max_count
        is_after = self.after and self.last_matched is not None and self.last_matched + self.after >= self.count
        matched = self.grep(row)

        if not matched and not is_after and not self.opts.passthru:
            # this line might be a before
            self.before.append(row)
            return

        if matched and not reached_maxcount:
            # matched this line
            self.last_matched = self.count
            if self.opts.max_count:
                self.matched_count += 1

            # print the lines before
            for i, r in enumerate(self.before, self.count - len(self.before)):
                if self.opts.line_number:
                    r.insert(0, b'%i' % i)
                super().on_row(r)
            self.before.clear()

        if self.opts.line_number:
            row.insert(0, b'%i' % self.count)
        super().on_row(row)

        # quit if reached max count
        if reached_maxcount and not is_after:
            return True
