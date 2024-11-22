import shlex
import argparse
import subprocess
from ._column_slicer import _ColumnSlicer

class sort(_ColumnSlicer):
    ''' sort the rows '''
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('fields', nargs='*', help='sort based only on these fields')
    parser.add_argument('-k', '--fields', metavar='fields', type=lambda x: x.split(','), dest='old_style_fields', help='search only these fields')
    parser.add_argument('--help', action='help', help='show this help message and exit')
    parser.add_argument('--regex', action='store_true', help='treat fields as regexes')
    parser.add_argument('-x', '--complement', action='store_true', help='exclude, rather than include, field names')
    parser.add_argument('-b', '--ignore-leading-blanks', action='append_const', dest='sort_flags', const='-b', help='ignore leading blanks')
    parser.add_argument('--dictionary-order', action='append_const', dest='sort_flags', const='-d', help='consider only blanks and alphanumeric characters')
    parser.add_argument('-f', '--ignore-case', action='append_const', dest='sort_flags', const='-f', help='fold lower case to upper case characters')
    parser.add_argument('-g', '--general-numeric-sort', action='append_const', dest='sort_flags', const='-g', help='compare according to general numerical value')
    parser.add_argument('-i', '--ignore-nonprinting', action='append_const', dest='sort_flags', const='-i', help='consider only printable characters')
    parser.add_argument('-M', '--month-sort', action='append_const', dest='sort_flags', const='-M', help='sort by month name e.g. JAN < DEC')
    parser.add_argument('-h', '--human-numeric-sort', action='append_const', dest='sort_flags', const='-h', help='compare human readable numbers e.g. 4K < 2G')
    parser.add_argument('-n', '--numeric-sort', action='append_const', dest='sort_flags', const='-n', help='compare according to string numerical value')
    parser.add_argument('-R', '--random-sort', action='append_const', dest='sort_flags', const='-R', help='shuffle, but group identical keys')
    parser.add_argument('-r', '--reverse', action='append_const', dest='sort_flags', const='-r', help='sort in reverse order')
    parser.add_argument('-V', '--version-sort', action='append_const', dest='sort_flags', const='-V', help='natural sort of version numbers within text')

    def __init__(self, opts):
        opts.fields += opts.old_style_fields or ()
        super().__init__(opts)
        self.rows = []

    sorter = None
    def start_sorter(self):
        if not self.sorter:
            cmd = ['sort', '-z', '-k2'] + (self.opts.sort_flags or [])
            cmd = ' '.join(map(shlex.quote, cmd)) + ' | cut -f1 -z | tr \\\\0 \\\\n '
            self.sorter = subprocess.Popen(['sh', '-c', cmd], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        return self.sorter

    def on_row(self, row, ofs=b'\t', ors=b'\x00'):
        key = self.slice(row, self.opts.complement)
        key = ofs.join(self.format_columns(key, ofs, ors, self.opts.quote_output))
        # add row index as first column
        key = b'%i\t%s%s' % (len(self.rows), key, ors)
        self.start_sorter().stdin.write(key)
        self.rows.append(row)

    def on_eof(self):
        # get the sorted values
        proc = self.start_sorter()
        proc.stdin.close()

        for line in proc.stdout:
            i = int(line)
            super().on_row(self.rows[i])

        super().on_eof()
        proc.wait()
