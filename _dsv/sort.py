import shlex
import argparse
import subprocess
from ._column_slicer import _ColumnSlicer

class sort(_ColumnSlicer):
    ''' sort the rows '''
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('fields', nargs='*')
    parser.add_argument('--help', action='help')
    parser.add_argument('-x', '--complement', action='store_true')
    parser.add_argument('-b', '--ignore-leading-blanks', action='append_const', dest='sort_flags', const='-b')
    parser.add_argument('--dictionary-order', action='append_const', dest='sort_flags', const='-d')
    parser.add_argument('-f', '--ignore-case', action='append_const', dest='sort_flags', const='-f')
    parser.add_argument('-g', '--general-numeric-sort', action='append_const', dest='sort_flags', const='-g')
    parser.add_argument('-i', '--ignore-nonprinting', action='append_const', dest='sort_flags', const='-i')
    parser.add_argument('-M', '--month-sort', action='append_const', dest='sort_flags', const='-M')
    parser.add_argument('-h', '--human-numeric-sort', action='append_const', dest='sort_flags', const='-h')
    parser.add_argument('-n', '--numeric-sort', action='append_const', dest='sort_flags', const='-n')
    parser.add_argument('-R', '--random-sort', action='append_const', dest='sort_flags', const='-R')
    parser.add_argument('-r', '--reverse', action='append_const', dest='sort_flags', const='-r')
    parser.add_argument('-V', '--version-sort', action='append_const', dest='sort_flags', const='-V')

    def __init__(self, opts):
        super().__init__(opts)
        self.rows = []
        self.header_map = None

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
