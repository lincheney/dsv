import shutil
import argparse
import subprocess
from ._column_slicer import _ColumnSlicer

class pipe(_ColumnSlicer):
    ''' pipe rows through a processs '''
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-k', '--fields', action='append', default=[])
    parser.add_argument('-x', '--complement', action='store_true')
    parser.add_argument('command', nargs='*')

    proc = None
    proc_stdout = None
    def start_process(self):
        if not self.proc:
            if stdbuf := shutil.which('stdbuf'):
                self.opts.command = [stdbuf, '-oL', '--'] + self.opts.command

            self.proc = subprocess.Popen(self.opts.command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            self.proc_stdout = self.iter_lines(self.proc.stdout, self.opts.ors)
        return self.proc

    def on_row(self, row, ofs=b'\n'):
        input = self.slice(row, self.opts.complement)
        input = ofs.join(self.format_columns(input, ofs, self.opts.ors, self.opts.quote_output))

        proc = self.start_process()
        proc.stdin.write(input + self.opts.ors)
        proc.stdin.flush()

        stdout = []
        incomplete = True
        while incomplete and (line := next(self.proc_stdout, None)) and line is not None:
            stdout, incomplete = self.parse_line(line.removesuffix(self.opts.ors), stdout)

        # write the stdout back into the original row
        indices = self.slice(list(range(len(row))), self.opts.complement)
        for k, v in zip(indices, stdout):
            row[k] = v

        super().on_row(row)

    def on_eof(self):
        super().on_eof()
        if self.proc:
            self.proc.stdin.close()
            self.proc.wait()
