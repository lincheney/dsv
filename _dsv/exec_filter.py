import argparse
from . import _utils
from .exec_ import exec_, Vec

class exec_filter(exec_):
    ''' filter rows using python '''
    name = None
    parser = argparse.ArgumentParser(parents=[exec_.parent])
    parser.add_argument('script', nargs='+', help='python statements to run')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-I', '--ignore-errors', action='store_true', help='do not abort on python errors')
    group.add_argument('--passthru', action='store_true', help='print both matching and non-matching lines')

    def __init__(self, opts):
        opts.slurp = False
        super().__init__(opts, eval_only=True)
        if self.opts.ignore_errors:
            self.opts.remove_errors = True

    def handle_exec_result(self, result, vars, table):
        if isinstance(result, Vec):
            result = all(result)

        if self.opts.passthru:
            if self.opts.colour:
                for row in table:
                    row[:] = (b'\x1b[1m' if result else b'\x1b[2m') + row.map(_utils.to_bytes)
            result = table
        else:
            result = table if result else None

        super().handle_exec_result(result, vars, table)
