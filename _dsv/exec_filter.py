import argparse
from .exec_ import exec_, to_bytes

class exec_filter(exec_):
    ''' filter rows using python '''
    name = None
    parser = argparse.ArgumentParser(parents=[exec_.parent])
    parser.set_defaults(slurp=False)
    parser.add_argument('script')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-I', '--ignore-errors', action='store_true')
    group.add_argument('-E', '--remove-errors', action='store_true')
    group.add_argument('--passthru', action='store_true')

    def __init__(self, opts):
        opts.script = [f'{opts.var} = (({opts.script}), {opts.var})']
        super().__init__(opts)

    def handle_exec_result(self, vars):
        success, result = vars[self.opts.var]

        if self.opts.passthru:
            headers = result.__headers__
            result = [to_bytes(v) for v in result[0]]
            if success:
                result[0] = b'\x1b[1m' + result[0] + b'\x1b[K'
            else:
                result[0] = b'\x1b[2m' + result[0]
            vars[self.opts.var] = dict(zip(headers, result))

        else:
            if success:
                vars[self.opts.var] = result
            else:
                del vars[self.opts.var]

        super().handle_exec_result(vars)
