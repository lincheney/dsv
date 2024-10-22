import argparse
from .exec_ import exec_

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
        if opts.passthru:
            opts.script = [f'''
if ({opts.script}):
    {opts.var}[0] = "\x1b[1m" + {opts.var}[0] + "\x1b[K"
else:
    {opts.var}[0] = "\x1b[2m" + {opts.var}[0]
            ''']
        else:
            opts.script = [f'if not ({opts.script}): del {opts.var}']
        super().__init__(opts)
