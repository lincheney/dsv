import argparse
from .exec_ import exec_

class exec_filter(exec_):
    ''' filter rows using python '''
    name = None
    parser = argparse.ArgumentParser()
    parser.set_defaults(slurp=False)
    parser.add_argument('script')
    parser.add_argument('-q', '--quiet', action='store_true')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-I', '--ignore-errors', action='store_true')
    group.add_argument('-E', '--remove-errors', action='store_true')
    group.add_argument('--passthru', action='store_true')
    parser.add_argument('--var', default='X')

    def __init__(self, opts):
        if opts.passthru:
            opts.script = [f'''
if ({opts.script}):
    row[0] = "\x1b[1m" + row[0] + "\x1b[K"
else:
    row[0] = "\x1b[2m" + row[0]
            ''']
        else:
            opts.script = [f'if not ({opts.script}): del row']
        super().__init__(opts)
