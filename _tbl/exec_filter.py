import argparse
from .exec_ import exec_

class exec_filter(exec_):
    ''' filter rows using python '''
    name = None
    parser = argparse.ArgumentParser()
    parser.add_argument('script')
    parser.add_argument('-E', '--remove-errors', action='store_true')

    def __init__(self, opts):
        opts.script = [f'if not ({opts.script}): row = None']
        super().__init__(opts)
