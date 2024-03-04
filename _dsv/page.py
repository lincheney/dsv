import argparse
from .pretty import pretty

class page(pretty):
    ''' view the file in a pager '''
    parser = argparse.ArgumentParser()
    parser.set_defaults(page=True)
