import argparse
from .grep import grep
from . import _utils

class replace(grep):
    ''' replace text '''
    parser = argparse.ArgumentParser(parents=[grep.parent])
    parser.add_argument('patterns', action='append')
    parser.add_argument('replace', type=_utils.utf8_type)
    parser.set_defaults(passthru=True)
