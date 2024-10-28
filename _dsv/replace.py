import argparse
from .grep import grep
from . import _utils

class replace(grep):
    ''' replace text '''
    parser = argparse.ArgumentParser(parents=[grep.parent])
    parser.add_argument('patterns', action='append', help='pattern to search for')
    parser.add_argument('replace', type=_utils.utf8_type, help='replaces every match with the given text')
    parser.set_defaults(passthru=True)
