import os
import re
import argparse
from functools import cache

@cache
def stdin_is_tty():
    return os.isatty(0)

@cache
def stdout_is_tty():
    return os.isatty(1)

def utf8_type(x):
    return x.encode('utf8')

def regex_arg_type(regex):
    def wrapped(value):
        if match := re.fullmatch(regex, value):
            return match
        raise argparse.ArgumentTypeError(f'{value} does not match: {regex}')
    return wrapped
