import os
import re
import sys
import math
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

def resolve_tty_auto(x: str):
    return x == 'always' or (x == 'auto' and stdout_is_tty())

def as_float(value, warn=True):
    try:
        return float(value)
    except ValueError as e:
        if warn:
            print(e, file=sys.stderr)
        return math.nan
