import os
import re
import datetime
import sys
import math
import argparse
from functools import cache
from ._table import to_bytes

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

def remove_ansi_colour(value: bytes):
    if b'\x1b[' in value or b'\x1b]' in value:
        # remove colour escapes
        value = re.sub(br'\x1b\[[0-9;:]*[mK]|\x1b]8;;.*?\x1b\\', b'', value)
    return value

def parse_value(value):
    if isinstance(value, (list, tuple)):
        return [parse_value(x) for x in value]

    if value.isdigit():
        return int(value)

    try:
        try:
            value = value.decode('utf8')
        except UnicodeDecodeError:
            return value
        return float(value)
    except ValueError:
        return value
