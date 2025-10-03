import os
import re
import datetime
import sys
import math
import argparse
from functools import cache
from ._table import to_bytes, parse_datetime

@cache
def is_tty(fd):
    return os.isatty(fd)

@cache
def getpid():
    return os.getpid()

def utf8_type(x):
    return x.encode('utf8')

def regex_arg_type(regex):
    def wrapped(value):
        if match := re.fullmatch(regex, value):
            return match
        raise argparse.ArgumentTypeError(f'{value} does not match: {regex}')
    return wrapped

def resolve_tty_auto(x: str, fd=1, checker=is_tty):
    return x == 'always' or (x == 'auto' and checker(fd))

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

class shtab:
    def __getattr__(self, key):
        if os.environ.get('ENABLE_SHTAB') == '1':
            import shtab
            return getattr(shtab, key)
shtab = shtab()
