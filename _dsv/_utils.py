import os
import re
import datetime
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

def remove_ansi_colour(value: bytes):
    if b'\x1b[' in value or b'\x1b]' in value:
        # remove colour escapes
        value = re.sub(br'\x1b\[[0-9;:]*[mK]|\x1b]8;;.*?\x1b\\', b'', value)
    return value

def to_bytes(x):
    if not isinstance(x, bytes):
        x = str(x).encode('utf8')
    return x

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

def parse_datetime(
    value,
    formats=(
        '%Y-%m-%dT%H:%M:%S.%f%z',
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S%z',
        '%Y-%m-%dT%H:%M:%S',
        '%d/%m/%y %H:%M:%S',
    ),
    date_yardstick=datetime.datetime(2000, 1, 1),
):
    if isinstance(value, (list, tuple)):
        return [parse_datetime(x) for x in value]

    if isinstance(value, datetime.datetime):
        return value

    elif isinstance(value, bytes) and value:
        value = re.sub(b'(\\.[0-9]{6})[0-9]*', b'\\1', value)
        for fmt in formats:
            try:
                return datetime.datetime.strptime(value.decode('utf8'), fmt)
            except ValueError:
                pass

    elif isinstance(value, (int, float)) and value >= date_yardstick:
        if value > date_yardstick.value() * 1000:
            # this is in milliseconds
            value /= 1000.0
        return datetime.datetime.fromtimestamp(value)

    return value
