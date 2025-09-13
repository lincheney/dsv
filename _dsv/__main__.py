import argparse
import sys
import os

from _dsv import _base, _utils

make_main_parser = _base.make_main_parser

def main():
    sub_mapping = {}
    parser = make_main_parser(sub_mapping, help=argparse.SUPPRESS)
    args = sys.argv[1:]
    opts, extras = parser.parse_known_args(args)

    # print help if no input file
    if _utils.is_tty(0):
        sub_mapping.get(opts.handler, parser).print_help()
        return

    opts.handler = opts.handler or _base._Base
    handler = opts.handler.from_opts(args, opts, extras, sub_mapping.get(opts.handler, parser))
    try:
        list(handler.process_file(sys.stdin.buffer))
    finally:
        handler.cleanup()

if __name__ == '__main__':
    try:
        main()
    except BrokenPipeError:
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
