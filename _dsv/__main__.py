import argparse
import pkgutil
import sys
import os

import _dsv
from _dsv._base import _Base
from _dsv import _utils

def interpret_c_escapes(x: str):
    return x.encode('utf8').decode('unicode_escape').encode('utf8')

def make_parser(**kwargs):
    parser = argparse.ArgumentParser(allow_abbrev=False, **kwargs)
    group = parser.add_argument_group('common options')
    header_group = group.add_mutually_exclusive_group()
    header_group.add_argument('-H', '--header', const='yes', action='store_const', help='treat first row as a header')
    header_group.add_argument('-N', '--no-header', dest='header', const='no', action='store_const', help='do not treat first row as header')
    group.add_argument('--drop-header', action='store_true', help='do not print the header')
    group.add_argument('--trailer', choices=('never', 'always', 'auto'), nargs='?', help='print a trailer')
    group.add_argument('--numbered-columns', choices=('never', 'always', 'auto'), nargs='?', help='number the columns in the header')
    group.add_argument('-d', '--ifs', type=interpret_c_escapes, help='input field separator')
    group.add_argument('--plain-ifs', action='store_true', help='treat input field separator as a literal not a regex')
    group.add_argument('-D', '--ofs', type=interpret_c_escapes, help='output field separator')
    group.add_argument('--irs', type=interpret_c_escapes, help='input row separator')
    group.add_argument('--ors', type=interpret_c_escapes, help='output row separator')
    group.add_argument('--csv', dest='ifs', action='store_const', const=b',', help='treat input as csv')
    group.add_argument('--tsv', dest='ifs', action='store_const', const=b'\t', help='treat input as tsv')
    group.add_argument('--ssv', dest='ifs', action='store_const', const=br'\s+', help='treat input as whitespace separated')
    group.add_argument('--combine-trailing-columns', action='store_true', help='if a row has more columns than the header, combine the last ones into one, useful with --ssv')
    group.add_argument('-P', '--pretty', dest='ofs', action='store_const', const=_Base.PRETTY_OUTPUT, help='prettified output')
    group.add_argument('--page', action='store_true', help='show output in a pager (less)')
    group.add_argument('--colour', '--color', choices=('never', 'always', 'auto'), nargs='?', help='enable colour')
    group.add_argument('--header-colour', type=_utils.utf8_type, default='\x1b[1;4m', help='ansi escape code for the header')
    group.add_argument('--header-bg-colour', type=_utils.utf8_type, default='\x1b[48;5;237m', help='ansi escape code for the header background')
    group.add_argument('--rainbow-columns', choices=('never', 'always', 'auto'), default='auto', nargs='?', help='enable rainbow columns')
    group.add_argument('-Q', '--no-quoting', action='store_true', help='do not handle quotes from input')
    return parser

def make_main_parser(sub_mapping={}, help=None):
    parent = make_parser(add_help=False, argument_default=argparse.SUPPRESS)
    parser = make_parser(formatter_class=argparse.RawTextHelpFormatter)
    parser.set_defaults(handler=None, quote_output=True)

    modules = [sub.name for sub in pkgutil.iter_modules(_dsv.__path__) if not sub.name.startswith('_')]
    handlers = [getattr(__import__('_dsv.'+name, fromlist=[name]), name) for name in modules]

    descr = '\n'.join(sorted(f'{h.get_name().ljust(20)}{h.__doc__ or ""}' for h in handlers))
    subparsers = parser.add_subparsers(dest='command', title='Commands', help=help, description=descr)

    for h in sorted(handlers, key=lambda h: h.get_name()):
        parents = [parent]
        if h.parser:
            parents.insert(0, h.parser)
        sub = subparsers.add_parser(h.get_name(), parents=parents, description=h.__doc__, add_help=False, help=None)
        sub.set_defaults(handler=h)
        sub_mapping[h] = sub

    return parser

def main():
    sub_mapping = {}
    parser = make_main_parser(sub_mapping, help=argparse.SUPPRESS)
    opts, extras = parser.parse_known_args()

    # print help if no input file
    if _utils.stdin_is_tty():
        sub_mapping.get(opts.handler, parser).print_help()
        return

    opts.extras = extras
    opts.parser = parser
    opts.handler = opts.handler or _Base
    opts.irs = opts.irs or b'\n'
    opts.ors = opts.ors or opts.irs

    opts.trailer = opts.trailer or 'auto'
    opts.colour = os.environ.get('NO_COLOR', '') == '' and _utils.resolve_tty_auto(opts.colour or 'auto')
    opts.numbered_columns = _utils.resolve_tty_auto(opts.numbered_columns or 'auto')
    opts.rainbow_columns = opts.colour and _utils.resolve_tty_auto(opts.rainbow_columns or 'auto')
    opts.header_colour = opts.header_colour or '\x1b[1;4m'
    opts.header_bg_colour = opts.header_bg_colour or '\x1b[48;5;237m'

    handler = opts.handler(opts)
    try:
        list(handler.process_file(sys.stdin.buffer))
    finally:
        if handler.outfile_proc:
            handler.outfile_proc.stdin.close()
            handler.outfile_proc.wait()

if __name__ == '__main__':
    try:
        main()
    except BrokenPipeError:
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
