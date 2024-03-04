import argparse
from ._base import _Base

class totsv(_Base):
    ''' convert to tsv '''
    parser = argparse.ArgumentParser()
    parser.set_defaults(ofs=b'\t')
