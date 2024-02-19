import argparse
from ._base import _Base

class tocsv(_Base):
    ''' convert to csv '''
    parser = argparse.ArgumentParser()
    parser.set_defaults(ofs=b',')
