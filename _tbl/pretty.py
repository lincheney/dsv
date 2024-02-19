import argparse
from ._base import _Base

class pretty(_Base):
    ''' pretty prints the file '''
    parser = argparse.ArgumentParser()
    parser.set_defaults(ofs=_Base.PRETTY_OUTPUT)
