#! /bin/env python

from argparse import ArgumentParser
from collections import defaultdict
import glob, sys, os, os.path, codecs

parser = ArgumentParser()
parser.add_argument('kb_file_in')
parser.add_argument('kb_file_out')
args = parser.parse_args()

def read_in_chunks(file_object, chunk_size=1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data

with codecs.open(args.kb_file_out, 'w', 'utf-8') as out_file:
    in_file = codecs.open(args.kb_file_in, 'r', 'utf-8')
    
    for piece in read_in_chunks(in_file):
        piece = piece.replace(u'\u0085', u' ')
        out_file.write(piece)
        
