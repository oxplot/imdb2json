#!/usr/bin/env python
#
# imdb2json.py - Convert IMDB list files to JSON
# Copyright (C) 2015 Mansour Behabadi <mansour@oxplot.com>

import argparse
import gzip
import heapq
import itertools
import json
import os
import sys

FILES = {
  'name': ['actors', 'biographies'],
  'title': ['movies', 'taglines'],
}

def imdb_parser(fn):
  def _fn(path):
    if not os.path.exists(path):
      return ()
    with gzip.open(path, 'rt', encoding='latin1') as f:
      return fn((l.rstrip() for l in f))
  return _fn

@imdb_parser
def parse_movies(f):
  yield (1,2,3)
  yield (4,9,6)

@imdb_parser
def parse_taglines(f):
  yield (1,2,4)
  yield (4,5,6)

def mix_title(title, rtype, obj):
  pass

def mix_name(name, rtype, obj):
  pass

def main():
  "Run main program."

  parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description='Convert IDMB list files to JSON'
  )
  parser.add_argument(
    '-l', '--line',
    action="store_true",
    help='put each title/name JSON object on its own independent line'
         ' without enclosing everying in one big JSON list'
  )
  parser.add_argument(
    '-d', '--dir',
    default='.',
    help='path to where the .list.gz files are - default is .'
  )
  parser.add_argument(
    'kind',
    choices=['title', 'name'],
    help='choose between movies/people output'
  )

  args = parser.parse_args()

  parsers = [globals()['parse_' + f.replace('-', '_')](
      os.path.join(args.dir, f + '.list.gz')
  ) for f in FILES[args.kind]]

  mixer = globals()['mix_' + args.kind]

  for id, tuples in itertools.groupby(
    heapq.merge(*parsers), key=lambda x: x[0]
  ):
    rec = {'#': id}
    for _, rtype, obj in tuples:
      mixer(rec, rtype, obj)
    json.dump(rec, sys.stdout, separators=(',', ':'), sort_keys=True)
    sys.stdout.write('\n')

if __name__ == '__main__':
  main()
