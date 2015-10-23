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
  'title': ['movies', 'taglines', 'trivia'],
}

def imdb_parser(fn):
  def _fn(path):
    if not os.path.exists(path):
      return
    with gzip.open(path, 'rt', encoding='latin1') as f:
      yield from fn(filter(bool, (l.rstrip() for l in f)))
  return _fn

def skip_till(f, mark):
  buf = []
  for l in f:
    buf.append(l)
    if buf[-len(mark):] == mark:
      break

@imdb_parser
def parse_movies(f):

  skip_till(f, ['MOVIES LIST', '==========='])

  for l in f:
    if l.startswith('--------------'):
      break
    l = l.split('\t')
    yr = [None if x == '????' else int(x) for x in l[-1].split('-', 1)]
    yield l[0], 'movies', yr

@imdb_parser
def parse_taglines(f):

  skip_till(f, ['TAG LINES LIST', '=============='])

  id, tags = None, []
  for l in f:
    if l.startswith('--------------'):
      break
    if l.startswith('#'):
      if tags:
        yield id, 'taglines', tags
      id, tags = l[2:], []
    elif l.startswith('\t'):
      tags.append(l[1:])

  if tags:
    yield id, 'taglines', tags

@imdb_parser
def parse_trivia(f):

  skip_till(f, ['FILM TRIVIA', '==========='])

  id, trivia, lines = None, [], []
  for l in f:
    if l.startswith('#'):
      if lines:
        trivia.append(' '.join(lines))
      if trivia:
        yield id, 'trivia', trivia
      id, trivia, lines = l[2:], [], []
    elif l.startswith('- '):
      if lines:
        trivia.append(' '.join(lines))
      lines = [l[2:]]
    elif l.startswith('  '):
      lines.append(l[2:])

  if lines:
    trivia.append(' '.join(lines))
  if trivia:
    yield id, 'trivia', trivia

def mix_title(title, rtype, obj):
  if 'cat' not in title:
    pass # TODO
  if rtype == 'movies':
    title['yr'] = obj
  elif rtype == 'taglines':
    title['taglines'] = obj
  elif rtype == 'trivia':
    title['trivia'] = obj

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
  mixer = globals()['mix_' + args.kind]

  parsers = [globals()['parse_' + f.replace('-', '_')](
      os.path.join(args.dir, f + '.list.gz')
  ) for f in FILES[args.kind]]

  if not args.line:
    print('[')
    first_line = True
  
  for id, tuples in itertools.groupby(
    heapq.merge(*parsers, key=lambda x: x[0].lower()),
    key=lambda x: x[0]
  ):

    if not args.line:
      if first_line:
        first_line = False
      else:
        print(',')

    rec = {'#': id}
    for _, rtype, obj in tuples:
      mixer(rec, rtype, obj)
    json.dump(rec, sys.stdout, separators=(',', ':'), sort_keys=True)

    if args.line:
      print()

  if not args.line:
    print('\n]')

if __name__ == '__main__':
  main()
