#!/usr/bin/env python
#
# imdb2json.py - Convert IMDB list files to JSON
# Copyright (C) 2015 Mansour Behabadi <mansour@oxplot.com>

import argparse
import collections
import gzip
import heapq
import itertools
import json
import os
import re
import sys

FILES = {
  'name': ['actors', 'biographies'],
  'title': ['movies', 'taglines', 'trivia', 'running-times'],
}

def imdb_parser(fn):
  def _fn(path):
    if not os.path.exists(path):
      return
    with gzip.open(path, 'rt', encoding='latin1') as f:
      yield from fn(filter(bool, (l.rstrip() for l in f)))
  return _fn

def skip_till(f, mark):
  deq = collections.deque(maxlen=len(mark))
  for l in f:
    deq.append(l)
    if list(deq) == mark:
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
      lines = [l[2:].strip()]
    elif l.startswith('  '):
      lines.append(l[2:].strip())

  if lines:
    trivia.append(' '.join(lines))
  if trivia:
    yield id, 'trivia', trivia

@imdb_parser
def parse_running_times(f):

  skip_till(f, ['RUNNING TIMES LIST', '=================='])

  rt_pat = re.compile(r'''
    ^(?:(?P<country>[^:]+):)? # optional country
    \s*(?:
      (?P<H>\d+):(?P<M>\d+):(?P<S>\d+) # H,M,S
    |
      (?P<M2>\d+)
      (?:
        \s*(?:[:,']|m|min[s.]?|minutes)\s*
        (?:(?P<S2>\d+)\s*(?:"|''|s|sec[.s]?|seconds)?)?
      )? # M,S
    |
      (?P<M3>\d+)\.(?P<S3>\d+) # M.S
    )(?:\s*[x*].*|\s*\d+\s*episodes)?$
    ''', re.X | re.I
  )

  for l in f:
    if l.startswith('--------------'):
      break
    l = [i for i in l.split('\t') if i]

    rt = rt_pat.match(l[1])
    if rt:
      # FIXME even if we match country fine, we can still fail the
      #       complete match and hence lose the country info too
      country = (rt.group('country') or '').strip() or None
      if rt.group('M2'):
        secs = int(rt.group('M2')) * 60
        if rt.group('S2'):
          secs += int(rt.group('S2'))
      elif rt.group('H'):
        secs = int(rt.group('H')) * 3600 \
          + int(rt.group('M')) * 60 + int(rt.group('S'))
      elif rt.group('M3'):
        secs = int(rt.group('M3')) * 60
        secs += 30 if rt.group('S3') == '5' else int(rt.group('S3'))
    else:
      secs = None
      print('bad-running-time:', l, file=sys.stderr)

    note = l[2].strip() if len(l) > 2 else ''
    if note[:1] == '(':
      note = note[1:]
    if note[-1:] == ')':
      note = note[:-1]
    note = note.strip() or None

    obj = {}
    for k, v in zip(['secs', 'country', 'note'], [secs, country, note]):
      if v is not None:
        obj[k] = v

    yield l[0], 'running-times', obj

def mix_title(title, rtype, obj):
  if 'cat' not in title:
    pass # TODO
  if rtype == 'movies':
    title['yr'] = obj
  elif rtype == 'taglines':
    title['taglines'] = obj
  elif rtype == 'trivia':
    title['trivia'] = obj
  elif rtype == 'running-times':
    runtimes = title.get('runtimes')
    if runtimes:
      runtimes.append(obj)
    else:
      title['runtimes'] = [obj]

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
