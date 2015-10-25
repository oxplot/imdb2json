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
  'name': [
    'actors', 'actresses', 'cinematographers', 'composers', 'directors',
    'costume-designers', 'editors', 'producers', 'writers'
  ],
  'title': [
    'movies', 'taglines', 'trivia', 'running-times', 'keywords',
    'genres', 'technical'
  ],
}

def imdb_parser(fn):
  def _fn(path):
    if not os.path.exists(path):
      return
    with gzip.open(path, 'rt', encoding='latin1') as f:
      yield from fn(filter(bool, (l.rstrip() for l in f)))
  return _fn

def skip_till(f, window, pat):
  pat = re.compile(pat)
  deq = collections.deque(maxlen=window)
  for l in f:
    deq.append(l)
    if pat.search('\n'.join(deq)):
      break

@imdb_parser
def parse_movies(f):

  skip_till(f, 2, r'^MOVIES LIST\n={8}')

  for l in f:
    if l.startswith('--------------'):
      break
    l = l.split('\t')
    yr = [None if x == '????' else int(x) for x in l[-1].split('-', 1)]
    yield l[0], 'movies', yr

@imdb_parser
def parse_taglines(f):

  skip_till(f, 2, r'^TAG LINES LIST\n={8}')

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

  skip_till(f, 2, r'^FILM TRIVIA\n={8}')

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

  skip_till(f, 2, r'^RUNNING TIMES LIST\n={8}')

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

@imdb_parser
def parse_keywords(f):

  skip_till(f, 2, r'^8: THE KEYWORDS LIST\n={8}')

  for l in f:
    l = l.split('\t')
    yield l[0], 'keywords', l[-1].strip()

@imdb_parser
def parse_genres(f):

  skip_till(f, 2, r'^8: THE GENRES LIST\n={8}')

  for l in f:
    l = l.split('\t')
    yield l[0], 'genres', l[-1].strip()

@imdb_parser
def parse_technical(f):

  skip_till(f, 2, r'^TECHNICAL LIST\n={8}')

  for l in f:
    l = [l for l in l.split('\t') if l]
    typ, val = l[1].split(':', 1)
    typ = {
      'RAT': 'ratios', 'CAM': 'cameras', 'MET': 'lengths',
      'PCS': 'processes', 'LAB': 'lab', 'OFM': 'negatives',
      'PFM': 'prints'
    }[typ]
    val = {'name': val}
    if len(l) > 2:
      val['note'] = l[2]
    yield l[0], 'technical', (typ, val)

@imdb_parser
def parse_actresses(f):
  yield from parse_people(f, 'actresses', 'actor')

@imdb_parser
def parse_actors(f):
  yield from parse_people(f, 'actors', 'actor')

@imdb_parser
def parse_cinematographers(f):
  yield from parse_people(f, 'cinematographers', 'cinematographer')

@imdb_parser
def parse_composers(f):
  yield from parse_people(f, 'composers', 'composer')

@imdb_parser
def parse_directors(f):
  yield from parse_people(f, 'directors', 'director')

@imdb_parser
def parse_costume_designers(f):
  yield from parse_people(f, 'costume-designers', 'costume-designer')

@imdb_parser
def parse_editors(f):
  yield from parse_people(f, 'editors', 'editor')

@imdb_parser
def parse_producers(f):
  yield from parse_people(f, 'producers', 'producer')

@imdb_parser
def parse_writers(f):
  yield from parse_people(f, 'writers', 'writer')

def parse_people(f, rtype, prole):

  skip_till(f, 2, r'^Name\s+Titles\n----\s+-----')

  role_pat = re.compile(r'''
    (?:\s\s (?P<notes> \([^)]+\) (?:\s\([^)]+\))? ) )?
    (?:\s\s\[(?P<character>[^\]]+)\])?
    (?:\s\s<(?P<ranks>[^>]+)>)?
    $
  ''', re.X)

  def get_role(v):
    v = v.split('  ', 1)
    role = {'title': v[0], 'role': prole}
    if len(v) > 1:
      m = role_pat.search('  ' + v[1])
      if m:
        if m.group('notes'):
          # XXX more processing here needed
          note = m.group('notes').replace('(%s)' % prole, '').strip()
          if note:
            role['notes'] = note
        if m.group('character'):
          role['character'] = m.group('character')
        if m.group('ranks'):
          role['ranks'] = list(map(int, m.group('ranks').split(',')))
      else:
        print('bad-role', v, file=sys.stderr)
    return role

  id, roles = None, []
  for l in f:
    if l.startswith('--------------'):
      break
    l = l.split('\t')
    if l[0]:
      if roles:
        yield id, rtype, roles
      id, roles = l[0], [get_role(l[-1])]
    else:
      roles.append(get_role(l[-1]))

  if roles:
    yield id, rtype, roles

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
  elif rtype in ('keywords', 'genres'):
    kg = title.get(rtype)
    if kg:
      kg.append(obj)
    else:
      title[rtype] = [obj]
  elif rtype == 'technical':
    title['technical'][obj[0]].append(obj[1])

def mix_name(name, rtype, obj):
  roles = name.get('roles')
  if roles is None:
    roles = []
    name['roles'] = roles
  if rtype in (
    'actresses', 'actors', 'cinematographers', 'composers', 'directors',
    'costume-designers', 'editors', 'producers', 'writers'
  ):
    roles.extend(obj)

def init_title(title):
  title['technical'] = collections.defaultdict(list)

def init_name(title):
  pass

def finalize_title(title):

  for v in title['technical'].values():
    v.sort(key=lambda x: x['name'])
  title.update(title['technical'])
  del title['technical']

  for t in ('keywords', 'genres'):
    kg = title.get(t)
    if kg:
      title[t].sort(key=str.lower)

def finalize_name(name):
  name['roles'].sort(key=lambda x: (x['title'].lower(), x['role']))

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
  initer = globals()['init_' + args.kind]
  finalizer = globals()['finalize_' + args.kind]

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
    initer(rec)
    for _, rtype, obj in tuples:
      mixer(rec, rtype, obj)
    finalizer(rec)
    json.dump(rec, sys.stdout, separators=(',', ':'), sort_keys=True)

    if args.line:
      print()

  if not args.line:
    print('\n]')

if __name__ == '__main__':
  main()
