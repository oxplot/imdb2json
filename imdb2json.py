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
    'costume-designers', 'editors', 'producers', 'writers', 'aka-names',
    'biographies'
  ],
  'title': [
    'movies', 'taglines', 'trivia', 'running-times', 'keywords',
    'genres', 'technical', 'aka-titles', 'alternate-versions',
    'certificates'
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

  yield from parse_bullet_pt(f, 'trivia')

@imdb_parser
def parse_alternate_versions(f):

  skip_till(f, 2, r'^ALTERNATE VERSIONS LIST\n={8}')

  yield from parse_bullet_pt(f, 'alternate-versions')

def parse_bullet_pt(f, rtype):

  id, pt, lines = None, [], []
  for l in f:
    if l.startswith('--------------'):
      break
    if l.startswith('#'):
      if lines:
        pt.append(' '.join(lines))
      if pt:
        yield id, rtype, pt
      id, pt, lines = l[2:], [], []
    elif l.startswith('- '):
      if lines:
        pt.append(' '.join(lines))
      lines = [l[2:].strip()]
    elif l.startswith('  '):
      lines.append(l[2:].strip())

  if lines:
    pt.append(' '.join(lines))
  if pt:
    yield id, rtype, pt

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
    l = [i for i in l.split('\t') if i]
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
def parse_aka_titles(f):

  skip_till(f, 2, r'^AKA TITLES LIST\n={8}')

  id, akas = None, []
  for l in f:
    if l.startswith(' '):
      l = [i for i in l.split('\t') if i]
      if not l[0].startswith('   (aka ') or not l[0].endswith(')'):
        print('bad-aka-title', l)
        continue
      aka = {'name': l[0][8:-1]} # TODO extract yr, etc
      if len(l) > 1:
        aka['note'] = l[1] # TODO extract country
      akas.append(aka)
    else:
      if akas:
        yield id, 'aka-titles', akas
      id, akas = l, []
  if akas:
    yield id, 'aka-titles', akas

@imdb_parser
def parse_certificates(f):

  skip_till(f, 2, r'^CERTIFICATES LIST\n={8}')

  for l in f:
    if l.startswith('--------------'):
      break
    l = [i for i in l.split('\t') if i]
    cert = {}
    cert['country'], cert['rating'] = l[1].split(':', 1)
    if len(l) > 2:
      cert['note'] = l[2] # TODO parse the data into sep fields
    yield l[0], 'certificates', cert

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
    (?:\s\s (?P<note> \([^)]+\) (?:\s\([^)]+\))? ) )?
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
        if m.group('note'):
          # TODO more processing here needed
          note = m.group('note').replace('(%s)' % prole, '').strip()
          if note:
            role['note'] = note
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

@imdb_parser
def parse_aka_names(f):

  skip_till(f, 2, r'^AKA NAMES LIST\n={8}')

  id, akas = None, []
  for l in f:
    if l.startswith(' '):
      if not l.startswith('   (aka ') or not l.endswith(')'):
        print('bad-aka-name', l)
        continue
      akas.append(l[8:-1])
    else:
      if akas:
        yield id, 'aka-names', akas
      id, akas = l, []
  if akas:
    yield id, 'aka-names', akas

@imdb_parser
def parse_biographies(f):

  skip_till(f, 2, r'^BIOGRAPHY LIST\n={8}')

  def build_bio(b):
    bio = {}
    bio_texts = []

    for text, by in b['BIO']:
      text = ' '.join(i if i else '\n' for i in text)
      text = text.replace(' \n ', '\n').strip()
      if text:
        bio_texts.append({'text': text, 'by': by})
    if bio_texts:
      bio['biographies'] = bio_texts

    for short, long in [
      ('OW', 'other_works'), ('BO', 'print_biography'),
      ('QU', 'quotes'), ('IT', 'interviews'), ('AT', 'articles'),
      ('CV', 'cover_photos'), ('TR', 'trivia'), ('SP', 'spuoses'),
      ('PT', 'pictorials'), ('TM', 'trademarks'), ('PI', 'portrayals'),
      ('SA', 'salaries'), ('BT', 'biographical_movies')
    ]:
      coll, el = [], []
      for l in b[short]:
        if l.startswith('*'):
          if el:
            coll.append(' '.join(el))
          el = []
        el.append(l[2:])
      if el:
        coll.append(' '.join(el))
      if coll:
        bio[long] = coll

    for short, long in [
      ('DD', 'date_of_death'), ('DB', 'date_of_birth'),
      ('HT', 'height'), ('RN', 'real_name')
    ]:
      if b[short]:
        bio[long] = b[short][0]

    # TODO data extraction for various fields (e.g. DOB)
    return bio

  id, bio = None, collections.defaultdict(list)
  for l in f:
    if l.startswith('-------'):
      if bio and id:
        yield id, 'biographies', build_bio(bio)
      id, bio = None, collections.defaultdict(list)
    else:
      l = l.split(':', 1)
      if l[0] == 'NM':
        id = l[1][1:]
      elif l[0] == 'BY':
        bio['BIO'].append((bio['BG'], l[1][1:]))
        del bio['BG']
      else:
        bio[l[0]].append(l[1][1:])
  if bio and id:
    yield id, 'biographies', build_bio(bio)

def mix_title(title, rtype, obj):
  if 'cat' not in title:
    pass # TODO
  if rtype == 'movies':
    title['yr'] = obj
  elif rtype == 'taglines':
    title['taglines'] = obj
  elif rtype == 'trivia':
    title['trivia'] = obj
  elif rtype == 'alternate-versions':
    title['alternates'] = obj
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
  elif rtype == 'aka-titles':
    title['akas'] = obj
  elif rtype == 'certificates':
    certs = title.get('certificates')
    if certs:
      certs.append(obj)
    else:
      title['certificates'] = [obj]

def mix_name(name, rtype, obj):
  if rtype in (
    'actresses', 'actors', 'cinematographers', 'composers', 'directors',
    'costume-designers', 'editors', 'producers', 'writers'
  ):
    roles = name.get('roles')
    if roles is None:
      roles = []
      name['roles'] = roles
    roles.extend(obj)
  elif rtype == 'aka-names':
    name['akas'] = obj
  elif rtype == 'biographies':
    name['bio'] = obj

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

  if 'akas' in title:
    title['akas'].sort(key=lambda x: x['name'].lower())

def finalize_name(name):
  if 'roles' in name:
    name['roles'].sort(key=lambda x: (x['title'].lower(), x['role']))
  if 'akas' in name:
    name['akas'].sort(key=str.lower)
  name.update(name['bio'])
  del name['bio']

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
