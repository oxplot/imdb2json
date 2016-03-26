#!/usr/bin/env python
#
# imdb2json.py - Convert IMDB list files to JSON
# Copyright (C) 2015 Mansour Behabadi <mansour@oxplot.com>
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the
#    distribution.
#
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived
#    from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import argparse
import collections
import gzip
import heapq
import io
import itertools
import os
import re
import sys
import tempfile

try:
  import ujson as json
except ImportError:
  import json

STORE, APPEND = 0, 1
TITLE, NAME = 'title', 'name'
imdb_parsers = {TITLE: {}, NAME: {}}

_OUT_BUF = 1000000
_MAX_SORT_BUF = 100000

sys.stdout = open(
  sys.stdout.fileno(), 'w', encoding='utf8', buffering=_OUT_BUF
)

TID_PAT = re.compile(r'''^
  (?:
    "(?P<series>[^"]+)"\s+\([?\d/IVLX]{4,}\)
    (?:\s+(?P<epi>\{)
      (?P<epi_title>.+?)?
      (?:
        \s*\([#](?P<season>\d+)\.(?P<episode>\d+)\)
        |
        \s*\(
          (?P<epi_yr>\d{4})-(?P<epi_mon>\d{2})-(?P<epi_day>\d{2})
        \)
      )?
    \})?
    |
    (?P<non_series>.+?)\s+\([?\d/IVLX]{4,}\)
    (?P<tv>\s+\(TV\))?
    (?P<video>\s+\(V\))?
    (?P<videogame>\s+\(VG\))?
  )
  (?P<suspended>\s+\{\{SUSPENDED\}\})?
$''', re.X)

def construct_title(id):
  rec = {'id': id}
  m = TID_PAT.match(id)
  if m:
    if m.group('series'):
      rec['title'] = m.group('series')
      if m.group('epi'):
        rec['episode'] = episode = {}
        rec['kind'] = 'episode'
        if m.group('epi_title'):
          episode['title'] = m.group('epi_title')
        if m.group('season'):
          episode['season'] = int(m.group('season'))
        if m.group('episode'):
          episode['episode'] = int(m.group('episode'))
        if m.group('epi_yr'):
          episode['year'] = int(m.group('epi_yr'))
          episode['month'] = int(m.group('epi_mon'))
          episode['day'] = int(m.group('epi_day'))
      else:
        rec['kind'] = 'series'
    else:
      rec['title'] = m.group('non_series')
      if m.group('tv'):
        rec['kind'] = 'tv-movie'
      elif m.group('video'):
        rec['kind'] = 'video'
      elif m.group('videogame'):
        rec['kind'] = 'videogame'
      else:
        rec['kind'] = 'movie'
    if m.group('suspended'):
      rec['suspended'] = True
  else:
    print('bad-tid', id, file=sys.stderr)
  return rec

def construct_name(id):
  rec = {'id': id}
  return rec

constructors = {TITLE: construct_title, NAME: construct_name}

def roundrobin(*iterables):
  pending = len(iterables)
  nexts = itertools.cycle(iter(it).__next__ for it in iterables)
  while pending:
    try:
      for next in nexts:
        yield next()
    except StopIteration:
      pending -= 1
      nexts = itertools.cycle(itertools.islice(nexts, pending))

def rec_sorted(recs):
  srtd = collections.deque()
  temps = collections.deque()

  def write_tmp():
    tmp = tempfile.TemporaryFile()
    tmpw = open(
      tmp.fileno(), 'w', encoding='utf8', buffering=_OUT_BUF,
      closefd=False
    )
    for sr in sorted(srtd, key=lambda x: x[0]):
      json.dump(sr, tmpw)
      tmpw.write('\n')
    tmpw.flush()
    del tmpw
    tmp.seek(0)
    srtd.clear()
    temps.append(tmp)

  for rec in recs:
    srtd.append(rec)
    if len(srtd) >= _MAX_SORT_BUF:
      write_tmp()
  if srtd:
    write_tmp()

  yield from heapq.merge(*((
    tuple(json.loads(i)) for i in open(f.fileno(), 'r', encoding='utf8')
  ) for f in temps), key=lambda x: x[0])

def load_parser(kind, f):
  f = open(f.fileno(), 'rb')
  if f.peek(1)[:1] == b'\x1f':
    f = gzip.open(f, 'rt', encoding='latin1')
  else:
    f = io.TextIOWrapper(f, encoding='latin1')
  m = re.search(r'\sFile:\s+([^.]+)\.list\s+', f.readline())
  if not m:
    return
  parser = imdb_parsers[kind].get(m.group(1))
  if not parser:
    return
  if parser:
    yield from parser(filter(bool, (l.rstrip() for l in f)))

def imdb_parser(kind, filename):
  def wrapper(fn):
    imdb_parsers[kind][filename] = fn
    return fn
  return wrapper

def skip_till(f, window, pat):
  pat = re.compile(pat)
  deq = collections.deque(maxlen=window)
  for l in f:
    deq.append(l)
    if pat.search('\n'.join(deq)):
      break

@imdb_parser(kind=TITLE, filename='movies')
def parse_movies(f):

  skip_till(f, 2, r'^MOVIES LIST\n={8}')

  for l in f:
    if l.startswith('--------------'):
      break
    l = l.split('\t')
    yr = [None if x == '????' else int(x) for x in l[-1].split('-', 1)]
    yield l[0], STORE, 'year', yr

@imdb_parser(kind=TITLE, filename='taglines')
def parse_taglines(f):

  skip_till(f, 2, r'^TAG LINES LIST\n={8}')

  id = None
  for l in f:
    if l.startswith('--------------'):
      break
    if l.startswith('#'):
      id = l[2:]
    elif l.startswith('\t') and id:
      yield id, APPEND, 'taglines', l[1:]

def parse_bullet_pt(f, key):

  id, pts, lines = None, [], []
  for l in f:
    if l.startswith('--------------'):
      break
    if l.startswith('#'):
      if lines:
        pts.append(' '.join(lines))
      if pts:
        yield id, STORE, key, pts
      id, pts, lines = l[2:], [], []
    elif l.startswith('- '):
      if lines:
        pts.append(' '.join(lines))
      lines = [l[2:].strip()]
    elif l.startswith('  '):
      lines.append(l[2:].strip())

  if lines:
    pts.append(' '.join(lines))
  if pts:
    yield id, STORE, key, pts

@imdb_parser(kind=TITLE, filename='trivia')
def parse_trivia(f):
  skip_till(f, 2, r'^FILM TRIVIA\n={8}')
  yield from parse_bullet_pt(f, 'trivia')

@imdb_parser(kind=TITLE, filename='alternate-versions')
def parse_alternate_versions(f):
  skip_till(f, 2, r'^ALTERNATE VERSIONS LIST\n={8}')
  yield from parse_bullet_pt(f, 'alternate_versions')

@imdb_parser(kind=TITLE, filename='crazy-credits')
def parse_crazy_credits(f):
  skip_till(f, 2, r'^CRAZY CREDITS\n={8}')
  yield from parse_bullet_pt(f, 'crazy_credits')

@imdb_parser(kind=TITLE, filename='goofs')
def parse_goofs(f):

  skip_till(f, 2, r'^GOOFS LIST\n={8}')

  type_map = {
    'CONT': 'continuity', 'FAKE': 'revealing',
    'FACT': 'factual', 'GEOG': 'geographical',
    'PLOT': 'plothole', 'FAIR': 'not_goof',
    'CREW': 'crew_visible', 'DATE': 'date',
    'CHAR': 'character', 'SYNC': 'audio_video_sync',
    'MISC': 'misc', 'BOOM': 'boom_mic_visible'
  }

  for id, mix, key, pts in parse_bullet_pt(f, 'goofs'):
    yield id, mix, key, [{
      'type': type_map[p[:4]],
      'text': p[6:]
    } for p in pts]

@imdb_parser(kind=TITLE, filename='language')
def parse_language(f):

  skip_till(f, 2, r'^LANGUAGE LIST\n={8}')

  for l in f:
    if l.startswith('--------------'):
      break
    l = [i for i in l.split('\t') if i]
    lang = {'name': l[1]}
    if len(l) > 2:
      lang['note'] = l[2]
    yield l[0], APPEND, 'languages', lang

@imdb_parser(kind=TITLE, filename='running-times')
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

    yield l[0], APPEND, 'running_times', obj

@imdb_parser(kind=TITLE, filename='keywords')
def parse_keywords(f):

  skip_till(f, 2, r'^8: THE KEYWORDS LIST\n={8}')

  for l in f:
    l = l.split('\t')
    yield l[0], APPEND, 'keywords', l[-1].strip()

@imdb_parser(kind=TITLE, filename='genres')
def parse_genres(f):

  skip_till(f, 2, r'^8: THE GENRES LIST\n={8}')

  for l in f:
    l = l.split('\t')
    yield l[0], APPEND, 'genres', l[-1].strip()

@imdb_parser(kind=TITLE, filename='technical')
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
    yield l[0], APPEND, typ, val

@imdb_parser(kind=TITLE, filename='aka-titles')
def parse_aka_titles(f):

  skip_till(f, 2, r'^AKA TITLES LIST\n={8}')

  id = None
  for l in f:
    if l.startswith(' ') and id:
      l = [i for i in l.split('\t') if i]
      if not l[0].startswith('   (aka ') or not l[0].endswith(')'):
        print('bad-aka-title', l)
        continue
      aka = {'name': l[0][8:-1]} # TODO extract yr, etc
      if len(l) > 1:
        aka['note'] = l[1] # TODO extract country
      yield id, APPEND, 'aka', aka
    else:
      id = l

@imdb_parser(kind=TITLE, filename='certificates')
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
    yield l[0], APPEND, 'certificates', cert

@imdb_parser(kind=TITLE, filename='color-info')
def parse_color_info(f):

  skip_till(f, 2, r'^COLOR INFO LIST\n={8}')

  for l in f:
    if l.startswith('--------------'):
      break
    l = [i for i in l.split('\t') if i]
    info = {'color': l[1].lower()}
    if len(l) > 2:
      info['note'] = l[2] # TODO parse the data into sep fields
    yield l[0], APPEND, 'color_info', info

@imdb_parser(kind=TITLE, filename='countries')
def parse_countries(f):

  skip_till(f, 2, r'^COUNTRIES LIST\n={8}')

  for l in f:
    if l.startswith('--------------'):
      break
    l = l.split('\t')
    yield l[0], APPEND, 'countries', l[-1]

@imdb_parser(kind=TITLE, filename='distributors')
def parse_distributors(f):

  skip_till(f, 2, r'^DISTRIBUTORS LIST\n={8}')

  for l in f:
    if l.startswith('--------------'):
      break
    l = [i for i in l.split('\t') if i]

    dist = {'name': l[1]} # TODO parse country out
    if len(l) > 2:
      dist['note'] = l[2] # TODO separate fields
    
    yield l[0], APPEND, 'distributors', dist

@imdb_parser(kind=TITLE, filename='literature')
def parse_literature(f):

  skip_till(f, 2, r'^LITERATURE LIST\n={8}')

  typ_map = {
    'ADPT': 'adaptations', 'BOOK': 'books', 'NOVL': 'novels',
    'ESSY': 'essays', 'CRIT': 'printed_reviews',
    'OTHR': 'other_literature',
    'IVIW': 'interviews', 'SCRP': 'screenplays',
    'PROT': 'production_process_protocols'
  }

  id = None

  for l in f:
    if l.startswith('--------------'):
      id = None
    elif l.startswith('MOVI:'):
      id = l[6:]
    elif id:
      yield id, APPEND, typ_map[l[:4]], l[6:] # TODO parse details

@imdb_parser(kind=TITLE, filename='locations')
def parse_locations(f):

  skip_till(f, 2, r'^LOCATIONS LIST\n={8}')

  for l in f:
    if l.startswith('--------------'):
      break
    l = [i for i in l.split('\t') if i]
    loc = {'name': l[1]}
    if len(l) > 2:
      loc['note'] = l[2]
    yield l[0], APPEND, 'locations', loc

def parse_companies(f, type):

  for l in f:
    if l.startswith('--------------'):
      break
    l = [i for i in l.split('\t') if i]
    comp = {'name': l[1], 'type': type}
    if len(l) > 2:
      comp['note'] = l[2]
    yield l[0], APPEND, 'companies', comp

@imdb_parser(kind=TITLE, filename='miscellaneous-companies')
def parse_miscellaneous_companies(f):
  skip_till(f, 2, r'^MISCELLANEOUS COMPANIES LIST\n={8}')
  yield from parse_companies(f, 'miscellaneous')

@imdb_parser(kind=TITLE, filename='production-companies')
def parse_production_companies(f):
  skip_till(f, 2, r'^PRODUCTION COMPANIES LIST\n={8}')
  yield from parse_companies(f, 'production')

@imdb_parser(kind=TITLE, filename='special-effects-companies')
def parse_special_effects_companies(f):
  skip_till(f, 2, r'^SPECIAL EFFECTS COMPANIES LIST\n={8}')
  yield from parse_companies(f, 'special_effects')

@imdb_parser(kind=TITLE, filename='movie-links')
def parse_movie_links(f):

  skip_till(f, 2, r'^MOVIE LINKS LIST\n={8}')

  rel_map = {
    '  (follows ': 'follows',
    '  (followed by ': 'followed_by',
    '  (version of ': 'alt_version',
    '  (alternate language version of ': 'alt_language'
  }

  id = None
  for l in f:
    if l.startswith('--------------'):
      break
    if l.startswith(' ') and id:
      for relf, relt in rel_map.items():
        if l.startswith(relf):
          link = {'title': l[len(relf):-1], 'rel': relt}
          yield id, APPEND, 'links', link
          break
    else:
      id = l

@imdb_parser(kind=TITLE, filename='mpaa-ratings-reasons')
def parse_mpaa_ratings_reasons(f):

  skip_till(f, 2, r'^MPAA RATINGS REASONS LIST\n={8}')

  pat = re.compile(r'''
    ^\s*[:-]?\s*Rated\s+(?P<rating>.+?)
    (?:\s+(?P<reason>(?:on|for)\s+.+))?$
  ''', re.X | re.I)

  def build(rr):
    rr = ' '.join(rr)
    m = pat.match(rr)
    if m:
      rr = {'rating': m.group('rating').replace(' ', '')}
      if m.group('reason'):
        rr['reason'] = m.group('reason')
      return id, STORE, 'mpaa_rating', rr
    else:
      print('bad-mpaa', rr, file=sys.stderr)

  id, rr = None, []
  for l in f:
    if l.startswith('--------------'):
      if rr and id:
        rr = build(rr)
        if rr:
          yield rr
      id, rr = None, []
    elif l.startswith('MV: '):
      id = l[4:]
    elif l.startswith('RE: '):
      rr.append(l[4:])

  if rr and id:
    rr = build(rr)
    if rr:
      yield rr

@imdb_parser(kind=TITLE, filename='ratings')
def parse_ratings(f):

  skip_till(f, 2, r'^MOVIE RATINGS REPORT\nNew\s+Distri')

  pat = re.compile(r'^\s+([^\s]+)\s+(\d+)\s+([\d.]+)\s+(.*)$')

  for l in f:
    if l.startswith('--------------'):
      break
    m = pat.match(l)
    yield m.group(4), STORE, 'rating', {
      'rank': float(m.group(3)), 'votes': int(m.group(2)),
      'distribution': m.group(1)
    }

@imdb_parser(kind=TITLE, filename='release-dates')
def parse_release_dates(f):

  skip_till(f, 2, r'^RELEASE DATES LIST\n={8}')

  for l in f:
    if l.startswith('--------------'):
      break
    l = [i for i in l.split('\t') if i]
    p2 = l[1].split(':', 1)
    rd = {'country': p2[0], 'date': p2[1]} # TODO parse date
    if len(l) > 2:
      rd['note'] = l[2]
    yield l[0], APPEND, 'release_dates', rd

@imdb_parser(kind=TITLE, filename='sound-mix')
def parse_sound_mix(f):

  skip_till(f, 2, r'^SOUND-MIX LIST\n={8}')

  for l in f:
    if l.startswith('--------------'):
      break
    l = [i for i in l.split('\t') if i]
    mix = {'type': l[1].lower()}
    if len(l) > 2:
      mix['note'] = l[2]
    yield l[0], APPEND, 'sound_mix', mix

@imdb_parser(kind=TITLE, filename='plot')
def parse_plot(f):

  skip_till(f, 2, r'^PLOT SUMMARIES LIST\n={8}')

  id, lines, author = None, [], None
  for l in f:
    if l.startswith('--------------'):
      if id and lines:
        yield id, APPEND, 'plots', {'plot': ' '.join(lines)}
      id, lines, author = None, [], None
    elif l.startswith('MV: '):
      id = l[4:]
    elif l.startswith('PL: '):
      lines.append(l[4:])
    elif l.startswith('BY: '):
      author = l[4:]
      yield id, APPEND, 'plots', {'by': author, 'plot': ' '.join(lines)}
      lines, author = [], None

  if id and lines:
    yield id, APPEND, 'plots', {'plot': ' '.join(lines)}

def people_parser_gen(filename, role):
  @imdb_parser(kind=NAME, filename=filename)
  def _parse_people(f):
    yield from parse_people(f, role)

for filename, role in [
  ('actresses', 'actress'), ('composers', 'composer'),
  ('actors', 'actor'), ('directors', 'director'),
  ('cinematographers', 'cinematographer'), ('editors', 'editor'),
  ('costume-designers', 'costume-designer'), ('writers', 'writer'),
  ('producers', 'producer'), ('miscellaneous', 'miscellaneous'),
  ('production-designers', 'production-designer')
]:
  people_parser_gen(filename, role)

def parse_people(f, prole):

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

  id = None
  for l in f:
    if l.startswith('--------------'):
      break
    l = l.split('\t')
    if id or l[0]:
      if l[0]:
        id = l[0]
      yield id, APPEND, 'roles', get_role(l[-1])

@imdb_parser(kind=NAME, filename='aka-names')
def parse_aka_names(f):

  skip_till(f, 2, r'^AKA NAMES LIST\n={8}')

  id = None
  for l in f:
    if l.startswith(' '):
      if not l.startswith('   (aka ') or not l.endswith(')'):
        print('bad-aka-name', l)
        continue
      if id:
        yield id, APPEND, 'aka', l[8:-1]
    else:
      id = l

@imdb_parser(kind=NAME, filename='biographies')
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
        for k, v in build_bio(bio).items():
          yield id, STORE, k, v
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
    for k, v in build_bio(bio).items():
      yield id, STORE, k, v

def main():
  "Run main program."

  def file_arg(path):
    try:
      return sys.stdin if path == '-' else open(path, 'rb')
    except FileNotFoundError:
      raise argparse.ArgumentTypeError(
        "'%s' not found" % path
      )

  parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description='Convert IDMB list files to JSON'
  )
  subparsers = parser.add_subparsers(
    title='main commands',
    dest='cmd'
  )
  subparsers.required = True
  parser_a = subparsers.add_parser(
    'convert',
    help='convert from IMDB list files to JSON'
  )
  parser_a.set_defaults(fn=do_convert)
  parser_a.add_argument(
    'kind',
    choices=['title', 'name'],
    help='choose between movies/people output'
  )
  parser_a.add_argument(
    'file',
    nargs='*',
    default=[sys.stdin],
    type=file_arg,
    help=".list or .list.gz file, '-' means stdin"
  )
  parser_a = subparsers.add_parser(
    'merge',
    help='merge multiple JSON streams into one'
  )
  parser_a.set_defaults(fn=do_merge)
  parser_a.add_argument(
    'file',
    nargs='*',
    default=[sys.stdin],
    type=file_arg,
    help="JSON file, '-' means stdin"
  )
  parser_a = subparsers.add_parser(
    'list',
    help='list the supported IMDB file names'
  )
  parser_a.set_defaults(fn=do_list)
  parser_a.add_argument(
    'kind',
    choices=['title', 'name'],
    help='choose between movies/people kind'
  )

  args = parser.parse_args()
  args.fn(args)

def do_list(args):

  for n in sorted(imdb_parsers[args.kind]):
    print(n)

def do_merge(args):

  for id, recs in itertools.groupby(heapq.merge(*(
    (json.loads(l) for l in open(f.fileno(), 'r', encoding='utf8'))
    for f in args.file
  ), key=lambda x: x['id']), key=lambda x: x['id']):
    rec = {}
    for r in recs:
      rec.update(r)
    json.dump(rec, sys.stdout)
    print()

def do_convert(args):

  construct = constructors[args.kind]

  for id, tuples in itertools.groupby(rec_sorted(roundrobin(
    *(load_parser(args.kind, f) for f in args.file)
  )), key=lambda x: x[0]):

    rec = construct(id)

    for _, mix, key, value in tuples:
      if mix == STORE:
        rec[key] = value
      elif mix == APPEND:
        lst = rec.get(key)
        if lst is None:
          rec[key] = lst = []
        lst.append(value)

    json.dump(rec, sys.stdout)
    print()

  sys.stdout.flush()

if __name__ == '__main__':
  main()
