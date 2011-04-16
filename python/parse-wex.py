#!/usr/bin/env python

import fileinput
import re
import sys
import itertools
from nlputil import *

def printable(line):
  return line.replace('\n', r'\n')

def process_params(article_wpid, params, args):
  #ty = None
  #reg = None
  #for param in these_params:
  #  for (key, val) in param:
  #    if key == 'type' and ty is None:
  #      ty = val
  #    if key == 'region' and reg is None:
  #      reg = val
  errprint("article_wpid: %s, %s, %s" % (article_wpid, params, args))

def process_article_lines(article_id, rawargs):
  for call_id, idargs in itertools.groupby(rawargs, key=lambda x:x[0]):
    params = {}
    args = []
    for idarg in idargs:
      try:
        (_, name, xml, _, id, section_id, template_article_name, line) = idarg
        if call_id != id:
          warning("Call id is %s but id is %s; line [%s]" % (
              call_id, id, line))
        m = re.match('<param name="(.*?)">(.*)</param>$', xml.strip())
        if not m:
          warning("Can't parse line: [%s]" % line)
          continue
        paramname = m.group(1)
        if paramname != name:
          warning("paramname is %s but name is %s; line [%s]" % (
              paramname, name, line))
        xmlval = m.group(2)
        if ':' in name:
          xmlval = name + ':' + xmlval
        elif not re.match(r'^[0-9]+$', name):
          params[name] = xmlval
          continue
        if ':' in xmlval:
          param_pairs = xmlval.split('_')
          for pair in param_pairs:
            (key, val) = pair.split(':')
            params[key] = val
        else:
          args += [xmlval]
      except Exception, e:
        warning("Saw exception %s: Line is [%s]" % (e, line))
    process_params(article_id, params, args)

# Read in the raw file containing output from PostGreSQL, ignore headers,
# join continued lines
def yield_joined_lines(file):
  # First two lines are headers
  file.next()
  file.next()

  for line in file:
    line = line.strip()
    try:
      while not line.endswith('Template:Coord'):
        contline = file.next().strip()
        line += contline
    except StopIteration:
      warning("Partial, unfinished line [%s]" % line)
      break
    yield line

# Read in lines, split and yield lists of the arguments, with the line itself
# as the last argument
def yield_arguments(lines):
  for line in lines:
    rawarg = [x.strip() for x in line.split('|')]
    if len(rawarg) != 7:
      warning("Wrong number of fields in line [%s]" % line)
      continue
    yield rawarg + [line]

def process_file():
  gen = yield_arguments(yield_joined_lines(fileinput.input()))

  for article_id, article_rawargs in itertools.groupby(gen, key=lambda x:x[3]):
    process_article_lines(article_id, sorted(article_rawargs))

process_file()
