#!/usr/bin/python

#######
####### wiki_disambig.py
#######
####### Copyright (c) 2010 Ben Wing.
#######

import sys, re
import os, os.path
import math
import collections
import traceback
from optparse import OptionParser
from perlwrap import *

# Count number of incoming links for articles
incoming_link_count = {}

# Coordinates for articles
article_coordinates = {}

# Map from short to full form of articles.  Each a value is a list of all
# the full forms.
short_to_full_name = {}

# Map from tuple (CITY, DIV) for articles of the form "Springfield, Ohio".
# Maps to the full article name.
tuple_to_full_name = {}

# List of stopwords
stopwords = set()

# For each Wikipedia article, value is a hash table of word->probability
# items.  In addition, an item with key None lists the probability of an
# unknown word.
article_probs = {}

# For each toponym, value is a list of Location items, listing gazetteer
# locations and corresponding matching Wikipedia articles.
toponym_to_location = {}

correct_toponyms_disambiguated = 0
total_toponyms_disambiguated = 0

# Debug level; if non-zero, output lots of extra information about how
# things are progressing.  If > 1, even more info.
debug = 1

# If true, print out warnings about strangely formatted input
show_warnings = True

# After ignoring stopwords, do we renormalize the probabilities or not?
renormalize_non_stopword_probs = True

############################################################################
#                              Documentation                               #
############################################################################

##### Quick start

# This program does disambiguation of geographic names on the TR-CONLL corpus.
# It uses data from Wikipedia to do this.  It is "unsupervised" in the sense
# that it does not do any supervised learning using the correct matches
# provided in the corpus; instead, it uses them only for testing purposes.

############################################################################
#                                   Code                                   #
############################################################################

#######################################################################
###                        Utility functions                        ###
#######################################################################

def uniprint(text):
  '''Print Unicode text in UTF-8, so it can be output without errors'''
  if type(text) is unicode:
    print text.encode("utf-8")
  else:
    print text

def warning(text):
  '''Output a warning, formatting into UTF-8 as necessary'''
  if show_warnings:
    uniprint("Warning: %s" % text)

def safe_float(x):
  '''Convert a string to floating point, but don't crash on errors;
instead, output a warning.'''
  try:
    return float(x)
  except:
    x = x.strip()
    if x:
      warning("Expected number, saw %s" % x)
    return 0.

# Compute spherical distance in miles (along a great circle) between two
# latitude/longitude points.

def spherical_distance(lat1, long1, lat2, long2):
  thisRadLat = (lat1 / 180.) * math.pi
  thisRadLong = (long1 / 180.) * math.pi
  otherRadLat = (lat2 / 180.) * math.pi
  otherRadLong = (long2 / 180.) * math.pi
        
  # 3963.191 = Earth radius in miles
  try:
    return 3963.191 * \
      math.acos(math.sin(thisRadLat)*math.sin(otherRadLat)
                + math.cos(thisRadLat)*math.cos(otherRadLat)*
                  math.cos(otherRadLong-thisRadLong))
  except Exception, exc:
    warning("Exception %s computing spherical distance (%s,%s) to (%s,%s)" %
            (exc, lat1, long1, lat2, long2))
    return 1000000.

#######################################################################
#                            Process files                            #
#######################################################################

# Read in the list of stopwords from the given filename.
def read_stopwords(filename):
  for line in chompopen(filename):
    stopwords.add(line)

# Parse the result of a previous run of --coords-counts and generate
# a unigram distribution for Naive Bayes matching.  We do a simple version
# of Good-Turing smoothing where we assign probability mass to unknown
# words equal to the probability mass of all words seen once, and rescale
# the remaining probabilities accordingly.

def construct_naive_bayes_dist(filename):
  articles_seen = 0
  for line in chompopen(filename):
    if rematch('Article title: (.*)$', line):
      title = m_[1]
      wordprob = {}
      wordhash = {}
      totalcount = 0
      oncecount = 0
    elif rematch('Article coordinates: (.*),(.*)$', line):
      # Compute probabilities.  Use a very simple version of Good-Turing
      # smoothing where we assign to unknown words the probability mass of
      # words seen once, and adjust all other probs accordingly.
      unknown_mass = float(oncecount)/totalcount
      # I would just reuse wordhash, but I'm not sure whether it's allowed
      # to modify an item you're iterating over
      for (word,count) in wordhash.iteritems():
        wordprob[word] = float(count)/totalcount*unknown_mass
      wordprob[None] = unknown_mass
      article_probs[title] = wordprob
      articles_seen += 1
      if (articles_seen % 10000) == 0:
        print "Processed %d articles" % articles_seen
    else:
      assert rematch('(.*) = ([0-9]+)$', line)
      word = m_[1]
      count = int(m_[2])
      if word in stopwords and not renormalize_non_stopword_probs: continue
      totalcount += count
      if count == 1: oncecount += 1
      if word in stopwords: continue
      wordhash[word] = count

class Location(object):
  pass

# Parse the result of a previous run of --coords-counts for articles with
# coordinates
def get_coordinates(filename):
  for line in open(filename):
    line = line.strip()
    if rematch('Article title: (.*)$', line):
      title = m_[1]
    elif rematch('Article coordinates: (.*),(.*)$', line):
      article_coordinates[title] = (safe_float(m_[1]), safe_float(m_[2]))
      short = title
      if rematch('(.*?), (.*)$', title):
        short = m_[1]
        tuple = (m_[1], m_[2])
        tuple_to_full_name[tuple] = title
      if short not in short_to_full_name:
        short_to_full_name[short] = []
      short_to_full_name[short].append(title)

# Maximum number of miles allowed when looking for a close match
maxdist = 20

def read_world_gazetteer_and_match(filename):
  def record_match(loc, match, lat, long, dist):
    lowername = loc.name.lower()
    if lowername not in toponym_to_location:
      toponym_to_location[lowername] = []
    toponym_to_location[lowername].append(loc)
    loc.match = match
    if debug > 1:
      uniprint("Matched location %s with coordinates (%s,%s), dist=%s" %
               (match, lat, long, dist))

  class GetOut: pass

  def output_non_match(match, lat, long, dist):
    if debug > 1:
      uniprint("Found location %s with coordinates (%s,%s) but dist %s > %s" %
               (match, lat, long, dist, maxdist))

  for line in chompopen(filename):
    fields = re.split(r'\t', line.strip())
    # Skip places without coordinates
    if len(fields) < 8 or not fields[6] or not fields[7]: continue
    loc = Location()
    loc.lat  = int(fields[6]) / 100.
    loc.long = int(fields[7]) / 100.
    loc.name = fields[1].strip()
    loc.type = fields[4].strip()
    loc.country = ''
    loc.div1 = ''
    loc.div2 = ''
    if len(fields) >= 9: loc.country = fields[8].strip()
    if len(fields) >= 10: loc.div1 = fields[9].strip()
    if len(fields) >= 11: loc.div2 = fields[10].strip()

    if debug > 1:
      uniprint("Saw location %s (div %s/%s/%s) with coordinates (%s,%s)" %
               (loc.name, loc.country, loc.div1, loc.div2, loc.lat, loc.long))

    def check_match(artname):
      (artlat, artlong) = article_coordinates[artname]
      dist = spherical_distance(loc.lat, loc.long, artlat, artlong)
      if dist <= maxdist:
        record_match(loc, artname, artlat, artlong, dist)
        return True
      else:
        output_non_match(artname, artlat, artlong, dist)
        return False

    try:
      if loc.name in article_coordinates:
        if check_match(loc.name): raise GetOut
      for div in (loc.country, loc.div1, loc.div2):
        artname = tuple_to_full_name.get((loc.name, div), None)
        if artname:
          if check_match(artname): raise GetOut
      artnames = short_to_full_name.get(loc.name, None)
      if artnames:
        if len(artnames) == 1:
          if check_match(artnames[0]): raise GetOut
        else:
          dists_names = []
          for artname in artnames:
            (artlat, artlong) = article_coordinates[artname]
            dists_names.append((artname, spherical_distance(loc.lat, loc.long,
                                                            artlat, artlong)))
          dists_names.sort(key = lambda x:x[1])
          if debug > 1:
            print("Warning: Saw %s short-name matches" % len(artnames))
          if debug > 1:
            for i in xrange(len(dists_names)):
              match = dists_names[i]
              artlat, artlong = article_coordinates[match[0]]
              uniprint("Match #%d: %s, dist=%s, coords (%s,%s)" %
                       (i+1, match[0], match[1], artlat, artlong))
          matchname = dists_names[0][0]
          matchdist = dists_names[0][1]
          (artlat, artlong) = article_coordinates[matchname]
          record_match(loc, matchname, artlat, artlong, matchdist)
          raise GetOut
      if debug > 1:
        uniprint("Unmatched name %s" % loc.name)
    except GetOut: pass

def read_incoming_link_info(filename):
  fi = open(filename)
  articles_seen = 0
  for line in fi:
    if rematch('------------------ Count of incoming links: ------------',
               line): continue
    elif rematch('==========================================', line):
      fi.close()
      return
    else:
      assert rematch('(.*) = ([0-9]+)$', line)
      incoming_link_count[m_[1]] = int(m_[2])
      articles_seen += 1
      if (articles_seen % 10000) == 0:
        print "Processed %d articles" % articles_seen

def read_trconll_file(filename):
  results = []
  words = collections.deque()
  in_loc = False
  for line in chompopen(filename):
    try:
      (word, ty) = re.split('\t', line, 1)
      if word and word not in stopwords:
        pass # FIXME
      if in_loc and word:
        in_loc = False
      elif ty.startswith('LOC'):
        in_loc = True
        loc_word = word
      elif in_loc and ty[0] == '>':
        (off, gaz, lat, long, fulltop) = re.split('\t', ty, 4)
        lat = float(lat)
        long = float(long)
        results.append((loc_word, lat, long))
        if debug > 0:
          uniprint("Saw loc %s with true coordinates %s,%s" %
                   (loc_word, lat, long))
    except Exception as exc:
      print "Bad line %s" % line
      print "Exception is %s" % exc
      if type(exc) is not ValueError:
        traceback.print_exc()
      return results
  return results

# Process all files in DIR, calling FUN on each one (with the directory name
# joined to the name of each file in the directory).
def process_dir_files(dir, fun):
  for fname in os.listdir(dir):
    fullname = os.path.join(dir, fname)
    fun(fullname)
  
def disambiguate_link_baseline(fname):
  print "Processing TR-CONLL file %s..." % fname
  results = read_trconll_file(fname)
  for (toponym, lat, long) in results:
    lowertop = toponym.lower()
    maxlinks = 0
    bestloc = None
    if lowertop not in toponym_to_location:
      if debug > 0:
        uniprint("Unable to find any possibilities for %s" % toponym)
      correct = False
    else:
      locs = toponym_to_location[lowertop]
      if debug > 0:
        uniprint("Considering toponym %s, coordinates %s,%s" %
                 (toponym, lat, long))
        uniprint("For toponym %s, %d possible locations" %
                 (toponym, len(locs)))
      for loc in locs:
        if debug > 0:
          artname = loc.match
          uniprint("Considering article %s" % artname)
          if artname not in incoming_link_count:
            warning("Strange, article %s has no link count" % artname)
            thislinks = 0
          else:
            thislinks = incoming_link_count[artname]
          if thislinks > maxlinks:
            maxlinks = thislinks
            bestloc = loc
      if bestloc:
        dist = spherical_distance(lat, long, bestloc.lat, bestloc.long)
        correct = dist <= maxdist
      else:
        # If we couldn't find link counts, this can happen
        correct = False
    if correct:
      global correct_toponyms_disambiguated
      correct_toponyms_disambiguated += 1
    global total_toponyms_disambiguated
    total_toponyms_disambiguated += 1
    if debug > 0 and bestloc:
      uniprint("Best match = %s, coordinates %s,%s, dist %s, correct %s"
               % (bestloc.match, bestloc.lat, bestloc.long, dist, correct))

#######################################################################
#                                Main code                            #
#######################################################################

def main():
  op = OptionParser(usage="%prog [options] input_dir")
  op.add_option("-t", "--gazetteer-type", default="world",
                choices=['world', 'db'],
                help="Type of gazetteer file specified using --gazetteer.")
  op.add_option("-l", "--links-file",
                help="""File containing incoming link information for Wikipedia articles.
Output by processwiki.py --find-links.""",
                metavar="FILE")
  op.add_option("-s", "--stopwords-file",
                help="""File containing list of stopwords.""",
                metavar="FILE")
  op.add_option("-g", "--gazetteer-file",
                help="""File containing gazetteer information to match.""",
                metavar="FILE")
  op.add_option("-c", "--coords-file",
                help="""File containing output from a prior run of
--coords-counts, listing all the articles with associated coordinates.
May be filtered only for articles and coordinates.""",
                metavar="FILE")
  op.add_option("-w", "--word-coords-file",
                help="""File containing output from a prior run of
--coords-counts, listing all the articles with associated coordinates.
Should not be filtered, as the counts of words are needed.""",
                metavar="FILE")
  op.add_option("-f", "--trconll-dir",
                help="""Directory containing TR-CONLL files in the text format.
Each file is read in and then disambiguation is performed.""",
                metavar="DIR")
  op.add_option("-b", "--link-baseline", action="store_true",
                help="""Output the baseline determined by using the matching
toponym with the highest incoming link count in Wikipedia.""")
  op.add_option("-m", "--only-match", action="store_true",
                help="""If specified, only do the "match" stage, where locations
in the gazetteer are matched with the corresponding Wikipedia articles to find
the best match.  Most useful when debug > 0.""")
  op.add_option("-d", "--debug", metavar="LEVEL",
                help="Output debug info at given level")
  opts, args = op.parse_args()

  global debug
  if opts.debug:
    debug = int(opts.debug)
 
  # FIXME! Can only currently handle World-type gazetteers.
  assert opts.gazetteer_type == 'world'

  if not opts.coords_file:
    op.error("Must specify coordinate file using -c or --coords-file")

  if not opts.only_match:
    if not opts.gazetteer_file:
      op.error("Must specify coordinate file using -g or --gazetteer-file")
    if not opts.stopwords_file:
      op.error("Must specify stopwords file using -s or --stopwords-file")
    if not opts.trconll_dir:
      op.error("Must specify TR-CONLL directory using -f or --trconll-dir")

    print "Reading stopwords file %s..." % opts.stopwords_file
    read_stopwords(opts.stopwords_file)

  print "Reading coordinates file %s..." % opts.coords_file
  get_coordinates(opts.coords_file)
  print "Reading and matching World gazetteer file %s..." % opts.gazetteer_file
  read_world_gazetteer_and_match(opts.gazetteer_file)

  if not opts.only_match:
    print "Reading incoming links file %s..." % opts.links_file
    read_incoming_link_info(opts.links_file)
    if opts.link_baseline:
      print "Processing TR-CONLL directory %s..." % opts.trconll_dir
      process_dir_files(opts.trconll_dir, disambiguate_link_baseline)
      print("Percent correct = %s/%s = %5.2f" %
            (correct_toponyms_disambiguated, total_toponyms_disambiguated,
             100*float(correct_toponyms_disambiguated)/
                 total_toponyms_disambiguated))
    else:
      construct_naive_bayes_dist(opts.word_coords_file)
      process_dir_files(opts.trconll_dir, disambiguate_naive_bayes)

main()
