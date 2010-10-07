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
from textutil import *

############################################################################
#                              Documentation                               #
############################################################################

##### Quick start

# This program does disambiguation of geographic names on the TR-CONLL corpus.
# It uses data from Wikipedia to do this.  It is "unsupervised" in the sense
# that it does not do any supervised learning using the correct matches
# provided in the corpus; instead, it uses them only for evaluation purposes.

############################################################################
#                                  Globals                                 #
############################################################################

# Count number of incoming links for Wikipedia articles.
incoming_link_count = {}

# Coordinates for articles
article_coordinates = {}

# Map from short to full form of Wikipedia articles.  Each a value is a list
# of all the full forms.  The "short form" of an article's name is the
# form before any comma, e.g. the short form of "Springfield, Ohio" is
# "Springfield".
short_to_full_name = listdict()

# Map from tuple (CITY, DIV) for Wikipedia articles of the form
# "Springfield, Ohio".  Maps to the full article name.
tuple_to_full_name = {}

# List of stopwords
stopwords = set()

# For each Wikipedia article, value is a hash table of word->probability
# items.  In addition, an item with key None lists the probability of an
# unknown word.
article_probs = {}

# For each toponym (name of location), value is a list of Locality items,
# listing gazetteer locations and corresponding matching Wikipedia articles.
toponym_to_location = listdict()

# For each toponym corresponding to a division higher than a locality,
# list of divisions with this name.
toponym_to_division = listdict()

# For each division, map from division's path to Division object.
path_to_division = {}

correct_toponyms_disambiguated = 0
total_toponyms_disambiguated = 0

# Debug level; if non-zero, output lots of extra information about how
# things are progressing.  If > 1, even more info.
debug = 2

# If true, print out warnings about strangely formatted input
show_warnings = True

# After ignoring stopwords, do we renormalize the probabilities or not?
renormalize_non_stopword_probs = True

# Maximum number of miles allowed when looking for a close match
max_dist_for_close_match = 20

# Maximum number of miles allowed between a point and any others in a
# division.  Points farther away than this are ignored as "outliers"
# (possible errors, etc.).
max_dist_for_outliers = 200

# Number of words on either side used for context
naive_bayes_context_len = 10

############################################################################
#                                   Code                                   #
############################################################################

# A general location (either locality or division).  The following
# fields are defined:
#
#   name: Name of location.
#   altnames: List of alternative names of location.
#   type: Type of location (locality, agglomeration, country, state,
#                           territory, province, etc.)
#   match: Wikipedia article corresponding to this location.
#   div: Next higher-level division this location is within, or None.

class Location(object):
  pass

# A location corresponding to an entry in a gazetteer, with a single
# coordinate.
#
# The following fields are defined, in addition to those for Location:
#
#   coord: Coordinates of the location, as a Coord object.

class Locality(Location):
  def __init__(self, name, coord):
    self.name = name
    self.coord = coord
    self.altnames = []
    self.match = None

# A class holding the boundary of a geographic object.  Currently this is
# just a bounding box, but eventually may be expanded to including a
# convex hull or more complex model.

class Boundary(object):
  def __init__(self, botleft, topright):
    self.botleft = botleft
    self.topright = topright

  def __str__(self):
    return '%s-%s' % (self.botleft, self.topright)

  def __contains__(self, coord):
    return (coord.lat >= self.botleft.lat and
            coord.lat <= self.topright.lat and
            coord.long >= self.botleft.long and
            coord.long <= self.topright.long)

# A division higher than a single locality.  According to the World
# gazetteer, there are three levels of divisions.  For the U.S., this
# corresponds to country, state, county.
#
# The following fields are defined:
#
#   level: 1, 2, or 3 for first, second, or third-level division
#   path: Tuple of same size as the level #, listing the path of divisions
#         from highest to lowest, leading to this division.  The last
#         element is the same as the "name" of the division.
#   points: Set of Coord objects listing all points inside of the division.
#   goodpoints: Set of Coord objects listing all points inside of the division
#               other than those rejected as outliers (too far from all other
#               points).
#   boundary: A Boundary object specifying the boundary of the area of the
#             division.  Currently in the form of a rectangular bounding box.
#             Eventually may contain a convex hull or even more complex
#             region (e.g. set of convex regions).

class Division(object):
  def __init__(self, path):
    self.name = path[-1]
    self.altnames = []
    self.path = path
    self.level = len(path)
    self.points = []
    self.match = None

  def __str__(self):
    return '%s (%s), boundary %s' % \
      (self.name, '/'.join(self.path), self.boundary)

  # Compute the boundary of the geographic region of this division, based
  # on the points in the region.
  def compute_boundary(self):
    # Yield up all points that are not "outliers", where outliers are defined
    # as points that are more than max_dist_for_outliers away from all other
    # points.
    def yield_non_outliers():
      # If not enough points, just return them; otherwise too much possibility
      # that all of them, or some good ones, will be considered outliers.
      if len(self.points) <= 5:
        for p in self.points: yield p
        return
      for p in self.points: yield p
      #for p in self.points:
      #  # Find minimum distance to all other points and check it.
      #  mindist = min(spheredist(p, x) for x in self.points if x is not p)
      #  if mindist <= max_dist_for_outliers: yield p

    if debug > 1:
      uniprint("Computing boundary for %s, path %s, num points %s" %
               (self.name, self.path, len(self.points)))
               
    self.goodpoints = list(yield_non_outliers())
    # If we've somehow discarded all points, just use the original list
    if not len(self.goodpoints):
      if debug > 0:
        warning("All points considered outliers?  Division %s, path %s" %
                (self.name, self.path))
      self.goodpoints = self.points
    topleft = Coord(min(x.lat for x in self.goodpoints),
                    min(x.long for x in self.goodpoints))
    botright = Coord(max(x.lat for x in self.goodpoints),
                     max(x.long for x in self.goodpoints))
    self.boundary = Boundary(topleft, botright)

  def __contains__(self, coord):
    return coord in self.boundary

  # Note that a location was seen with the given coordinate and path to
  # the location.  Return the corresponding Division.
  @staticmethod
  def note_point_seen_in_division(coord, path):
    higherdiv = None
    if len(path) > 1:
      # Also note location in next-higher division.
      higherdiv = Division.note_point_seen_in_division(coord, path[0:-1])
    # Skip divisions where last element in path is empty; this is a
    # reference to a higher-level division with no corresponding lower-level
    # division.
    if not path[-1]: return higherdiv
    if path in path_to_division:
      division = path_to_division[path]
    else:
      # If we haven't seen this path, create a new Division object.
      # Record the mapping from path to division, and also from the
      # division's "name" (name of lowest-level division in path) to
      # the division.
      division = Division(path)
      division.div = higherdiv
      path_to_division[path] = division
      toponym_to_division[path[-1].lower()] += [division]
    division.points += [coord]
    return division

# A 2-dimensional coordinate.
#
# The following fields are defined:
#
#   lat, long: Latitude and longitude of coordinate.

class Coord(object):
  def __init__(self, lat, long):
    self.lat = lat
    self.long = long

  def __str__(self):
    return '(%s,%s)' % (self.lat, self.long)

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
# coordinates.

def spheredist(p1, p2):
  thisRadLat = (p1.lat / 180.) * math.pi
  thisRadLong = (p1.long / 180.) * math.pi
  otherRadLat = (p2.lat / 180.) * math.pi
  otherRadLong = (p2.long / 180.) * math.pi
        
  # 3963.191 = Earth radius in miles

  anglecos = (math.sin(thisRadLat)*math.sin(otherRadLat)
              + math.cos(thisRadLat)*math.cos(otherRadLat)*
                math.cos(otherRadLong-thisRadLong))
  # If the values are extremely close to each other, the resulting cosine
  # value will be extremely close to 1.  In reality, however, if the values
  # are too close (e.g. the same), the computed cosine will be slightly
  # above 1, and acos() will complain.  So special-case this.
  if abs(anglecos) > 1.0:
    if abs(anglecos) > 1.000001:
      warning("Something wrong in computation of spherical distance, out-of-range cosine value %f" % anglecos)
      return 1000000.
    else:
      return 0.
  return 3963.191 * math.acos(anglecos)

#######################################################################
#                            Process files                            #
#######################################################################

# Read in the list of stopwords from the given filename.
def read_stopwords(filename):
  for line in uchompopen(filename):
    stopwords.add(line)

# Parse the result of a previous run of --coords-counts and generate
# a unigram distribution for Naive Bayes matching.  We do a simple version
# of Good-Turing smoothing where we assign probability mass to unknown
# words equal to the probability mass of all words seen once, and rescale
# the remaining probabilities accordingly.

def construct_naive_bayes_dist(filename):
  articles_seen = 0
  for line in uchompopen(filename):
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

# Parse the result of a previous run of --coords-counts for articles with
# coordinates
def get_coordinates(filename):
  for line in uchompopen(filename):
    if rematch('Article title: (.*)$', line):
      title = m_[1]
    elif rematch('Article coordinates: (.*),(.*)$', line):
      article_coordinates[title] = Coord(safe_float(m_[1]), safe_float(m_[2]))
      short = title
      if rematch('(.*?), (.*)$', title):
        short = m_[1]
        tuple = (m_[1], m_[2])
        tuple_to_full_name[tuple] = title
      short_to_full_name[short] += [title]

# Find Wikipedia article matching name NAME for location LOC.  NAME
# will generally be one of the names of LOC (either its canonical
# name or one of the alternate name).  CHECK_MATCH is a function that
# is passed two aruments, the location and the Wikipedia artile name,
# and should return True if the location matches the article.
# PREFER_MATCH is used when two or more articles match.  It is passed
# three argument, the location and two Wikipedia article names.  It
# should return TRUE if the first is to be preferred to the second.
# Return the name of the article matched, or None.

def find_one_wikipedia_match(loc, name, check_match, prefer_match):

  # See if there is an article with the same name as the location
  if name in article_coordinates:
    if check_match(loc, name): return name

  # Check whether there is a match for an article whose name is
  # a combination of the location's name and one of the divisions that
  # the location is in (e.g. "Augusta, Georgia" for a location named
  # "Augusta" in a second-level division "Georgia").
  if loc.div:
    for div in loc.div.path:
      artname = tuple_to_full_name.get((name, div), None)
      if artname:
        if check_match(loc, artname): return artname

  # See if there is a match with any of the articles whose short
  # name is the same as the location's name
  artnames = short_to_full_name.get(name, None)
  if artnames:
    goodarts = [artname for artname in artnames if check_match(loc, artname)]
    if len(goodarts) == 1:
      return goodarts[0] # One match
    elif len(goodarts) > 1:
      # Multiple matches: Sort by preference, return most preferred one
      if debug > 1:
        print("Warning: Saw %s short-name matches: %s" %
              (len(goodarts), goodarts))
      sortedarts = \
        sorted(goodarts, cmp=(lambda x,y:1 if prefer_match(loc, x,y) else -1),
               reverse=True)
      return sortedarts[0]

  # No match.
  return None

# Find Wikipedia article matching location LOC.  CHECK_MATCH and
# PREFER_MATCH are as above.  Return the name of the article matched, or None.

def find_wikipedia_match(loc, check_match, prefer_match):
  # Try to find a match for the canonical name of the location
  match = find_one_wikipedia_match(loc, loc.name, check_match, prefer_match)
  if match: return match

  # No match; try each of the alternate names in turn.
  for altname in loc.altnames:
    match = find_one_wikipedia_match(loc, altname, check_match, prefer_match)
    if match: return match

  # No match.
  return None

def find_match_for_locality(loc, maxdist):
  # Check whether the given location matches the specified Wikipedia
  # article by seeing if the distance away is at most MAXDIST.

  def check_match(loc, artname):
    artcoord = article_coordinates[artname]
    dist = spheredist(loc.coord, artcoord)
    if dist <= maxdist:
      return True
    else:
      if debug > 1:
        uniprint("Found location %s with coordinates %s but dist %s > %s" %
                 (artname, artcoord, dist, maxdist))
      return False

  def prefer_match(loc, art1, art2):
    return spheredist(loc.coord, article_coordinates[art1]) < \
      spheredist(loc.coord, article_coordinates[art2])

  return find_wikipedia_match(loc, check_match, prefer_match)

def find_match_for_division(loc):
  # Check whether the given location matches the specified Wikipedia
  # article by seeing if the distance away is at most MAXDIST.

  def check_match(loc, artname):
    artcoord = article_coordinates[artname]
    if artcoord in loc:
      return True
    else:
      if debug > 1:
        uniprint("Found article %s with coordinates %s but not in location named %s, path %s" %
                 (artname, artcoord, loc.name, loc.path))
      return False

  def prefer_match(loc, art1, art2):
    # Prefer according to incoming link counts, if that info is available
    if art1 in incoming_link_count and art2 in incoming_link_count:
      return incoming_link_count[art1] > incoming_link_count[art2]
    else:
      # FIXME: Do something smart here -- maybe check that location is farther
      # in the middle of the bounding box (does this even make sense???)
      return True

  return find_wikipedia_match(loc, check_match, prefer_match)

# Find the Wikipedia article matching an entry in the gazetteer.
# The format of an entry is
#
# ID  NAME  ALTNAMES  ORIG-SCRIPT-NAME  TYPE  POPULATION  LAT  LONG  DIV1  DIV2  DIV3
#
# where there is a tab character separating each field.  Fields may be empty;
# but there will still be a tab character separating the field from others.
#
# The ALTNAMES specify any alternative names of the location, often including
# the equivalent of the original name without any accent characters.  If
# there is more than one alternative name, the possibilities are separated
# by a comma and a space, e.g. "Dongshi, Dongshih, Tungshih".  The
# ORIG-SCRIPT-NAME is the name in its original script, if that script is not
# Latin characters (e.g. names in Russia will be in Cyrillic). (For some
# reason, names in Chinese characters are listed in the ALTNAMES rather than
# the ORIG-SCRIPT-NAME.)
#
# LAT and LONG specify the latitude and longitude, respectively.  These are
# given as integer values, where the actual value is found by dividing this
# integer value by 100.
#
# DIV1, DIV2 and DIV3 specify different-level divisions that a location is
# within, from largest to smallest.  Typically the largest is a country.
# For locations in the U.S., the next two levels will be state and county,
# respectively.  Note that such divisions also have corresponding entries
# in the gazetteer.  However, these entries are somewhat lacking in that
# (1) no coordinates are given, and (2) only the top-level division (the
# country) is given, even for third-level divisions (e.g. counties in the
# U.S.).

def match_world_gazetteer_entry(line):
  # Split on tabs, make sure at least 11 fields present and strip off
  # extra whitespace
  fields = re.split(r'\t', line.strip()) + ['']*11
  fields = [x.strip() for x in fields[0:11]]
  (id, name, altnames, orig_script_name, typ, population, lat, long,
   div1, div2, div3) = fields

  # Skip places without coordinates
  if not lat or not long:
    if debug > 1:
      uniprint("Skipping location %s (div %s/%s/%s) without coordinates" %
               (name, div1, div2, div3))
    return

  # Create and populate a Locality object
  loc = Locality(name, Coord(int(lat) / 100., int(long) / 100.))
  loc.type = typ
  if altnames:
    loc.altnames = re.split(', ', altnames)
  # Add the given location to the division the location is in
  loc.div = Division.note_point_seen_in_division(loc.coord, (div1, div2, div3))
  if debug > 1:
    uniprint("Saw location %s (div %s/%s/%s) with coordinates %s" %
             (loc.name, div1, div2, div3, loc.coord))

  # Record the location.  For each name for the location (its
  # canonical name and all alternates), add the location to the list of
  # locations associated with the name.  Record the name in lowercase
  # for ease in matching.
  for name in [loc.name] + loc.altnames:
    lowername = name.lower()
    if debug > 1:
      uniprint("Noting toponym_to_location for toponym %s, canonical name %s"
               % (name, loc.name))
    toponym_to_location[lowername] += [loc]

  # We start out looking for articles whose distance is no more
  # than max_dist_for_close_match, preferring articles whose name
  # is as close as possible to the name of the toponym.  Then we
  # steadily widen the match radius until we have considered the
  # entire earth.
  maxdist = max_dist_for_close_match
  # For points on the earth, we should never see a great-circle
  # distance greater than 12,500 miles or so, but set it much higher
  # just in case.
  while maxdist < 30000:
    match = find_match_for_locality(loc, maxdist)
    if match: break
    maxdist *= 2

  if not match: 
    if debug > 1:
      uniprint("Unmatched name %s" % loc.name)
    return
  
  # Record the match.
  loc.match = match
  if debug > 1:
    uniprint("Matched location %s (coord %s) with article %s (coord %s), dist=%s"
             % (loc.name, loc.coord, match, article_coordinates[match],
                spheredist(loc.coord, article_coordinates[match])))

# Read in the data from the World gazetteer and find the Wikipedia article
# matching each entry in the gazetteer.  The format of an entry is
def read_world_gazetteer_and_match(filename):

  # Match each entry in the gazetteer
  for line in uchompopen(filename):
    if debug > 1:
      uniprint("Processing line: %s" % line)
    match_world_gazetteer_entry(line)

  for division in path_to_division.itervalues():
    if debug > 1:
      uniprint("Processing division named %s, path %s"
               % (division.name, division.path))
    division.compute_boundary()
    match = find_match_for_division(division)
    if match:
      if debug > 1:
        uniprint("Matched article %s (coord %s) for division %s, path %s" %
                 (match, article_coordinates[match],
                  division.name, division.path))
      division.match = match
    else:
      if debug > 1:
        uniprint("Couldn't find match for division %s, path %s" %
                 (division.name, division.path))

def read_incoming_link_info(filename):
  articles_seen = 0
  for line in uchompopen(filename):
    if rematch('------------------ Count of incoming links: ------------',
               line): continue
    elif rematch('==========================================', line):
      return
    else:
      assert rematch('(.*) = ([0-9]+)$', line)
      incoming_link_count[m_[1]] = int(m_[2])
      articles_seen += 1
      if (articles_seen % 10000) == 0:
        print "Processed %d articles" % articles_seen

# Class of word in a CONLL file.  Fields:
#
#   word: The identity of the word.
#   isstop: True if it is a stopword.
#   coord: For a location with specified ground-truth coordinate, the
#          coordinate.  Else, none.
#   context: Vector including the word and 10 words on other side.
#
class ConllWord(object):
  def __init__(self, word):
    self.word = word
    self.isstop = word in stopwords
    self.coord = None
    self.context = None

def read_trconll_file(filename):
  results = []
  in_loc = False
  try:
    for line in uchompopen(filename):
      try:
        (word, ty) = re.split('\t', line, 1)
        if word:
          wordstruct = ConllWord(word)
          results.append(wordstruct)
        if in_loc and word:
          in_loc = False
        elif ty.startswith('LOC'):
          in_loc = True
          loc_word = wordstruct
        elif in_loc and ty[0] == '>':
          (off, gaz, lat, long, fulltop) = re.split('\t', ty, 4)
          lat = float(lat)
          long = float(long)
          loc_word.coord = Coord(lat, long)
          if debug > 0:
            uniprint("Saw loc %s with true coordinates %s" %
                     (loc_word.word, loc_word.coord))
      except Exception as exc:
        print "Bad line %s" % line
        print "Exception is %s" % exc
        if type(exc) is not ValueError:
          traceback.print_exc()
        return None
  except Exception as exc:
    print "Exception %s reading from file %s" % (exc, filename)
    traceback.print_exc()
    return None

  # Now compute context for words
  nbcl = naive_bayes_context_len
  for i in xrange(len(results)):
    pass
    # Select up to naive_bayes_context_len words on either side
    #results.context = [x.word for x in
    #                   results[max(0,i-nbcl) : min(len(results),i+nbcl+1)]]
  return results
  

# Process all files in DIR, calling FUN on each one (with the directory name
# joined to the name of each file in the directory).
def process_dir_files(dir, fun):
  for fname in os.listdir(dir):
    fullname = os.path.join(dir, fname)
    fun(fullname)
  
# Given a TR-CONLL file, find each toponym explicitly mentioned as such
# and disambiguate it (find the correct geographic location) using the
# "link baseline", i.e. use the location with the highest number of
# incoming links.
def disambiguate_link_baseline(fname):
  print "Processing TR-CONLL file %s..." % fname
  results = read_trconll_file(fname)
  # Return value will be None if error occurred
  if not results: return
  for conllword in results:
    toponym = conllword.word
    coord = conllword.coord
    if not coord: continue
    lowertop = toponym.lower()
    maxlinks = 0
    bestloc = None
    locs = toponym_to_location.get(lowertop, []) + \
           toponym_to_division.get(lowertop, [])
    if not locs:
      if debug > 0:
        uniprint("Unable to find any possibilities for %s" % toponym)
      correct = False
    else:
      if debug > 0:
        uniprint("Considering toponym %s, coordinates %s" %
                 (toponym, coord))
        uniprint("For toponym %s, %d possible locations" %
                 (toponym, len(locs)))
      for loc in locs:
        artname = loc.match
        if debug > 0:
          if type(loc) is Locality:
            uniprint("Considering location %s (Locality), coord %s, article %s"
                     % (loc.name, loc.coord, artname))
          else:
            uniprint("Considering location %s (Division), article %s"
                     % (loc.name, artname))
        if not artname:
          thislinks = 0
          if debug > 0:
            uniprint("--> Location without matching article")
        else:
          if artname not in incoming_link_count:
            thislinks = 0
            if debug > 0:
              warning("Strange, article (coord %s) has no link count" %
                      (article_coordinates[artname]))
          else:
            thislinks = incoming_link_count[artname]
            if debug > 0:
              uniprint("--> Coord %s, link count is %s" %
                       (article_coordinates[artname], thislinks))
        if thislinks > maxlinks:
          maxlinks = thislinks
          bestloc = loc
      if bestloc:
        if type(bestloc) is Locality:
          dist = spheredist(coord, bestloc.coord)
          correct = dist <= max_dist_for_close_match
        else:
          correct = coord in bestloc
      else:
        # If we couldn't find link counts, this can happen
        correct = False
    if correct:
      global correct_toponyms_disambiguated
      correct_toponyms_disambiguated += 1
    global total_toponyms_disambiguated
    total_toponyms_disambiguated += 1
    if debug > 0 and bestloc:
      if type(bestloc) is Locality:
        uniprint("Best match = %s, link count = %s, coordinates %s, dist %s, correct %s"
                 % (bestloc.match, maxlinks, bestloc.coord, dist, correct))
      else:
        uniprint("Best match = %s, link count = %s, correct %s" %
                 (bestloc.match, maxlinks, correct))

# Given a TR-CONLL file, find each toponym explicitly mentioned as such
# and disambiguate it (find the correct geographic location) using
# Naive Bayes.
#def disambiguate_naive_bayes(fname):
#  print "Processing TR-CONLL file %s..." % fname
#  results = read_trconll_file(fname)
#  for conllword in results:
#    toponym = conllword.word
#    coord = conllword.coord
#    if not coord: continue
#    lowertop = toponym.lower()
#    maxlinks = 0
#    bestloc = None
#    locs = toponym_to_location.get(lowertop, []) + \
#           toponym_to_division.get(lowertop, [])
#    if not locs:
#      if debug > 0:
#        uniprint("Unable to find any possibilities for %s" % toponym)
#      correct = False
#    else:
#      if debug > 0:
#        uniprint("Considering toponym %s, coordinates %s" %
#                 (toponym, coord))
#        uniprint("For toponym %s, %d possible locations" %
#                 (toponym, len(locs)))
#      for loc in locs:
#        artname = loc.match
#        if debug > 0:
#          if type(loc) is Locality:
#            uniprint("Considering location %s (Locality), coord %s, article %s"
#                     % (loc.name, loc.coord, artname))
#          else:
#            uniprint("Considering location %s (Division), article %s"
#                     % (loc.name, artname))
#        if not artname:
#          thislinks = 0
#          if debug > 0:
#            uniprint("--> Location without matching article")
#        else:
#          if artname not in incoming_link_count:
#            thislinks = 0
#            if debug > 0:
#              warning("Strange, article (coord %s) has no link count" %
#                      (article_coordinates[artname]))
#          else:
#            thislinks = incoming_link_count[artname]
#            if debug > 0:
#              uniprint("--> Coord %s, link count is %s" %
#                       (article_coordinates[artname], thislinks))
#        if thislinks > maxlinks:
#          maxlinks = thislinks
#          bestloc = loc
#      if bestloc:
#        if type(bestloc) is Locality:
#          dist = spheredist(coord, bestloc.coord)
#          correct = dist <= max_dist_for_close_match
#        else:
#          correct = coord in bestloc
#      else:
#        # If we couldn't find link counts, this can happen
#        correct = False
#    if correct:
#      global correct_toponyms_disambiguated
#      correct_toponyms_disambiguated += 1
#    global total_toponyms_disambiguated
#    total_toponyms_disambiguated += 1
#    if debug > 0 and bestloc:
#      if type(bestloc) is Locality:
#        uniprint("Best match = %s, link count = %s, coordinates %s, dist %s, correct %s"
#                 % (bestloc.match, maxlinks, bestloc.coord, dist, correct))
#      else:
#        uniprint("Best match = %s, link count = %s, correct %s" %
#                 (bestloc.match, maxlinks, correct))

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
