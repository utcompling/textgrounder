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
import cPickle
import itertools
import gc
from optparse import OptionParser
from textutil import *

# FIXME:
#
# -- If coords and 0,9999, they are inaccurate, ignore them
# -- Add flags for lower-case-words, etc.

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

# Map from short name to list of Wikipedia articles.  The short name for an
# article is computed from the article's name.  If the article name has a
# comma, the short name is the part before the comma, e.g. the short name of
# "Springfield, Ohio" is "Springfield".  If the name has no comma, the short
# name is the same as the article name.  The idea is that the short name
# should be the same as one of the toponyms used to refer to the article.
short_name_to_articles = listdict()

# Map from tuple (NAME, DIV) for Wikipedia articles of the form
# "Springfield, Ohio".
name_div_to_article = {}

# List of stopwords
stopwords = set()

# Map of redirects
redirect_map = {}

# Total number of word types seen (size of vocabulary)
num_word_types = 0

# Total number of word tokens seen
num_word_tokens = 0

# Total number of types seen once
num_types_seen_once = 0

# Estimate of number of unseen word types for all articles
num_unseen_word_types = 0

# Overall probabilities over all articles of seeing a word in an article,
# for all words seen at least once in any article, computed using the
# empirical frequency of a word among all articles, adjusted by the mass
# to be assigned to globally unseen words (words never seen at all), i.e. the
# value in 'globally_unseen_word_prob'.
overall_word_probs = intdict()

# The total probability mass to be assigned to words not seen at all in
# any article, estimated using Good-Turing smoothing as the unadjusted
# empirical probability of having seen a word once.
globally_unseen_word_prob = 0.0

# For articles whose word counts are not known, use an empty list to
# look up in.
unknown_article_counts = ([], [])

max_articles_to_count = None

# For each toponym (name of location), value is a list of Locality items,
# listing gazetteer locations and corresponding matching Wikipedia articles.
toponym_to_location = listdict()

# For each toponym corresponding to a division higher than a locality,
# list of divisions with this name.
toponym_to_division = listdict()

# For each division, map from division's path to Division object.
path_to_division = {}

# Debug level; if non-zero, output lots of extra information about how
# things are progressing.  If > 1, even more info.
debug = 0

# If true, print out warnings about strangely formatted input
show_warnings = True

# Do we ignore stopwords when computing word distributions?
ignore_stopwords_in_article_dists = False

# If true, ignore case when creating distributions and looking up words
ignore_case_words = True

# Maximum number of miles allowed when looking for a close match
max_dist_for_close_match = 20

# Maximum number of miles allowed between a point and any others in a
# division.  Points farther away than this are ignored as "outliers"
# (possible errors, etc.).
max_dist_for_outliers = 200

# Number of words on either side used for context
naive_bayes_context_len = 10

# Size of each region in degrees.  Determined by the --region-size option
# (however, that option is expressed in miles).
degrees_per_region = 0.0

# Number of tiling regions on a side of a Naive Bayes region
width_of_nbregion = None

# Minimum, maximum latitude/longitude, directly and in indices
# (integers used to index the set of regions that tile the earth)
minimum_latitude = -90.0
maximum_latitude = 90.0
minimum_longitude = -180.0
maximum_longitude = 179.99999
minimum_latind = None
maximum_latind = None
minimum_longind = None
maximum_longind = None

# Radius of the earth in miles.  Used to compute spherical distance in miles,
# and miles per degree of latitude/longitude.
earth_radius_in_miles = 3963.191

# Number of miles per degree, at the equator.  For longitude, this is the
# same everywhere, but for latitude it is proportional to the degrees away
# from the equator.
miles_per_degree = math.pi * 2 * earth_radius_in_miles / 360.

# Type of Naive Bayes disambiguation to be done (article, square-region,
# round-region): from --naive-bayes-type
naive_bayes_type = None

# Mapping of region->locations in region, for region-based Naive Bayes
# disambiguation.  The key is a tuple expressing the integer indices of the
# latitude and longitude of the southwest corner of the region. (Basically,
# given an index, the latitude or longitude of the southwest corner is
# index*degrees_per_region, and the region includes all locations whose
# latitude or longitude is in the half-open interval
# [index*degrees_per_region, (index+1)*degrees_per_region).
#
# We don't just create an array because we expect many regions to have no
# locations in them, esp. as we decrease the region size.  The idea is that
# the regions provide a first approximation to the regions used to create the
# article distributions.
region_to_locations = listdict()

# Mapping from center of Naive Bayes region to corresponding region object.
# A "Naive Bayes region" is a square of four tiling regions.
corner_to_nbregion = {}

# Table of all toponyms seen in evaluation files, along with how many times
# seen.  Used to determine when caching of certain toponym-specific values
# should be done.
toponyms_seen_in_eval_files = intdict()

############################################################################
#                             Utility functions                            #
############################################################################

def warning(text):
  '''Output a warning, formatting into UTF-8 as necessary'''
  if show_warnings:
    uniprint("Warning: %s" % text, sys.stderr)

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

############################################################################
#                                Evaluation                                #
############################################################################

class Eval(object):
  # Statistics on the types of toponyms processed
  # Total number of toponyms
  total_toponyms = 0
  correct_toponyms = 0
  incorrect_toponyms = 0

  # Toponyms by number of candidates available
  total_toponyms_by_num_candidates = intdict()
  correct_toponyms_by_num_candidates = intdict()
  incorrect_toponyms_by_num_candidates = intdict()
  # Toponyms by number of candidates with associated Wikipedia articles
  total_toponyms_by_num_article_candidates = intdict()
  correct_toponyms_by_num_article_candidates = intdict()
  incorrect_toponyms_by_num_article_candidates = intdict()

  incorrect_values = [
    ('incorrect_with_no_candidates',
     'Incorrect, with no candidates'),
    ('incorrect_with_no_article_candidates',
     'Incorrect, with candidates but none with matching articles'),
    ('incorrect_with_no_correct_candidates',
     'Incorrect, with article candidates but no correct candidates'),
    ('incorrect_with_correct_candidates_but_no_correct_article_candidates',
     'Incorrect, with article candidates, correct candidates but none with articles'),
    ('incorrect_with_multiple_correct_article_candidates',
     'Incorrect, with article candidates, with multiple correct article candidates'),
    ('incorrect_one_correct_article_candidate_but_article_coord_doesnt_match_location',
     "Incorrect, with article candidates, one correct but article coordinates don't match location"),
    ('incorrect_one_good_correct_article_candidate',
     'Incorrect, with article candidates, one good correct article candidate'),
  ]

  for (val, descr) in incorrect_values:
    exec "%s = 0" % val

  @staticmethod
  def output():
    tot = Eval.total_toponyms
    print ("Percent correct = %s/%s = %5.2f" %
           (Eval.correct_toponyms, tot, 100*float(Eval.correct_toponyms)/tot))
    print ("Percent incorrect = %s/%s = %5.2f" %
           (Eval.incorrect_toponyms, tot,
            100*float(Eval.incorrect_toponyms)/tot))
    for (val, descr) in Eval.incorrect_values:
      print ("  %s = %s/%s = %5.2f" %
        (descr, getattr(Eval, val), tot, 100*float(getattr(Eval, val))/tot))


############################################################################
#                                Coordinates                               #
############################################################################

# A class holding the boundary of a geographic object.  Currently this is
# just a bounding box, but eventually may be expanded to including a
# convex hull or more complex model.

class Boundary(object):
  __slots__ = ['botleft', 'topright']

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

# A 2-dimensional coordinate.
#
# The following fields are defined:
#
#   lat, long: Latitude and longitude of coordinate.

class Coord(object):
  __slots__ = ['lat', 'long']

  def __init__(self, lat, long):
    self.lat = lat
    self.long = long

  def __str__(self):
    return '(%s,%s)' % (self.lat, self.long)

# Compute spherical distance in miles (along a great circle) between two
# coordinates.

def spheredist(p1, p2):
  if not p1 or not p2: return 1000000.
  thisRadLat = (p1.lat / 180.) * math.pi
  thisRadLong = (p1.long / 180.) * math.pi
  otherRadLat = (p2.lat / 180.) * math.pi
  otherRadLong = (p2.long / 180.) * math.pi
        
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
  return earth_radius_in_miles * math.acos(anglecos)

def coord_to_region_indices(coord):
  latind = int(math.floor(coord.lat / degrees_per_region))
  longind = int(math.floor(coord.long / degrees_per_region))
  return (latind, longind)

def region_indices_to_coord(latind, longind):
  return Coord(latind * degrees_per_region, longind * degrees_per_region)

############################################################################
#                           Geographic locations                           #
############################################################################

# Distribution used for Naive Bayes disambiguation.  The following
# fields are defined: 
#
#   finished: Whether we have finished computing the distribution over all
#             articles to be added.
#   locs: List of locations whose articles are used in computing the
#         distribution.
#   num_arts: Total number of articles included.
#   incoming_links: Total number of incoming links, or None if unknown.
#   counts: During accumulation, a hash table of (word, count) items,
#           specifying the count of each word that has been seen at least
#           once in any article making up the distribution.  After
#           all articles have been added, converted to a "sorted list"
#           (tuple of sorted keys and values) of words and counts.
#   unseen_mass: Total probability mass to be assigned to all words not
#                seen in any articles, estimated using Good-Turing smoothing
#                as the unadjusted empirical probability of having seen a
#                word once.
#   total_tokens: Total number of tokens in all articles
#   num_types_seen_once: Number of word types seen only once across
#                        all articles.
#   overall_unseen_mass:
#     Probability mass assigned in 'overall_word_probs' to all words not seen
#     in any article.  This is 1 - (sum over seen W of overall_word_probs[W]).

class NBDist(object):
  __slots__ = ['finished', 'locs', 'num_arts', 'incoming_links', 'counts',
               'unseen_mass', 'total_tokens', 'num_types_seen_once',
               'overall_unseen_mass']

  def __init__(self):
    self.finished = False
    self.locs = []
    self.num_arts = 0
    self.incoming_links = 0
    self.counts = intdict()
    self.unseen_mass = 1.0
    self.total_tokens = 0
    self.num_types_seen_once = 0
    self.overall_unseen_mass = 1.0

  # Add the given locations to the total distribution seen so far
  def add_locations(self, locs):
    self.locs += locs
    num_arts = sum(1 for loc in locs if loc.match)
    total_tokens = 0
    incoming_links = 0
    if debug > 0:
      uniprint("Naive Bayes dist, number of articles = %s" % num_arts)
    counts = self.counts
    for loc in locs:
      art = loc.match
      if not art or not art.finished: continue
      for (word,count) in itertools.izip(art.counts[0], art.counts[1]):
        counts[word] += count
      total_tokens += art.total_tokens
      if art.incoming_links: # Might be None, for unknown link count
        incoming_links += art.incoming_links
    self.num_arts += num_arts
    self.total_tokens += total_tokens
    self.incoming_links += incoming_links
    if debug > 0:
      uniprint("""--> Finished processing, number articles seen = %s/%s,
    total tokens = %s/%s, incoming links = %s/%s""" %
               (num_arts, self.num_arts, total_tokens, self.total_tokens,
                incoming_links, self.incoming_links))

  def finish_dist(self):
    self.num_types_seen_once = \
      sum(1 for word in self.counts if self.counts[word] == 1)
    overall_seen_mass = 0.0
    for word in self.counts:
      overall_seen_mass += overall_word_probs[word]
    self.overall_unseen_mass = 1.0 - overall_seen_mass
    if self.total_tokens > 0:
      # If no words seen only once, we will have a problem if we assign 0
      # to the unseen mass, as unseen words will end up with 0 probability.
      self.unseen_mass = \
        float(max(1, self.num_types_seen_once))/self.total_tokens
    else:
      self.unseen_mass = 1.0
    self.counts = make_sorted_list(self.counts)
    self.finished = True

    if debug > 0:
      uniprint("""For Naive Bayes dist, num articles = %s, total tokens = %s,
    unseen_mass = %s, types seen once = %s, incoming links = %s,
    overall unseen mass = %s""" %
               (self.num_arts, self.total_tokens, self.unseen_mass,
                self.num_types_seen_once, self.incoming_links,
                self.overall_unseen_mass))

############ Naive Bayes regions ############

# Info used in region-based Naive Bayes disambiguation.  This class contains
# values used in computing the distribution over all locations in the
# region surrounding the locality in question.  The region is currently a
# square of 4 tiling regions.  The following fields are defined: 
#
#   latind, longind: Region indices of southwest-most tiling region in
#                    Naive Bayes region.
#   nbdist: Distribution corresponding to region.

class NBRegion(object):
  __slots__ = ['latind', 'longind', 'nbdist']

  def __init__(self, latind, longind):
    self.latind = latind
    self.longind = longind
    self.nbdist = NBDist()

  # Generate the distribution for a Naive Bayes region from the tiling regions.
  def generate_dist(self):

    nblat = self.latind
    nblong = self.longind

    if debug > 0:
      uniprint("Generating distribution for Naive Bayes region centered at %s"
               % region_indices_to_coord(nblat, nblong))

    # Accumulate counts for the given region
    def process_one_region(latind, longind):
      locs = region_to_locations.get((latind, longind), None)
      if not locs:
        return
      if debug > 0:
        uniprint("--> Processing tiling region %s" %
                 region_indices_to_coord(latind, longind))
      self.nbdist.add_locations(locs)

    # Process the tiling regions making up the Naive Bayes region;
    # but be careful around the edges.
    for i in range(nblat, nblat + width_of_nbregion):
      for j in range(nblong, nblong + width_of_nbregion):
        jj = j
        if jj > maximum_longind: jj = minimum_longind
        process_one_region(i, jj)

    self.nbdist.finish_dist()

# Determine the Naive Bayes distribution object for a given locality.
# Create and populate one if necessary.
def find_nbdist(loc):
  if type(loc) is Division:
    if not loc.nbdist:
      loc.generate_nbdist()
    return loc.nbdist
  nbreg = loc.nbregion
  if nbreg: return nbreg.nbdist

  # When width_of_nbregion = 1, don't subtract anything.
  # When width_of_nbregion = 2, subtract 0.5*degrees_per_region.
  # When width_of_nbregion = 3, subtract degrees_per_region.
  # When width_of_nbregion = 4, subtract 1.5*degrees_per_region.
  # In general, subtract (width_of_nbregion-1)/2.0*degrees_per_region.

  # Compute the indices of the southwest region
  subval = (width_of_nbregion-1)/2.0*degrees_per_region
  lat = loc.coord.lat - subval
  long = loc.coord.long - subval
  if lat < minimum_latitude: lat = minimum_latitude
  if long < minimum_longitude: long += 360.
  
  latind, longind = coord_to_region_indices(Coord(lat, long))
  nbreg = corner_to_nbregion.get((latind, longind), None)
  if not nbreg:
    nbreg = NBRegion(latind, longind)
    nbreg.generate_dist()
    corner_to_nbregion[(latind, longind)] = nbreg
  loc.nbregion = nbreg
  return nbreg.nbdist

############ Locations ############

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
  __slots__ = ['name', 'altnames', 'type', 'match', 'div']
  pass

# A location corresponding to an entry in a gazetteer, with a single
# coordinate.
#
# The following fields are defined, in addition to those for Location:
#
#   coord: Coordinates of the location, as a Coord object.
#   nbregion: The Naive Bayes region surrounding this location, including
#             all necessary information to determine the region-based
#             distribution.

class Locality(Location):
  # This is an optimization that causes space to be allocated in the most
  # efficient possible way for exactly these attributes, and no others.

  __slots__ = Location.__slots__ + ['coord', 'nbregion']

  def __init__(self, name, coord):
    self.name = name
    self.coord = coord
    self.altnames = []
    self.match = None
    self.nbregion = None

  def __str__(self):
    return 'Locality %s (%s) at %s, match=%s' % \
      (self.name, self.div and '/'.join(self.div.path), self.coord, self.match)

  def distance_to_coord(self, coord):
    return spheredist(self.coord, coord)

  def matches_coord(self, coord):
    return self.distance_to_coord(coord) <= max_dist_for_close_match


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
#   locs: List of locations inside of the division.
#   goodlocs: List of locations inside of the division other than those
#             rejected as outliers (too far from all other locations).
#   boundary: A Boundary object specifying the boundary of the area of the
#             division.  Currently in the form of a rectangular bounding box.
#             Eventually may contain a convex hull or even more complex
#             region (e.g. set of convex regions).
#   nbdist: For region-based Naive Bayes disambiguation, a distribution
#           over the division's article and all locations within the region.

class Division(object):
  __slots__ = Location.__slots__ + \
    ['level', 'path', 'locs', 'goodlocs', 'boundary', 'nbdist']

  def __init__(self, path):
    self.name = path[-1]
    self.altnames = []
    self.path = path
    self.level = len(path)
    self.locs = []
    self.match = None
    self.nbdist = None

  def __str__(self):
    return 'Division %s (%s), match=%s, boundary=%s' % \
      (self.name, '/'.join(self.path), self.match, self.boundary)

  def distance_to_coord(self, coord):
    return "Unknown"

  def matches_coord(self, coord):
    return coord in self

  # Compute the boundary of the geographic region of this division, based
  # on the points in the region.
  def compute_boundary(self):
    # Yield up all points that are not "outliers", where outliers are defined
    # as points that are more than max_dist_for_outliers away from all other
    # points.
    def yield_non_outliers():
      # If not enough points, just return them; otherwise too much possibility
      # that all of them, or some good ones, will be considered outliers.
      if len(self.locs) <= 5:
        for p in self.locs: yield p
        return
      for p in self.locs: yield p
      #for p in self.locs:
      #  # Find minimum distance to all other points and check it.
      #  mindist = min(spheredist(p, x) for x in self.locs if x is not p)
      #  if mindist <= max_dist_for_outliers: yield p

    if debug > 1:
      uniprint("Computing boundary for %s, path %s, num points %s" %
               (self.name, self.path, len(self.locs)))
               
    self.goodlocs = list(yield_non_outliers())
    # If we've somehow discarded all points, just use the original list
    if not len(self.goodlocs):
      if debug > 0:
        warning("All points considered outliers?  Division %s, path %s" %
                (self.name, self.path))
      self.goodlocs = self.locs
    topleft = Coord(min(x.coord.lat for x in self.goodlocs),
                    min(x.coord.long for x in self.goodlocs))
    botright = Coord(max(x.coord.lat for x in self.goodlocs),
                     max(x.coord.long for x in self.goodlocs))
    self.boundary = Boundary(topleft, botright)

  def generate_nbdist(self):
    self.nbdist = NBDist()
    self.nbdist.add_locations([self])
    self.nbdist.add_locations(self.goodlocs)
    self.nbdist.finish_dist()

  def __contains__(self, coord):
    return coord in self.boundary

  # Note that a location was seen with the given path to the location.
  # Return the corresponding Division.
  @staticmethod
  def note_point_seen_in_division(loc, path):
    higherdiv = None
    if len(path) > 1:
      # Also note location in next-higher division.
      higherdiv = Division.note_point_seen_in_division(loc, path[0:-1])
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
    division.locs += [loc]
    return division

############################################################################
#                             Wikipedia articles                           #
############################################################################

# A Wikipedia article.  Defined fields:
#
#   name: Name of article.
#   coord: Coordinates of article.
#   incoming_links: Number of incoming links, or None if unknown.
#   counts: A "sorted list" (tuple of sorted keys and values) of
#           (word, count) items, specifying the counts of all words seen
#           at least once.
#   finished: Whether we have finished computing the distribution in
#             'counts'.
#   unseen_mass: Total probability mass to be assigned to all words not
#                seen in the article, estimated using Good-Turing smoothing
#                as the unadjusted empirical probability of having seen a
#                word once.
#   overall_unseen_mass:
#     Probability mass assigned in 'overall_word_probs' to all words not seen
#     in the article.  This is 1 - (sum over W in A of overall_word_probs[W]).
#     The idea is that we compute the probability of seeing a word W in
#     article A as
#
#     -- if W has been seen before in A, use the following:
#          COUNTS[W]/TOTAL_TOKENS*(1 - UNSEEN_MASS)
#     -- else, if W seen in any articles (W in 'overall_word_probs'),
#        use UNSEEN_MASS * (overall_word_probs[W] / OVERALL_UNSEEN_MASS).
#        The idea is that overall_word_probs[W] / OVERALL_UNSEEN_MASS is
#        an estimate of p(W | W not in A).  We have to divide by
#        OVERALL_UNSEEN_MASS to make these probabilities be normalized
#        properly.  We scale p(W | W not in A) by the total probability mass
#        we have available for all words not seen in A.
#     -- else, use UNSEEN_MASS * globally_unseen_word_prob / NUM_UNSEEN_WORDS,
#        where NUM_UNSEEN_WORDS is an estimate of the total number of words
#        "exist" but haven't been seen in any articles.  One simple idea is
#        to use the number of words seen once in any article.  This certainly
#        underestimates this number of not too many articles have been seen
#        but might be OK if many articles seen.
#   total_tokens: Total number of word tokens seen
#   words_seen_once: List of words seen once, for computing the value of
#     unseen_mass for a combination of multiple articles.

class Article(object):
  __slots__ = ['name', 'coord', 'incoming_links', 'counts', 'finished',
               'unseen_mass', 'overall_unseen_mass', 'total_tokens',
               'words_seen_once']

  def __init__(self, name):
    self.name = name
    self.coord = None
    self.incoming_links = None
    self.counts = None
    self.finished = False
    self.unseen_mass = None
    self.overall_unseen_mass = None
    self.total_tokens = None
    self.words_seen_once = None
    short = name
    if rematch('(.*?), (.*)$', name):
      short = m_[1]
      tuple = (m_[1], m_[2])
      name_div_to_article[tuple] = self
    elif rematch('(.*) \(.*\)$', name):
      short = m_[1]
    short_name_to_articles[short] += [self]

  # Copy properties (but not name) from another article
  def copy_from(self, art):
    self.coord = art.coord
    self.incoming_links = art.incoming_links
    self.counts = art.counts
    self.finished = art.finished
    self.unseen_mass = art.unseen_mass
    self.overall_unseen_mass = art.overall_unseen_mass
    self.total_tokens = art.total_tokens
    self.words_seen_once = art.words_seen_once

  # Return the article with the given name; if none exists, create a new
  # article with that name.
  @staticmethod
  def find_article_create(name):
    art = name_to_article.get(name, None)
    if not art:
      art = Article(name)
      name_to_article[name] = art
    return art

  def __str__(self):
    return '%s%s' % (self.name, self.coord)

# Mapping from article names to Article objects
name_to_article = {}

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
  art = name_to_article.get(name, None)
  if art:
    if check_match(loc, art): return art

  # Check whether there is a match for an article whose name is
  # a combination of the location's name and one of the divisions that
  # the location is in (e.g. "Augusta, Georgia" for a location named
  # "Augusta" in a second-level division "Georgia").
  if loc.div:
    for div in loc.div.path:
      art = name_div_to_article.get((name, div), None)
      if art:
        if check_match(loc, art): return art

  # See if there is a match with any of the articles whose short name
  # is the same as the location's name
  arts = short_name_to_articles.get(name, None)
  if arts:
    goodarts = [art for art in arts if check_match(loc, art)]
    if len(goodarts) == 1:
      return goodarts[0] # One match
    elif len(goodarts) > 1:
      # Multiple matches: Sort by preference, return most preferred one
      if debug > 1:
        uniprint("Warning: Saw %s toponym matches: %s" %
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

  def check_match(loc, art):
    dist = spheredist(loc.coord, art.coord)
    if dist <= maxdist:
      return True
    else:
      if debug > 1:
        uniprint("Found article %s but dist %s > %s" %
                 (art, dist, maxdist))
      return False

  def prefer_match(loc, art1, art2):
    return spheredist(loc.coord, art1.coord) < \
      spheredist(loc.coord, art2.coord)

  return find_wikipedia_match(loc, check_match, prefer_match)

def find_match_for_division(loc):
  # Check whether the given location matches the specified Wikipedia
  # article by seeing if the distance away is at most MAXDIST.

  def check_match(loc, art):
    if art.coord in loc:
      return True
    else:
      if debug > 1:
        uniprint("Found article %s but not in location named %s, path %s" %
                 (art, loc.name, loc.path))
      return False

  def prefer_match(loc, art1, art2):
    l1 = art1.incoming_links
    l2 = art2.incoming_links
    # Prefer according to incoming link counts, if that info is available
    if l1 != None and l2 != None:
      return l1 > l2
    else:
      # FIXME: Do something smart here -- maybe check that location is farther
      # in the middle of the bounding box (does this even make sense???)
      return True

  return find_wikipedia_match(loc, check_match, prefer_match)

############################################################################
#                               Process files                              #
############################################################################

# Read in the list of stopwords from the given filename.
def read_stopwords(filename):
  for line in uchompopen(filename):
    stopwords.add(line)

# Parse the result of a previous run of --coords-counts and generate
# a unigram distribution for Naive Bayes matching.  We do a simple version
# of Good-Turing smoothing where we assign probability mass to unseen
# words equal to the probability mass of all words seen once, and rescale
# the remaining probabilities accordingly.

def construct_naive_bayes_dist(filename):

  articles_seen = 0
  total_tokens = 0

  def one_article_probs():
    if total_tokens == 0: return
    art = Article.find_article_create(title)
    if art.counts:
      warning("Article %s already has counts for it!" % art)
    wordcounts = {}
    # Compute probabilities.  Use a very simple version of Good-Turing
    # smoothing where we assign to unseen words the probability mass of
    # words seen once, and adjust all other probs accordingly.
    art.words_seen_once = [word for word in wordhash if wordhash[word] == 1]
    oncecount = len(art.words_seen_once)
    unseen_mass = float(oncecount)/total_tokens
    for (word,count) in wordhash.iteritems():
      global num_word_types
      ind = internasc(word)
      if ind not in overall_word_probs:
        num_word_types += 1
      # Record in overall_word_probs; note more tokens seen.
      overall_word_probs[ind] += count
      global num_word_tokens
      num_word_tokens += count
      # Record in current article's word->probability map.
      # wordcounts[ind] = float(count)/total_tokens*(1 - unseen_mass)
      wordcounts[ind] = count
    art.counts = make_sorted_list(wordcounts)
    art.unseen_mass = unseen_mass
    art.total_tokens = total_tokens
    if debug > 3:
      uniprint("Title = %s, numtypes = %s, numtokens = %s, unseen_mass = %s"
               % (title, len(art.counts[0]), total_tokens, unseen_mass))
    if (articles_seen % 100) == 0:
      print >>sys.stderr, "Processed %d articles" % articles_seen

  for line in uchompopen(filename):
    if line.startswith('Article title: '):
      m = re.match('Article title: (.*)$', line)
      articles_seen += 1
      one_article_probs()
      # Stop if we've reached the maximum
      if max_articles_to_count and articles_seen >= max_articles_to_count:
        break
      title = m.group(1)
      wordhash = intdict()
      total_tokens = 0
    elif line.startswith('Article coordinates: '):
      pass
    else:
      m = re.match('(.*) = ([0-9]+)$', line)
      if not m:
        warning("Strange line, can't parse: title=%s: line=%s" % (title, line))
        continue
      word = m.group(1)
      if ignore_case_words: word = word.lower()
      count = int(m.group(2))
      if word in stopwords and ignore_stopwords_in_article_dists: continue
      word = internasc(word)
      total_tokens += count
      wordhash[word] += count
  else:
    articles_seen += 1
    one_article_probs()

  # Now, adjust overall_word_probs accordingly.
  global num_types_seen_once
  num_types_seen_once = 0
  for count in overall_word_probs.itervalues():
    if count == 1:
      num_types_seen_once += 1
  global globally_unseen_word_prob
  globally_unseen_word_prob = float(num_types_seen_once)/num_word_tokens
  for (wordind,count) in overall_word_probs.iteritems():
    overall_word_probs[wordind] = (
      float(count)/num_word_tokens*(1 - globally_unseen_word_prob))
  # A very rough estimate, perhaps totally wrong
  global num_unseen_word_types
  num_unseen_word_types = num_types_seen_once

  #if debug > 2:
  #  uniprint("Num types = %s, num tokens = %s, num_seen_once = %s, globally unseen word prob = %s, total mass = %s" % (num_word_types, num_word_tokens, num_types_seen_once, globally_unseen_word_prob, globally_unseen_word_prob + sum(overall_word_probs.itervalues())))

  # Figure out the value of OVERALL_UNSEEN_MASS for each article.
  for art in name_to_article.itervalues():
    # make sure counts not None (eg article in coords file but not counts file)
    if not art.counts: continue
    overall_seen_mass = 0.0
    for ind in art.counts[0]:
      overall_seen_mass += overall_word_probs[ind]
    art.overall_unseen_mass = 1.0 - overall_seen_mass
    art.finished = True

# Parse the result of a previous run of --coords-counts for articles with
# coordinates
def get_coordinates(filename):
  for line in uchompopen(filename):
    if rematch('Article title: (.*)$', line):
      title = m_[1]
    elif rematch('Article coordinates: (.*),(.*)$', line):
      # The context may be modified by find_article_create()
      c = Coord(safe_float(m_[1]), safe_float(m_[2]))
      art = Article.find_article_create(title)
      art.coord = c

# Add the given locality to the region map, which covers the earth in regions
# of a particular size to aid in computing the regions used in region-based
# Naive Bayes.
def add_locality_to_region_map(loc):
  (latind, longind) = coord_to_region_indices(loc.coord)
  region_to_locations[(latind, longind)] += [loc]

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
#
# For localities, add them to the region-map that covers the earth if
# ADD_TO_REGION_MAP is true.

def match_world_gazetteer_entry(line, add_to_region_map):
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
  loc.div = Division.note_point_seen_in_division(loc, (div1, div2, div3))
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
  
  if add_to_region_map:
    add_locality_to_region_map(loc)

  # Record the match.
  loc.match = match
  if debug > 1:
    uniprint("Matched location %s (coord %s) with article %s, dist=%s"
             % (loc.name, loc.coord, match,
                spheredist(loc.coord, match.coord)))

# Read in the data from the World gazetteer in FILENAME and find the
# Wikipedia article matching each entry in the gazetteer.  For localities,
# add them to the region-map that covers the earth if ADD_TO_REGION_MAP is
# true.
def read_world_gazetteer_and_match(filename, add_to_region_map):

  # Match each entry in the gazetteer
  for line in uchompopen(filename):
    if debug > 1:
      uniprint("Processing line: %s" % line)
    match_world_gazetteer_entry(line, add_to_region_map)

  for division in path_to_division.itervalues():
    if debug > 1:
      uniprint("Processing division named %s, path %s"
               % (division.name, division.path))
    division.compute_boundary()
    match = find_match_for_division(division)
    if match:
      if debug > 1:
        uniprint("Matched article %s for division %s, path %s" %
                 (match, division.name, division.path))
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
      art = Article.find_article_create(m_[1])
      art.incoming_links = int(m_[2])
      articles_seen += 1
      if (articles_seen % 10000) == 0:
        print >>sys.stderr, "Processed %d articles" % articles_seen

def read_redirect_file(filename):
  for line in uchompopen(filename):
    if rematch('Article title: (.*)$', line):
      title = m_[1]
    elif rematch('Redirect to: (.*)$', line):
      redirect = m_[1]
      redart = name_to_article.get(redirect, None)
      if redart:
        art = Article.find_article_create(title)
        art.copy_from(redart)
    else:
      warning("Strange line in redirect file: %s" % line)

# Class of word in a file containing toponyms.  Fields:
#
#   word: The identity of the word.
#   is_stop: True if it is a stopword.
#   is_toponym: True if it is a toponym.
#   coord: For a toponym with specified ground-truth coordinate, the
#          coordinate.  Else, none.
#   context: Vector including the word and 10 words on other side.
#
class GeogWord(object):
  __slots__ = ['word', 'is_stop', 'is_toponym', 'coord', 'context']

  def __init__(self, word):
    self.word = word
    self.is_stop = False
    self.is_toponym = False
    self.coord = None
    self.context = None

# Read a file formatted in TR-CONLL text format (.tr files).  An example of
# how such files are fomatted is:
#
#...
#...
#last    O       I-NP    JJ
#week    O       I-NP    NN
#&equo;s O       B-NP    POS
#U.N.    I-ORG   I-NP    NNP
#Security        I-ORG   I-NP    NNP
#Council I-ORG   I-NP    NNP
#resolution      O       I-NP    NN
#threatening     O       I-VP    VBG
#a       O       I-NP    DT
#ban     O       I-NP    NN
#on      O       I-PP    IN
#Sudanese        I-MISC  I-NP    NNP
#flights O       I-NP    NNS
#abroad  O       I-ADVP  RB
#if      O       I-SBAR  IN
#Khartoum        LOC
#        >c1     NGA     15.5833333      32.5333333      Khartoum > Al Khar<BA>om > Sudan
#        c2      NGA     -17.8833333     30.1166667      Khartoum > Zimbabwe
#        c3      NGA     15.5880556      32.5341667      Khartoum > Al Khar<BA>om > Sudan
#        c4      NGA     15.75   32.5    Khartoum > Al Khar<BA>om > Sudan
#does    O       I-VP    VBZ
#not     O       I-NP    RB
#hand    O       I-NP    NN
#over    O       I-PP    IN
#three   O       I-NP    CD
#men     O       I-NP    NNS
#...
#...
#
# The return value is a list of GeogWord objects, one per word 

def read_trconll_file(filename, compute_context):
  results = []
  in_loc = False
  for line in uchompopen(filename, errors='replace'):
    try:
      (word, ty) = re.split('\t', line, 1)
      if word:
        wordstruct = GeogWord(word)
        results.append(wordstruct)
      if in_loc and word:
        in_loc = False
      elif ty.startswith('LOC'):
        in_loc = True
        wordstruct.is_toponym = True
        loc_word = wordstruct
      elif in_loc and ty[0] == '>':
        (off, gaz, lat, long, fulltop) = re.split('\t', ty, 4)
        lat = float(lat)
        long = float(long)
        loc_word.coord = Coord(lat, long)
        if debug > 0:
          uniprint("Saw loc %s with true coordinates %s" %
                   (loc_word.word, loc_word.coord))
    except Exception, exc:
      print "Bad line %s" % line
      print "Exception is %s" % exc
      if type(exc) is not ValueError:
        traceback.print_exc()
      return []

  # Now compute context for words
  nbcl = naive_bayes_context_len
  if compute_context:
    # First determine whether each word is a stopword
    for i in xrange(len(results)):
      # If a word tagged as a toponym is homonymous with a stopword, it
      # still isn't a stopword.
      results[i].is_stop = not results[i].coord and results[i].word in stopwords
    # Now generate context for toponyms
    for i in xrange(len(results)):
      if results[i].coord:
        # Select up to naive_bayes_context_len words on either side; skip
        # stopwords.
        results[i].context = \
          [x.word for x in results[max(0,i-nbcl):min(len(results),i+nbcl+1)]
                  if x.word not in stopwords]
  return results
  
# Given a TR-CONLL file, read in the words specified, including the toponyms.
# If COMPUTE_CONTEXT, also generate the set of "context" words used for
# disambiguation (some window, e.g. size 20, of words around each toponym).
# Call PROCESS_FUN on each word; however, if ONLY_TOPONYMS is True, only
# call it on toponyms.

def process_trconll_file(fname, process_fun, compute_context, only_toponyms):
  print "Processing TR-CONLL file %s..." % fname
  results = read_trconll_file(fname, compute_context)
  for geogword in results:
    if not only_toponyms or geogword.is_toponym:
      process_fun(geogword)

def disambiguate_trconll_file(fname, compute_score, compute_context):
  def process_fun(geogword):
    disambiguate_toponym(geogword, compute_score)
  process_trconll_file(fname, process_fun, compute_context, only_toponyms=True)

# Process all files in DIR, calling FUN on each one (with the directory name
# joined to the name of each file in the directory).
def process_dir_files(dir, fun):
  for fname in os.listdir(dir):
    fullname = os.path.join(dir, fname)
    fun(fullname)
  
# Given a TR-CONLL file, count the toponyms seen and add to the global count
# in toponyms_seen_in_eval_files.
def count_toponyms_in_file(fname):
  def count_toponyms(geogword):
    toponyms_seen_in_eval_files[geogword.word.lower()] += 1
  process_trconll_file(fname, count_toponyms, compute_context=False,
                       only_toponyms=True)

# Disambiguate the toponym, specified in GEOGWORD.  Determine the possible
# locations that the toponym can map to, and call COMPUTE_SCORE on each one
# to determine a score.  The best score determines the location considered
# "correct".  Locations without a matching Wikipedia article are skipped.
# The location considered "correct" is compared with the actual correct
# location specified in the toponym, and global variables corresponding to
# the total number of toponyms processed and number correctly determined are
# incremented.  Various debugging info is output if 'debug' is set.
# COMPUTE_SCORE is passed two arguments: GEOGWORD and the location to
# compute the score of.

def disambiguate_toponym(geogword, compute_score):
  toponym = geogword.word
  coord = geogword.coord
  if not coord: return # If no ground-truth, skip it
  lowertop = toponym.lower()
  bestscore = -1e308
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
      art = loc.match
      if debug > 0:
          uniprint("Considering %s, article %s" % (loc, art))
      if not art:
        if debug > 0:
          uniprint("--> Location without matching article")
        continue
      else:
        thisscore = compute_score(geogword, loc)
      if thisscore > bestscore:
        bestscore = thisscore
        bestloc = loc
    if bestloc:
      correct = bestloc.matches_coord(coord)
    else:
      correct = False

  # Toponyms by number of candidates available
  total_toponyms_by_number_of_candidates = intdict()
  correct_toponyms_by_number_of_candidates = intdict()
  incorrect_toponyms_by_number_of_candidates = intdict()
  # Toponyms by number of candidates available with associated Wikipedia articles
  total_toponyms_by_number_of_article_candidates = intdict()
  correct_toponyms_by_number_of_article_candidates = intdict()
  incorrect_toponyms_by_number_of_article_candidates = intdict()
  # Toponyms where the correct answer is one of the candidates
  total_toponyms_with_correct_in_candidates = 0
  correct_toponyms_with_correct_in_candidates = 0
  incorrect_toponyms_with_correct_in_candidates = 0
  # Toponyms where the correct answer is one of the candidates with associated
  # Wikipedia article
  total_toponyms_with_correct_in_article_candidates = 0
  correct_toponyms_with_correct_in_article_candidates = 0
  incorrect_toponyms_with_correct_in_article_candidates = 0

  num_locs = len(locs)
  num_article_locs = sum(1 for loc in locs if loc.match)

  Eval.total_toponyms_by_num_candidates[num_locs] += 1
  Eval.total_toponyms_by_num_article_candidates[num_article_locs] += 1
  Eval.total_toponyms += 1

  if correct:
    Eval.correct_toponyms += 1
    Eval.correct_toponyms_by_num_candidates[num_locs] += 1
    Eval.correct_toponyms_by_num_article_candidates[num_article_locs] += 1
  else:
    Eval.incorrect_toponyms += 1
    Eval.incorrect_toponyms_by_num_candidates[num_locs] += 1
    Eval.incorrect_toponyms_by_num_article_candidates[num_article_locs] += 1
    if num_locs == 0:
      reason = 'incorrect_with_no_candidates'
    elif num_article_locs == 0:
      reason = 'incorrect_with_no_article_candidates'
    else:
      good_locs = [loc for loc in locs if loc.matches_coord(coord)]
      good_article_locs = [loc for loc in good_locs if loc.match]
      if not good_locs:
        reason = 'incorrect_with_no_correct_candidates'
      elif not good_article_locs:
        reason = 'incorrect_with_correct_candidates_but_no_correct_article_candidates'
      elif len(good_article_locs) > 1:
        reason = 'incorrect_with_multiple_correct_article_candidates'
      else:
        goodloc = good_article_locs[0]
        if type(goodloc) is Locality and \
           not goodloc.matches_coord(goodloc.match.coord):
          reason = 'incorrect_one_correct_article_candidate_but_article_coord_doesnt_match_location'
        else:
          reason = 'incorrect_one_good_correct_article_candidate'
  if correct:
    uniprint("Eval: Toponym %s, correct" % toponym)
  else:
    uniprint("Eval: Toponym %s, incorrect, reason = %s" % (toponym, reason))
    setattr(Eval, reason, getattr(Eval, reason) + 1)

  if debug > 0 and bestloc:
    uniprint("Best location = %s, score = %s, dist = %s, correct %s"
             % (bestloc, bestscore, bestloc.distance_to_coord(coord), correct))

def get_adjusted_incoming_links(art):
  thislinks = art.incoming_links
  if thislinks == None:
    thislinks = 0
    if debug > 0:
      warning("Strange, %s has no link count" % art)
  else:
    if debug > 0:
      uniprint("--> Link count is %s" % thislinks)
  if thislinks == 0: # Whether from unknown count or count is actually zero
    thislinks = 0.01 # So we don't get errors from log(0)
  return thislinks

# Given a TR-CONLL file, find each toponym explicitly mentioned as such
# and disambiguate it (find the correct geographic location) using the
# "link baseline", i.e. use the location with the highest number of
# incoming links.
def disambiguate_link_baseline(fname):
  def compute_score(geogword, loc):
    art = loc.match
    return get_adjusted_incoming_links(art)

  disambiguate_trconll_file(fname, compute_score, compute_context=False)

# Given a TR-CONLL file, find each toponym explicitly mentioned as such
# and disambiguate it (find the correct geographic location) using
# Naive Bayes.
def disambiguate_naive_bayes(fname):
  def compute_score(geogword, loc):
    art = loc.match
    thislinks = get_adjusted_incoming_links(art)

    if naive_bayes_type == 'article':
      distobj = art
    else:
      distobj = find_nbdist(loc)
    if not distobj.finished:
      wordcounts = unknown_article_counts
      total_tokens = 0
      unseen_mass = 1.0
      overall_unseen_mass = 1.0
      if debug > 0:
        uniprint("Counts for article %s not tabulated" % art)
    else:
      wordcounts = distobj.counts
      unseen_mass = distobj.unseen_mass
      total_tokens = distobj.total_tokens
      overall_unseen_mass = distobj.overall_unseen_mass
      if debug > 0:
        uniprint("Found counts for article %s, num word types = %s"
                 % (art, len(wordcounts[0])))
        uniprint("Unknown prob = %s, overall_unseen_mass = %s" %
                 (unseen_mass, overall_unseen_mass))
    totalprob = 0.0
    for word in geogword.context:
      if ignore_case_words: word = word.lower()
      if word in overall_word_probs:
        ind = internasc(word)
      else:
        ind = None
      if ind == None:
        wordprob = (unseen_mass*globally_unseen_word_prob
                    / num_unseen_word_types)
        if debug > 0:
          uniprint("Word %s, never seen at all, wordprob = %s" %
                   (word, wordprob))
      else:
        wordprob = lookup_sorted_list(wordcounts, ind)
        if wordprob == None:
          wordprob = (unseen_mass *
                      (overall_word_probs[ind] / overall_unseen_mass))
          #if wordprob <= 0:
          #  warning("Bad values; unseen_mass = %s, overall_word_probs[ind] = %s, overall_unseen_mass = %s" % (unseen_mass, overall_word_probs[ind], overall_unseen_mass))
          if debug > 0:
            uniprint("Word %s, seen but not in article, wordprob = %s" %
                     (word, wordprob))
        else:
          #if wordprob <= 0 or total_tokens <= 0 or unseen_mass >= 1.0:
          #  warning("Bad values; wordprob = %s, unseen_mass = %s" %
          #          (wordprob, unseen_mass))
          #  for (word,count) in itertools.izip(wordcounts[0], wordcounts[1]):
          #    uniprint("%s: %s" % (word, count))
          wordprob = float(wordprob)/total_tokens*(1 - unseen_mass)
          if debug > 0:
            uniprint("Word %s, seen in article, wordprob = %s" %
                     (word, wordprob))
      totalprob += math.log(wordprob)
    if debug > 0:
      uniprint("Computed total log-likelihood as %s" % totalprob)
    totalprob += math.log(thislinks)
    if debug > 0:
      uniprint("Computed thislinks-prob + total log-likelihood as %s" % totalprob)
    return totalprob

  def process_fun(geogword):
    disambiguate_toponym(geogword, compute_score)
  process_trconll_file(fname, process_fun, compute_context=True,
                       only_toponyms=True)

############################################################################
#                                  Main code                               #
############################################################################

def main():
  op = OptionParser(usage="%prog [options] input_dir")
  op.add_option("-t", "--gazetteer-type", type='choice', default="world",
                choices=['world', 'db'],
                help="""Type of gazetteer file specified using --gazetteer;
default '%default'.""")
  op.add_option("-l", "--links-file",
                help="""File containing incoming link information for
Wikipedia articles. Output by processwiki.py --find-links.""",
                metavar="FILE")
  op.add_option("-s", "--stopwords-file",
                help="""File containing list of stopwords.""",
                metavar="FILE")
  op.add_option("--redirect-file",
                help="""File containing redirects from Wikipedia.""",
                metavar="FILE")
  op.add_option("-g", "--gazetteer-file",
                help="""File containing gazetteer information to match.""",
                metavar="FILE")
  op.add_option("-c", "--coords-file",
                help="""File containing output from a prior run of
--coords-counts, listing all the articles with associated coordinates.
May be filtered only for articles and coordinates.""",
                metavar="FILE")
  op.add_option("-w", "--words-coords-file",
                help="""File containing output from a prior run of
--coords-counts, listing all the articles with associated coordinates.
Should not be filtered, as the counts of words are needed.""",
                metavar="FILE")
  op.add_option("-p", "--pickle-file",
                help="""Serialize the result of processing the word-coords
file to the given file.""",
                metavar="FILE")
  op.add_option("-u", "--unpickle-file",
                help="""Read the result of serializing the word-coords file
to the given file.""",
                metavar="FILE")
  op.add_option("-f", "--trconll-dir",
                help="""Directory containing TR-CONLL files in the text format.
Each file is read in and then disambiguation is performed.""",
                metavar="DIR")
  op.add_option("--width-of-nbregion", type='int', default=2,
                help="""Width of the Naive Bayes region used for region-based
Naive Bayes disambiguation, in tiling regions.  Default %default.""")
  op.add_option("--max-articles-to-count", type='int', default=0,
                help="""Maximum number of articles to load word counts from, for testing.
If 0, load all articles.  Default %default.""")
  op.add_option("-r", "--region-size", type='float', default=50.0,
                help="""Size of the region (in miles) to use when doing
region-based Naive Bayes disambiguation.  Default %default.""")
  op.add_option("-b", "--naive-bayes-type", type='choice',
                default="round-region",
                choices=['article', 'round-region', 'square-region'],
                help="""Type of context used when doing Naive Bayes
disambiguation. 'article' means use only the words in the article itself.
'round-region' means to use a region of constant radius centered on the
article's location. 'square-region' means to use a "square" region
approximately centered on the article's location. ("Square" is a misnomer
because it actually works by dividing the earth into regions of constant
latitude and longitude; as a result, the width of the regions gets
smaller as the latitude moves away from the equator. In addition,
for "square" regions, the region boundaries are fixed rather than
corresponding to a region properly centered on a location.) In
addition, when the article itself refers to a region rather than a
locality, both region-based methods use the articles which are
specified in the gazetteer to belong to the region, rather than any
particular-sized region.  Default '%default'.""")
  op.add_option("-m", "--mode", type='choice', default='match-only',
                choices=['disambig-link-baseline',
                         'disambig-naive-bayes',
                         'match-only', 'pickle-only'],
                help="""Action to perform.  'match-only' means to only do the
stage that involves finding matches between gazetteer locations and Wikipedia
articles (mostly useful when debugging output is enabled). 'pickle-only' means
only to generate the pickled version of the --words-coords file.  The other
two cause disambiguation of locations in the TR-CONLL files specified in the
'--trconll-dir' option to be performed. 'disambig-link-baseline' means simply
use the matching location with the highest number of incoming links;
'disambig-naive-bayes' means also use the words around the toponym to be
disambiguated, in a Naive-Bayes scheme. Default '%default'.""")
  op.add_option("-d", "--debug", type='int', metavar="LEVEL",
                help="Output debug info at given level")

  uniprint("Arguments: %s" % ' '.join(sys.argv), flush=True)

  ### Process the command-line options and set other values from them ###
  
  opts, args = op.parse_args()

  global debug
  if opts.debug:
    debug = int(opts.debug)
 
  # FIXME! Can only currently handle World-type gazetteers.
  if opts.gazetteer_type != 'world':
    op.error("Currently can only handle world-type gazetteers")

  if opts.region_size <= 0:
    op.error("Region size must be positive")
  global degrees_per_region
  degrees_per_region = opts.region_size / miles_per_degree
  global maximum_latind, minimum_latind, maximum_longind, minimum_longind
  maximum_latind, maximum_longind = \
     coord_to_region_indices(Coord(maximum_latitude, maximum_longitude))
  minimum_latind, minimum_longind = \
     coord_to_region_indices(Coord(minimum_latitude, minimum_longitude))

  if opts.width_of_nbregion <= 0:
    op.error("Width of Naive Bayes region must be positive")
  global width_of_nbregion
  width_of_nbregion = opts.width_of_nbregion

  global naive_bayes_type
  naive_bayes_type = opts.naive_bayes_type

  global max_articles_to_count
  max_articles_to_count = opts.max_articles_to_count

  ### Start reading in the files and operating on them ###

  # Files needed for different options:
  #
  # 1. match-only:
  #
  #    coords-file
  #    gazetteer-file
  #    links-file
  #
  # 2. pickle-only:
  #
  #    words-coords-file
  #    stopwords-file
  #    pickle-file
  #
  # 3. disambig-link-baseline:
  #
  #    coords-file
  #    gazetteer-file
  #    links-file
  #    trconll-dir
  #
  # 4. disambig-naive-bayes:
  #
  #    stopwords-file
  #    words-coords-file or unpickle-file
  #    pickle-file if given
  #    coords-file
  #    gazetteer-file
  #    links-file
  #    trconll-dir

  if opts.mode == 'pickle-only' or opts.mode == 'disambig-naive-bayes':
    if not opts.stopwords_file:
      op.error("Must specify stopwords file using -s or --stopwords-file")
    print "Reading stopwords file %s..." % opts.stopwords_file
    read_stopwords(opts.stopwords_file)
    if not opts.unpickle_file and not opts.words_coords_file:
      op.error("Must specify either unpickle file or words-coords file")

  if opts.mode != 'pickle-only':
    if not opts.coords_file:
      op.error("Must specify coordinate file using -c or --coords-file")
    if not opts.gazetteer_file:
      op.error("Must specify gazetteer file using -g or --gazetteer-file")
    if not opts.links_file:
      op.error("Must specify links file using -l or --links-file")

  if opts.mode == 'disambig-link-baseline' or \
     opts.mode == 'disambig-naive-bayes':
    if not opts.trconll_dir:
      op.error("Must specify TR-CONLL directory using -f or --trconll-dir")

  if opts.mode == 'pickle-only':
    if not opts.pickle_file:
      op.error("For pickle-only mode, must specify pickle file using -p or --pick-file")

  #print "Processing TR-CONLL directory %s for toponym counts..." % opts.trconll_dir
  #process_dir_files(opts.trconll_dir, count_toponyms_in_file)
  #print "Number of toponyms seen: %s" % len(toponyms_seen_in_eval_files)
  #print "Number of toponyms seen more than once: %s" % \
  #  len([foo for (foo,count) in toponyms_seen_in_eval_files.iteritems() if
  #       count > 1])
  #output_reverse_sorted_table(toponyms_seen_in_eval_files)

  # Read in (or unpickle) and maybe pickle the words-counts file
  if opts.mode == 'pickle-only' or opts.mode == 'disambig-naive-bayes':
    if opts.unpickle_file:
      global article_probs
      infile = open(opts.unpickle_file)
      #FIXME: article_probs = cPickle.load(infile)
      infile.close()
    else:
      print "Reading words and coordinates file %s..." % opts.words_coords_file
      construct_naive_bayes_dist(opts.words_coords_file)
    if opts.pickle_file:
      outfile = open(opts.pickle_file, "w")
      #FIXME: cPickle.dump(article_probs, outfile)
      outfile.close()

  if opts.mode == 'pickle-only': return

  print "Reading coordinates file %s..." % opts.coords_file
  get_coordinates(opts.coords_file)
  print "Reading incoming links file %s..." % opts.links_file
  read_incoming_link_info(opts.links_file)
  if opts.redirect_file:
    print "Reading redirect file %s..." % opts.redirect_file
    read_redirect_file(opts.redirect_file)
  print "Reading and matching World gazetteer file %s..." % opts.gazetteer_file
  read_world_gazetteer_and_match(opts.gazetteer_file,
                                 opts.naive_bayes_type != "article")

  if opts.mode == 'match-only': return

  if opts.mode == 'disambig-link-baseline':
    disambig_fun = disambiguate_link_baseline
  else:
    assert opts.mode == 'disambig-naive-bayes'
    disambig_fun = disambiguate_naive_bayes

  print "Processing TR-CONLL directory %s..." % opts.trconll_dir
  process_dir_files(opts.trconll_dir, disambig_fun)
  Eval.output()

main()
