#!/usr/bin/python

#######
####### wiki_disambig.py
#######
####### Copyright (c) 2010 Ben Wing.
#######

import sys, re
import os, os.path
import math
from math import log
import collections
import traceback
import cPickle
import itertools
import gc
from textutil import *
from process_article_data import *

# FIXME:
#
# -- If coords are 0,9999, they are inaccurate, ignore them

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

# List of stopwords
stopwords = set()

# For each toponym (name of location), value is a list of Locality items,
# listing gazetteer locations and corresponding matching Wikipedia articles.
lower_toponym_to_location = listdict()

# For each toponym, list of Wikipedia articles matching the name.
lower_toponym_to_article = listdict()

# For each toponym corresponding to a division higher than a locality,
# list of divisions with this name.
lower_toponym_to_division = listdict()

# For each division, map from division's path to Division object.
path_to_division = {}

# Debug level; if non-zero, output lots of extra information about how
# things are progressing.  If > 1, even more info.
debug = 0

# Size of each region in degrees.  Determined by the --region-size option
# (however, that option is expressed in miles).
degrees_per_region = 0.0

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

# Table of all toponyms seen in evaluation files, along with how many times
# seen.  Used to determine when caching of certain toponym-specific values
# should be done.
#toponyms_seen_in_eval_files = intdict()

# Count of total documents processed so far
documents_processed = 0

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

def coord_to_tiling_region_indices(coord):
  latind = int(math.floor(coord.lat / degrees_per_region))
  longind = int(math.floor(coord.long / degrees_per_region))
  return (latind, longind)

def coord_to_nbregion_indices(coord):
  # When Opts.width_of_nbregion = 1, don't subtract anything.
  # When Opts.width_of_nbregion = 2, subtract 0.5*degrees_per_region.
  # When Opts.width_of_nbregion = 3, subtract degrees_per_region.
  # When Opts.width_of_nbregion = 4, subtract 1.5*degrees_per_region.
  # In general, subtract (Opts.width_of_nbregion-1)/2.0*degrees_per_region.

  # Compute the indices of the southwest region
  subval = (Opts.width_of_nbregion-1)/2.0*degrees_per_region
  lat = coord.lat - subval
  long = coord.long - subval
  if lat < minimum_latitude: lat = minimum_latitude
  if long < minimum_longitude: long += 360.

  latind, longind = coord_to_tiling_region_indices(Coord(lat, long))
  return (latind, longind)

def region_indices_to_coord(latind, longind):
  return Coord(latind * degrees_per_region, longind * degrees_per_region)

############################################################################
#                             Word distributions                           #
############################################################################

class GlobalDist(object):
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
  # unknown_article_counts = ([], [])

  @staticmethod
  def finish_article_distributions():
    # Now, adjust overall_word_probs accordingly.
    GlobalDist.num_types_seen_once = 0
    ### FIXME: A simple calculation reveals that in the scheme where we use
    ### globally_unseen_word_prob, num_types_seen_once cancels out and
    ### we never actually have to compute it.
    for count in GlobalDist.overall_word_probs.itervalues():
      if count == 1:
        GlobalDist.num_types_seen_once += 1
    GlobalDist.globally_unseen_word_prob = (
      float(GlobalDist.num_types_seen_once)/GlobalDist.num_word_tokens)
    for (wordind,count) in GlobalDist.overall_word_probs.iteritems():
      GlobalDist.overall_word_probs[wordind] = (
        float(count)/GlobalDist.num_word_tokens*
          (1 - GlobalDist.globally_unseen_word_prob))
    # A very rough estimate, perhaps totally wrong
    GlobalDist.num_unseen_word_types = GlobalDist.num_types_seen_once

    #if debug > 2:
    #  errprint("Num types = %s, num tokens = %s, num_seen_once = %s, globally unseen word prob = %s, total mass = %s" % (GlobalDist.num_word_types, GlobalDist.num_word_tokens, GlobalDist.num_types_seen_once, GlobalDist.globally_unseen_word_prob, GlobalDist.globally_unseen_word_prob + sum(GlobalDist.overall_word_probs.itervalues())))

    # Figure out the value of OVERALL_UNSEEN_MASS for each article.
    for art in ArticleTable.name_to_article.itervalues():
      # make sure counts not None (eg article in coords file but not counts file)
      if not art.counts: continue
      overall_seen_mass = 0.0
      for ind in art.counts[0]:
        overall_seen_mass += GlobalDist.overall_word_probs[ind]
      art.overall_unseen_mass = 1.0 - overall_seen_mass
      art.finished = True


# Fields defined:
#
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

class WordDistribution(object):
  # Can't use __slots__, or you get this following for NBArticle:
  #TypeError: Error when calling the metaclass bases
  #    multiple bases have instance lay-out conflict
  myslots = ['finished', 'counts', 'unseen_mass', 'total_tokens',
             'overall_unseen_mass']

  def init_word_distribution(self):
    self.finished = False
    self.counts = intdict()
    self.unseen_mass = 1.0
    self.total_tokens = 0
    self.overall_unseen_mass = 1.0

  # Compute the KL divergence between this distribution and another
  # distribution.  This is a bit tricky.  We have to take into account:
  # 1. Words in this distribution (may or may not be in the other).
  # 2. Words in the other distribution that are not in this one.
  # 3. Words in neither distribution but seen globally.
  # 4. Words never seen at all.  These have the 
  def kl_divergence(self, other, partial=False):
    assert self.finished
    assert other.finished
    kldiv = 0.0
    overall_probs_diff_words = 0.0
    # 1.
    for word in self.counts[0]:
      p = self.lookup_word(word)
      q = other.lookup_word(word)
      kldiv += p*(log(p) - log(q))

    if partial:
      return kldiv

    # 2.
    for word in other.counts[0]:
      if lookup_sorted_list(self.counts, word) == None:
        p = self.lookup_word(word)
        q = other.lookup_word(word)
        kldiv += p*(log(p) - log(q))
        overall_probs_diff_words += GlobalDist.overall_word_probs[word]
    # 3. For words seen in neither dist but seen globally:
    # You can show that this is
    #
    # factor1 = (log(self.unseen_mass) - log(self.overall_unseen_mass)) -
    #           (log(other.unseen_mass) - log(other.overall_unseen_mass))
    # factor2 = self.unseen_mass / self.overall_unseen_mass * factor1
    # kldiv = factor2 * (sum(words seen globally but not in either dist)
    #                    of overall_word_probs[word]) 
    #
    # The final sum
    #   = 1 - sum(words in self) overall_word_probs[word]
    #       - sum(words in other, not self) overall_word_probs[word]
    #   = self.overall_unseen_mass
    #       - sum(words in other, not self) overall_word_probs[word]
    #
    # So we just need the sum over the words in other, not self.

    factor1 = ((log(self.unseen_mass) - log(self.overall_unseen_mass)) -
               (log(other.unseen_mass) - log(other.overall_unseen_mass)))
    factor2 = self.unseen_mass / self.overall_unseen_mass * factor1
    the_sum = self.overall_unseen_mass - overall_probs_diff_words
    kldiv += factor2 * the_sum

    # 4. For words never seen at all:
    p = (self.unseen_mass*GlobalDist.globally_unseen_word_prob /
          GlobalDist.num_unseen_word_types)
    q = (other.unseen_mass*GlobalDist.globally_unseen_word_prob /
          GlobalDist.num_unseen_word_types)
    kldiv += GlobalDist.num_unseen_word_types*(p*(log(p) - log(q)))

    return kldiv

  def symmetric_kldiv(self, other):
    return (0.5*self.kl_divergence(other) + 
            0.5*other.kl_divergence(self))

  def lookup_word(self, word):
    assert self.finished
    #if debug > 0:
    #  errprint("Found counts for article %s, num word types = %s"
    #           % (art, len(wordcounts[0])))
    #  errprint("Unknown prob = %s, overall_unseen_mass = %s" %
    #           (unseen_mass, overall_unseen_mass))
    if word in GlobalDist.overall_word_probs:
      ind = internasc(word)
    else:
      ind = None
    if ind == None:
      wordprob = (self.unseen_mass*GlobalDist.globally_unseen_word_prob
                  / GlobalDist.num_unseen_word_types)
      if debug > 1:
        errprint("Word %s, never seen at all, wordprob = %s" %
                 (word, wordprob))
    else:
      wordprob = lookup_sorted_list(self.counts, ind)
      if wordprob == None:
        wordprob = (self.unseen_mass *
                    (GlobalDist.overall_word_probs[ind] /
                     self.overall_unseen_mass))
        #if wordprob <= 0:
        #  warning("Bad values; unseen_mass = %s, overall_word_probs[ind] = %s, overall_unseen_mass = %s" % (unseen_mass, GlobalDist.overall_word_probs[ind], GlobalDist.overall_unseen_mass))
        if debug > 1:
          errprint("Word %s, seen but not in article, wordprob = %s" %
                   (word, wordprob))
      else:
        #if wordprob <= 0 or total_tokens <= 0 or unseen_mass >= 1.0:
        #  warning("Bad values; wordprob = %s, unseen_mass = %s" %
        #          (wordprob, unseen_mass))
        #  for (word,count) in itertools.izip(wordcounts[0], wordcounts[1]):
        #    errprint("%s: %s" % (word, count))
        wordprob = float(wordprob)/self.total_tokens*(1 - self.unseen_mass)
        if debug > 1:
          errprint("Word %s, seen in article, wordprob = %s" %
                   (word, wordprob))
    return wordprob

# Distribution used for Naive Bayes disambiguation.  The following
# fields are defined in addition to base class fields:
#
#   articles: Articles used in computing the distribution.
#   num_arts: Total number of articles included.
#   incoming_links: Total number of incoming links, or None if unknown.

class NBDist(WordDistribution):
  __slots__ = WordDistribution.myslots + \
      ['articles', 'num_arts', 'incoming_links']

  def __init__(self):
    self.init_word_distribution()
    self.articles = []
    self.num_arts = 0
    self.incoming_links = 0

  def is_empty(self):
    return self.num_arts == 0

  # Add the given articles to the total distribution seen so far
  def add_articles(self, articles):
    total_tokens = 0
    incoming_links = 0
    if debug > 1:
      errprint("Naive Bayes dist, number of articles = %s" % num_arts)
    counts = self.counts
    total_arts = 0
    num_arts = 0
    for art in articles:
      total_arts += 1
      if not art.finished:
        if not Opts.max_time_per_stage:
          warning("Saw unfinished article %s" % art)
        continue
      elif art.split != 'training':
        continue
      num_arts += 1
      self.articles += [art]
      for (word,count) in itertools.izip(art.counts[0], art.counts[1]):
        counts[word] += count
      total_tokens += art.total_tokens
      if art.incoming_links: # Might be None, for unknown link count
        incoming_links += art.incoming_links
    self.num_arts += num_arts
    self.total_tokens += total_tokens
    self.incoming_links += incoming_links
    if num_arts and debug > 0:
      errprint("""--> Finished processing, number articles handled = %s/%s,
    skipped articles = %s, total tokens = %s/%s, incoming links = %s/%s""" %
               (num_arts, self.num_arts, total_arts - num_arts,
                total_tokens, self.total_tokens, incoming_links,
                self.incoming_links))

  def add_locations(self, locs):
    arts = [loc.match for loc in locs if loc.match]
    self.add_articles(arts)

  def finish_dist(self):
    num_types_seen_once = \
      sum(1 for word in self.counts if self.counts[word] == 1)
    overall_seen_mass = 0.0
    for word in self.counts:
      overall_seen_mass += GlobalDist.overall_word_probs[word]
    self.overall_unseen_mass = 1.0 - overall_seen_mass
    if self.total_tokens > 0:
      # If no words seen only once, we will have a problem if we assign 0
      # to the unseen mass, as unseen words will end up with 0 probability.
      self.unseen_mass = \
        float(max(1, num_types_seen_once))/self.total_tokens
    else:
      self.unseen_mass = 1.0
    self.counts = make_sorted_list(self.counts)
    self.finished = True

    if debug > 1:
      errprint("""For Naive Bayes dist, num articles = %s, total tokens = %s,
    unseen_mass = %s, types seen once = %s, incoming links = %s,
    overall unseen mass = %s""" %
               (self.num_arts, self.total_tokens, self.unseen_mass,
                num_types_seen_once, self.incoming_links,
                self.overall_unseen_mass))

############################################################################
#                           Geographic locations                           #
############################################################################

############ Naive Bayes regions ############

# Info used in region-based Naive Bayes disambiguation.  This class contains
# values used in computing the distribution over all locations in the
# region surrounding the locality in question.  The region is currently a
# square of NxN tiling regions, for N = Opts.width_of_nbregion.
# The following fields are defined: 
#
#   latind, longind: Region indices of southwest-most tiling region in
#                    Naive Bayes region.
#   nbdist: Distribution corresponding to region.

class NBRegion(object):
  __slots__ = ['latind', 'longind', 'nbdist']
  
  # Mapping of region->locations in region, for region-based Naive Bayes
  # disambiguation.  The key is a tuple expressing the integer indices of the
  # latitude and longitude of the southwest corner of the region. (Basically,
  # given an index, the latitude or longitude of the southwest corner is
  # index*degrees_per_region, and the region includes all locations whose
  # latitude or longitude is in the half-open interval
  # [index*degrees_per_region, (index+1)*degrees_per_region).
  #
  # We don't just create an array because we expect many regions to have no
  # articles in them, esp. as we decrease the region size.  The idea is that
  # the regions provide a first approximation to the regions used to create the
  # article distributions.
  tiling_region_to_articles = listdict()

  # Mapping from center of Naive Bayes region to corresponding region object.
  # A "Naive Bayes region" is made up of a square of tiling regions, with
  # the number of regions on a side determined by `Opts.width_of_nbregion'.  A
  # word distribution is associated with each Naive Bayes region.
  corner_to_nbregion = {}

  empty_nbregion = None # Can't compute this until class is initialized
  all_regions_computed = False
  num_empty_regions = 0
  num_non_empty_regions = 0

  def __init__(self, latind, longind):
    self.latind = latind
    self.longind = longind
    self.nbdist = NBDist()

  # Generate the distribution for a Naive Bayes region from the tiling regions.
  def generate_dist(self):

    nblat = self.latind
    nblong = self.longind

    if debug > 1:
      errprint("Generating distribution for Naive Bayes region centered at %s"
               % region_indices_to_coord(nblat, nblong))

    # Accumulate counts for the given region
    def process_one_region(latind, longind):
      arts = NBRegion.tiling_region_to_articles.get((latind, longind), None)
      if not arts:
        return
      if debug > 1:
        errprint("--> Processing tiling region %s" %
                 region_indices_to_coord(latind, longind))
      self.nbdist.add_articles(arts)

    # Process the tiling regions making up the Naive Bayes region;
    # but be careful around the edges.
    for i in range(nblat, nblat + Opts.width_of_nbregion):
      for j in range(nblong, nblong + Opts.width_of_nbregion):
        jj = j
        if jj > maximum_longind: jj = minimum_longind
        process_one_region(i, jj)

    self.nbdist.finish_dist()

  # Find the correct NBRegion for the given coordinates.
  # If none, create the region.
  @staticmethod
  def find_region_for_coord(coord):
    latind, longind = coord_to_nbregion_indices(coord)
    return NBRegion.find_region_for_region_indices(latind, longind)

  # Find the NBRegion with the given indices at the southwest point.
  # If none, create the region.
  @staticmethod
  def find_region_for_region_indices(latind, longind, no_create_empty=False):
    nbreg = NBRegion.corner_to_nbregion.get((latind, longind), None)
    if not nbreg:
      if NBRegion.all_regions_computed:
        if not NBRegion.empty_nbregion:
          NBRegion.empty_nbregion = NBRegion(None, None)
          NBRegion.empty_nbregion.nbdist.finish_dist()
        return NBRegion.empty_nbregion
      nbreg = NBRegion(latind, longind)
      nbreg.generate_dist()
      empty = nbreg.nbdist.is_empty()
      if empty:
        NBRegion.num_empty_regions += 1
      else:
        NBRegion.num_non_empty_regions += 1
      if not empty or not no_create_empty:
        NBRegion.corner_to_nbregion[(latind, longind)] = nbreg
    return nbreg

  # Generate all NBRegions that are non-empty.
  @staticmethod
  def generate_all_nonempty_regions():
    errprint("Generating all non-empty Naive Bayes regions...")
    status = StatusMessage('Naive Bayes region')

    for i in xrange(minimum_latind, maximum_latind + 1):
      for j in xrange(minimum_longind, maximum_longind + 1):
        NBRegion.find_region_for_region_indices(i, j, no_create_empty=True)
        status.item_processed()

    NBRegion.all_regions_computed = True
    
  # Add the given article to the region map, which covers the earth in regions
  # of a particular size to aid in computing the regions used in region-based
  # Naive Bayes.
  @staticmethod
  def add_article_to_region(article):
    latind, longind = coord_to_tiling_region_indices(article.coord)
    NBRegion.tiling_region_to_articles[(latind, longind)] += [article]

  @staticmethod
  def yield_all_nonempty_regions():
    return NBRegion.corner_to_nbregion.iteritems()

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
    return self.distance_to_coord(coord) <= Opts.max_dist_for_close_match


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
    # as points that are more than Opts.max_dist_for_outliers away from all
    # other points.
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
      #  if mindist <= Opts.max_dist_for_outliers: yield p

    if debug > 1:
      errprint("Computing boundary for %s, path %s, num points %s" %
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
      lower_toponym_to_division[path[-1].lower()] += [division]
    division.locs += [loc]
    return division

############################################################################
#                             Wikipedia articles                           #
############################################################################

#####################  Article table

# Static class maintaining tables listing all articles and mapping between
# names, ID's and articles.  Objects corresponding to redirect articles
# should not be present anywhere in this table; instead, the name of the
# redirect article should point to the article object for the article
# pointed to by the redirect.
class ArticleTable(object):
  # Map from short name (lowercased) to list of Wikipedia articles.  The short
  # name for an article is computed from the article's name.  If the article
  # name has a comma, the short name is the part before the comma, e.g. the
  # short name of "Springfield, Ohio" is "Springfield".  If the name has no
  # comma, the short name is the same as the article name.  The idea is that
  # the short name should be the same as one of the toponyms used to refer to
  # the article.
  short_lower_name_to_articles = listdict()

  # Map from tuple (NAME, DIV) for Wikipedia articles of the form
  # "Springfield, Ohio", lowercased.
  lower_name_div_to_articles = listdict()

  # Mapping from article names to Article objects, using the actual case of
  # the article.
  name_to_article = {}

  # Mapping from lowercased article names to Article objects
  lower_name_to_articles = listdict()

  articles_by_split = {}

  # Look up an article named NAME and return the associated article.
  # Note that article names are case-sensitive but the first letter needs to
  # be capitalized.
  @staticmethod
  def lookup_article(name):
    assert name
    return ArticleTable.name_to_article.get(capfirst(name), None)

  # Record the article as having NAME as one of its names (there may be
  # multiple names, due to redirects).  Also add to related lists mapping
  # lowercased form, short form, etc.  If IS_REDIRECT, this is a redirect to
  # an article, so don't record it again.
  @staticmethod
  def record_article(name, art, is_redirect=False):
    # Must pass in properly cased name
    assert name == capfirst(name)
    ArticleTable.name_to_article[name] = art
    loname = name.lower()
    ArticleTable.lower_name_to_articles[loname] += [art]
    (short, div) = compute_short_form(loname)
    if div:
      ArticleTable.lower_name_div_to_articles[(short, div)] += [art]
    ArticleTable.short_lower_name_to_articles[short] += [art]
    if art not in lower_toponym_to_article[loname]:
      lower_toponym_to_article[loname] += [art]
    if short != loname and art not in lower_toponym_to_article[short]:
      lower_toponym_to_article[short] += [art]
    if not is_redirect:
      splithash = ArticleTable.articles_by_split
      if art.split not in splithash:
        #splithash[art.split] = set()
        splithash[art.split] = []
      splitcoll = splithash[art.split]
      if isinstance(splitcoll, set):
        splitcoll.add(art)
      else:
        splitcoll.append(art)


######################## Articles

# Compute the short form of an article name.  If short form includes a
# division (e.g. "Tucson, Arizona"), return a tuple (SHORTFORM, DIVISION);
# else return a tuple (SHORTFORM, None).

def compute_short_form(name):
  if rematch('(.*?), (.*)$', name):
    return (m_[1], m_[2])
  elif rematch('(.*) \(.*\)$', name):
    return (m_[1], None)
  else:
    return (name, None)

# A Wikipedia article for geotagging.  Defined fields, in addition to those
# of the base classes:
#
#   location: Corresponding location for this article.
#   nbregion: NBRegion object corresponding to this article.

class NBArticle(Article, WordDistribution):
  __slots__ = Article.__slots__ + WordDistribution.myslots + [
    'location', 'nbregion']

  def __init__(self, **args):
    super(NBArticle, self).__init__(**args)
    self.init_word_distribution()
    self.location = None
    self.nbregion = None

  def distance_to_coord(self, coord):
    return spheredist(self.coord, coord)

  def matches_coord(self, coord):
    if self.distance_to_coord(coord) <= Opts.max_dist_for_close_match:
      return True
    if self.location and type(self.location) is Division and \
        self.location.matches_coord(coord): return True
    return False

  # Determine the Naive Bayes distribution object for a given article:
  # Create and populate one if necessary.
  def find_nbdist(self):
    loc = self.location
    if loc and type(loc) is Division:
      if not loc.nbdist:
        loc.generate_nbdist()
      return loc.nbdist
    if not self.nbregion:
      self.nbregion = NBRegion.find_region_for_coord(self.coord)
    return self.nbregion.nbdist

  def compute_distribution(self, wordhash, total_tokens):
    if total_tokens == 0: return
    if self.counts:
      warning("Article %s already has counts for it!" % art)
    wordcounts = {}
    # Compute probabilities.  Use a very simple version of Good-Turing
    # smoothing where we assign to unseen words the probability mass of
    # words seen once, and adjust all other probs accordingly.
    oncecount = sum(1 for word in wordhash if wordhash[word] == 1)
    unseen_mass = float(oncecount)/total_tokens
    for (word,count) in wordhash.iteritems():
      ind = internasc(word)
      if ind not in GlobalDist.overall_word_probs:
        GlobalDist.num_word_types += 1
      # Record in overall_word_probs; note more tokens seen.
      GlobalDist.overall_word_probs[ind] += count
      GlobalDist.num_word_tokens += count
      # Record in current article's word->probability map.
      # wordcounts[ind] = float(count)/total_tokens*(1 - unseen_mass)
      wordcounts[ind] = count
    self.counts = make_sorted_list(wordcounts)
    self.unseen_mass = unseen_mass
    self.total_tokens = total_tokens
    if debug > 3:
      errprint("Title = %s, numtypes = %s, numtokens = %s, unseen_mass = %s"
               % (title, len(self.counts[0]), total_tokens, unseen_mass))


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

  loname = name.lower()

  # Look for any articles with same name (case-insensitive) as the location,
  # check for matches
  for art in ArticleTable.lower_name_to_articles[loname]:
    if check_match(loc, art): return art

  # Check whether there is a match for an article whose name is
  # a combination of the location's name and one of the divisions that
  # the location is in (e.g. "Augusta, Georgia" for a location named
  # "Augusta" in a second-level division "Georgia").
  if loc.div:
    for div in loc.div.path:
      for art in ArticleTable.lower_name_div_to_articles[(loname, div.lower())]:
        if check_match(loc, art): return art

  # See if there is a match with any of the articles whose short name
  # is the same as the location's name
  arts = ArticleTable.short_lower_name_to_articles[loname]
  if arts:
    goodarts = [art for art in arts if check_match(loc, art)]
    if len(goodarts) == 1:
      return goodarts[0] # One match
    elif len(goodarts) > 1:
      # Multiple matches: Sort by preference, return most preferred one
      if debug > 1:
        errprint("Warning: Saw %s toponym matches: %s" %
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
        errprint("Found article %s but dist %s > %s" %
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
    if art.coord and art.coord in loc:
      return True
    else:
      if debug > 1:
        if not art.coord:
          errprint("Found article %s but no coordinate, so not in location named %s, path %s" %
                   (art, loc.name, loc.path))
        else:
          errprint("Found article %s but not in location named %s, path %s" %
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
#                             Accumulate results                           #
############################################################################

class Eval(object):
  def __init__(self, incorrect_reasons):
    # Statistics on the types of instances processed
    # Total number of instances
    self.total_instances = 0
    self.correct_instances = 0
    self.incorrect_instances = 0
    self.incorrect_reasons = incorrect_reasons
    for (attrname, engname) in self.incorrect_reasons:
      setattr(self, attrname, 0)
    self.other_stats = intdict()
  
  def record_result(self, correct, reason=None):
    self.total_instances += 1
    if correct:
      self.correct_instances += 1
    else:
      self.incorrect_instances += 1
      if reason != None:
        setattr(self, reason, getattr(self, reason) + 1)

  def record_other_stat(self, othertype):
    self.other_stats[othertype] += 1

  def output_fraction(self, header, amount, total):
    if amount > total:
      warning("Something wrong: Fractional quantity %s greater than total %s"
              % (amount, total))
    if total == 0:
      percent = "indeterminate percent"
    else:
      percent = "%5.2f%%" % (100*float(amount)/total)
    errprint("%s = %s/%s = %s" % (header, amount, total, percent))

  def output_correct_results(self):
    self.output_fraction("Percent correct", self.correct_instances,
                         self.total_instances)

  def output_incorrect_results(self):
    self.output_fraction("Percent incorrect", self.incorrect_instances,
                         self.total_instances)
    for (reason, descr) in self.incorrect_reasons:
      self.output_fraction("  %s" % descr, getattr(self, reason),
                           self.total_instances)

  def output_other_stats(self):
    for (ty, count) in self.other_stats.iteritems():
      errprint("%s = %s" % (ty, count))

  def output_results(self):
    if not self.total_instances:
      warning("Strange, no instances found at all; perhaps --eval-format is incorrect?")
      return
    self.output_correct_results()
    self.output_incorrect_results()
    self.output_other_stats()

class EvalWithCandidateList(Eval):
  def __init__(self, incorrect_reasons, max_individual_candidates=5):
    super(EvalWithCandidateList, self).__init__(incorrect_reasons)
    self.max_individual_candidates = max_individual_candidates
    # Toponyms by number of candidates available
    self.total_instances_by_num_candidates = intdict()
    self.correct_instances_by_num_candidates = intdict()
    self.incorrect_instances_by_num_candidates = intdict()

  def record_result(self, correct, reason, num_arts):
    super(EvalWithCandidateList, self).record_result(correct, reason)
    self.total_instances_by_num_candidates[num_arts] += 1
    if correct:
      self.correct_instances_by_num_candidates[num_arts] += 1
    else:
      self.incorrect_instances_by_num_candidates[num_arts] += 1

  def output_table_by_num_candidates(self, table, total):
    for i in range(0, 1+self.max_individual_candidates):
      self.output_fraction("  With %d  candidates" % i, table[i], total)
    items = sum(val for key, val in table.iteritems()
                if key > self.max_individual_candidates)
    self.output_fraction("  With %d+ candidates" %
                           (1+self.max_individual_candidates),
                         items, total)

  def output_correct_results(self):
    super(EvalWithCandidateList, self).output_correct_results()
    self.output_table_by_num_candidates(
      self.correct_instances_by_num_candidates, self.correct_instances)

  def output_incorrect_results(self):
    super(EvalWithCandidateList, self).output_incorrect_results()
    self.output_table_by_num_candidates(
      self.incorrect_instances_by_num_candidates, self.incorrect_instances)

class EvalWithRank(Eval):
  def __init__(self, max_rank_for_credit=10):
    super(EvalWithRank, self).__init__(incorrect_reasons=[])
    self.max_rank_for_credit = max_rank_for_credit
    self.incorrect_by_exact_rank = intdict()
    self.correct_by_up_to_rank = intdict()
    self.incorrect_past_max_rank = 0
    self.total_credit = 0
  
  def record_result(self, rank):
    assert rank >= 1
    correct = rank == 1
    super(EvalWithRank, self).record_result(correct, reason=None)
    if rank <= self.max_rank_for_credit:
      self.total_credit += self.max_rank_for_credit + 1 - rank
      self.incorrect_by_exact_rank[rank] += 1
      for i in xrange(rank, self.max_rank_for_credit + 1):
        self.correct_by_up_to_rank[i] += 1
    else:
      self.incorrect_past_max_rank += 1

  def output_correct_results(self):
    super(EvalWithRank, self).output_correct_results()
    possible_credit = self.max_rank_for_credit*self.total_instances
    self.output_fraction("Percent correct with partial credit",
                         self.total_credit, possible_credit)
    for i in xrange(2, self.max_rank_for_credit + 1):
      self.output_fraction("  Correct is at or above rank %s" % i,
                           self.correct_by_up_to_rank[i], self.total_instances)

  def output_incorrect_results(self):
    super(EvalWithRank, self).output_incorrect_results()
    for i in xrange(2, self.max_rank_for_credit + 1):
      self.output_fraction("  Incorrect, with correct at rank %s" % i,
                           self.incorrect_by_exact_rank[i],
                           self.total_instances)
    self.output_fraction("  Incorrect, with correct not in top %s" %
                           self.max_rank_for_credit,
                           self.incorrect_past_max_rank, self.total_instances)


class Results(object):
  ####### Results for geotagging toponyms

  incorrect_geotag_toponym_reasons = [
    ('incorrect_with_no_candidates',
     'Incorrect, with no candidates'),
    ('incorrect_with_no_correct_candidates',
     'Incorrect, with candidates but no correct candidates'),
    ('incorrect_with_multiple_correct_candidates',
     'Incorrect, with multiple correct candidates'),
    ('incorrect_one_correct_candidate_missing_link_info',
     'Incorrect, with one correct candidate, but link info missing'),
    ('incorrect_one_correct_candidate',
     'Incorrect, with one correct candidate'),
  ]

  # Overall statistics
  all_toponym = EvalWithCandidateList(incorrect_geotag_toponym_reasons) 

  # Statistics when toponym not same as true name of location
  diff_surface = EvalWithCandidateList(incorrect_geotag_toponym_reasons)

  # Statistics when toponym not same as true name or short form of location
  diff_short = EvalWithCandidateList(incorrect_geotag_toponym_reasons)

  @staticmethod
  def record_geotag_toponym_result(correct, toponym, trueloc, reason,
                                   num_arts):
    Results.all_toponym.record_result(correct, reason, num_arts)
    if toponym != trueloc:
      Results.diff_surface.record_result(correct, reason, num_arts)
      (short, div) = compute_short_form(trueloc)
      if toponym != short:
        Results.diff_short.record_result(correct, reason, num_arts)

  @staticmethod
  def output_geotag_toponym_results():
    errprint("Results for all toponyms:")
    Results.all_toponym.output_results()
    errprint("")
    errprint("Results for toponyms when different from true location name:")
    Results.diff_surface.output_results()
    errprint("")
    errprint("Results for toponyms when different from either true location name")
    errprint("  or its short form:")
    Results.diff_short.output_results()


  ####### Results for geotagging documents/articles

  all_document = EvalWithRank()

  @staticmethod
  def record_geotag_document_result(rank):
    Results.all_document.record_result(rank)

  @staticmethod
  def record_geotag_document_other_stat(othertype):
    Results.all_document.record_other_stat(othertype)

  @staticmethod
  def output_geotag_document_results():
    errprint("Results for all documents/articles:")
    Results.all_document.output_results()


############################################################################
#                             Main geotagging code                         #
############################################################################

# Class of word in a file containing toponyms.  Fields:
#
#   word: The identity of the word.
#   is_stop: True if it is a stopword.
#   is_toponym: True if it is a toponym.
#   coord: For a toponym with specified ground-truth coordinate, the
#          coordinate.  Else, none.
#   location: True location if given, else None.
#   context: Vector including the word and 10 words on other side.
#   document: The document (article, etc.) of the word.  Useful when a single
#             file contains multiple such documents.
#
class GeogWord(object):
  __slots__ = ['word', 'is_stop', 'is_toponym', 'coord', 'location',
               'context', 'document']

  def __init__(self, word):
    self.word = word
    self.is_stop = False
    self.is_toponym = False
    self.coord = None
    self.location = None
    self.context = None
    self.document = None

class GeotagToponymStrategy(object):
  def need_context(self):
    pass

  def compute_score(self, geogword, art):
    pass

# Find each toponym explicitly mentioned as such and disambiguate it
# (find the correct geographic location) using the "link baseline", i.e.
# use the location with the highest number of incoming links.
class LinkBaselineStrategy(GeotagToponymStrategy):
  def need_context(self):
    return False

  def compute_score(self, geogword, art):
    return get_adjusted_incoming_links(art)

# Find each toponym explicitly mentioned as such and disambiguate it
# (find the correct geographic location) using Naive Bayes, possibly
# in conjunction with the baseline.
class NaiveBayesStrategy(GeotagToponymStrategy):
  def __init__(self, use_baseline):
    self.use_baseline = use_baseline

  def need_context(self):
    return True

  def compute_score(self, geogword, art):
    # FIXME FIXME!!! We are assuming that the baseline is 'internal-link',
    # regardless of its actual settings.
    thislinks = get_adjusted_incoming_links(art)

    if self.opts.naive_bayes_type == 'article':
      distobj = art
    else:
      distobj = art.find_nbdist()
    totalprob = 0.0
    total_word_weight = 0.0
    if not self.strategy.use_baseline:
      word_weight = 1.0
      baseline_weight = 0.0
    elif self.opts.naive_bayes_weighting == 'equal':
      word_weight = 1.0
      baseline_weight = 1.0
    else:
      baseline_weight = self.opts.baseline_weight
      word_weight = 1 - baseline_weight
    for (dist, word) in geogword.context:
      if not Opts.preserve_case_words: word = word.lower()
      wordprob = distobj.lookup_word(word)

      # Compute weight for each word, based on distance from toponym
      if self.opts.naive_bayes_weighting == 'equal' or \
         self.opts.naive_bayes_weighting == 'equal-words':
        thisweight = 1.0
      else:
        thisweight = 1.0/(1+dist)

      total_word_weight += thisweight
      totalprob += thisweight*log(wordprob)
    if debug > 0:
      errprint("Computed total word log-likelihood as %s" % totalprob)
    # Normalize probability according to the total word weight
    if total_word_weight > 0:
      totalprob /= total_word_weight
    # Combine word and prior (baseline) probability acccording to their
    # relative weights
    totalprob *= word_weight
    totalprob += baseline_weight*log(thislinks)
    if debug > 0:
      errprint("Computed total log-likelihood as %s" % totalprob)
    return totalprob

  def need_context(self):
    return True

  def compute_score(self, geogword, art):
    return get_adjusted_incoming_links(art)

# Abstract class for reading documents from a test file and evaluating on
# them.
class TestFileEvaluator(object):
  def __init__(self, opts):
    self.opts = opts
    self.documents_processed = 0

  def yield_documents(self, filename):
    pass

  def evaluate_document(self, doc):
    # Return True if document was actually processed and evaluated; False
    # is skipped.
    return True

  def output_results(self):
    pass

  def evaluate_and_output_results(self, files):
    status = StatusMessage('document')
    last_elapsed = 0
    last_processed = 0
    for filename in files:
      errprint("Processing evaluation file %s..." % filename)
      for doc in self.yield_documents(filename):
        errprint("Processing document: %s" % doc)
        if self.evaluate_document(doc):
          new_elapsed = status.item_processed()
          new_processed = status.num_processed()
          # If five minutes and ten documents have gone by, print out results
          if (new_elapsed - last_elapsed >= 300 and
              new_processed - last_processed >= 10):
            errprint("Results after %d documents:" % status.num_processed())
            self.output_results()
            last_elapsed = new_elapsed
            last_processed = new_processed
  
    errprint("Final results: All %d documents processed:" %
             status.num_processed())
    self.output_results()


class GeotagToponymEvaluator(TestFileEvaluator):
  def __init__(self, opts, strategy):
    super(GeotagToponymEvaluator, self).__init__(opts)
    self.strategy = strategy
    
  # Given an evaluation file, read in the words specified, including the
  # toponyms.  Mark each word with the "document" (e.g. article) that it's
  # within.
  def yield_geogwords(self, filename):
    pass

  # Retrieve the words yielded by yield_geowords() and separate by "document"
  # (e.g. article); yield each "document" as a list of such Geogword objects.
  # If self.compute_context, also generate the set of "context" words used for
  # disambiguation (some window, e.g. size 20, of words around each
  # toponym).
  def yield_documents(self, filename):
    def return_word(word):
      if word.is_toponym:
        if debug > 1:
          errprint("Saw loc %s with true coordinates %s, true location %s" %
                   (word.word, word.coord, word.location))
      else:
        if debug > 2:
          errprint("Non-toponym %s" % word.word)
      return word

    for k, g in itertools.groupby(self.yield_geogwords(filename),
                                  lambda word: word.document or 'foo'):
      if k:
        errprint("Processing document %s..." % k)
      results = [return_word(word) for word in g]

      # Now compute context for words
      nbcl = Opts.naive_bayes_context_len
      if self.strategy.need_context():
        # First determine whether each word is a stopword
        for i in xrange(len(results)):
          # If a word tagged as a toponym is homonymous with a stopword, it
          # still isn't a stopword.
          results[i].is_stop = (not results[i].coord and
                                results[i].word in stopwords)
        # Now generate context for toponyms
        for i in xrange(len(results)):
          if results[i].coord:
            # Select up to Opts.naive_bayes_context_len words on either side;
            # skip stopwords.  Associate each word with the distance away from
            # the toponym.
            minind = max(0,i-nbcl)
            maxind = min(len(results),i+nbcl+1)
            results[i].context = \
              [(dist, x.word)
               for (dist, x) in
                 zip(range(i-minind, i-maxind), results[minind:maxind])
               if x.word not in stopwords]

      yield [word for word in results if word.coord]

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

  def disambiguate_toponym(self, geogword):
    toponym = geogword.word
    coord = geogword.coord
    if not coord: return # If no ground-truth, skip it
    lotop = toponym.lower()
    bestscore = -1e308
    bestart = None
    articles = lower_toponym_to_article[lotop]
    locs = (lower_toponym_to_location[lotop] +
            lower_toponym_to_division[lotop])
    for loc in locs:
      if loc.match and loc.match not in articles:
        articles += [loc.match]
    if not articles:
      if debug > 0:
        errprint("Unable to find any possibilities for %s" % toponym)
      correct = False
    else:
      if debug > 0:
        errprint("Considering toponym %s, coordinates %s" %
                 (toponym, coord))
        errprint("For toponym %s, %d possible articles" %
                 (toponym, len(articles)))
      for art in articles:
        if debug > 0:
            errprint("Considering article %s" % art)
        if not art:
          if debug > 0:
            errprint("--> Location without matching article")
          continue
        else:
          thisscore = self.strategy.compute_score(geogword, art)
        if thisscore > bestscore:
          bestscore = thisscore
          bestart = art 
      if bestart:
        correct = bestart.matches_coord(coord)
      else:
        correct = False

    num_arts = len(articles)

    if correct:
      reason = None
    else:
      if num_arts == 0:
        reason = 'incorrect_with_no_candidates'
      else:
        good_arts = [art for art in articles if art.matches_coord(coord)]
        if not good_arts:
          reason = 'incorrect_with_no_correct_candidates'
        elif len(good_arts) > 1:
          reason = 'incorrect_with_multiple_correct_candidates'
        else:
          goodart = good_arts[0]
          if goodart.incoming_links == None:
            reason = 'incorrect_one_correct_candidate_missing_link_info'
          else:
            reason = 'incorrect_one_correct_candidate'

    errprint("Eval: Toponym %s (true: %s at %s),"
             % (toponym, geogword.location, coord), nonl=True)
    if correct:
      errprint("correct")
    else:
      errprint("incorrect, reason = %s" % reason)

    Results.record_geotag_toponym_result(correct, toponym, geogword.location,
                                         reason, num_arts)

    if debug > 0 and bestart:
      errprint("Best article = %s, score = %s, dist = %s, correct %s"
               % (bestart, bestscore, bestart.distance_to_coord(coord),
                  correct))

  def evaluate_document(self, doc):
    for geogword in doc:
       self.disambiguate_toponym(geogword)
    return True

  def output_results(self):
    Results.output_geotag_toponym_results()


def get_adjusted_incoming_links(art):
  thislinks = art.incoming_links
  if thislinks == None:
    thislinks = 0
    if debug > 0:
      warning("Strange, %s has no link count" % art)
  else:
    if debug > 0:
      errprint("--> Link count is %s" % thislinks)
  if thislinks == 0: # Whether from unknown count or count is actually zero
    thislinks = 0.01 # So we don't get errors from log(0)
  return thislinks

class TRCoNLLGeotagToponymEvaluator(GeotagToponymEvaluator):
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
  # Yield GeogWord objects, one per word.
  def yield_geogwords(self, filename):
    in_loc = False
    for line in uchompopen(filename, errors='replace'):
      try:
        (word, ty) = re.split('\t', line, 1)
        if word:
          if in_loc:
            in_loc = False
            yield wordstruct
          wordstruct = GeogWord(word)
          wordstruct.document = filename
          if ty.startswith('LOC'):
            in_loc = True
            wordstruct.is_toponym = True
          else:
            yield wordstruct
        elif in_loc and ty[0] == '>':
          (off, gaz, lat, long, fulltop) = re.split('\t', ty, 4)
          lat = float(lat)
          long = float(long)
          wordstruct.coord = Coord(lat, long)
          wordstruct.location = fulltop
      except Exception, exc:
        errprint("Bad line %s" % line)
        errprint("Exception is %s" % exc)
        if type(exc) is not ValueError:
          traceback.print_exc()
    if in_loc:
      yield wordstruct

class WikipediaGeotagToponymEvaluator(GeotagToponymEvaluator):
  def yield_geogwords(self, filename):
    title = None
    for line in uchompopen(filename, errors='replace'):
      if rematch('Article title: (.*)$', line):
        title = m_[1]
      elif rematch('Link: (.*)$', line):
        args = m_[1].split('|')
        trueart = args[0]
        linkword = trueart
        if len(args) > 1:
          linkword = args[1]
        word = GeogWord(linkword)
        word.is_toponym = True
        word.location = trueart
        word.document = title
        art = ArticleTable.lookup_article(trueart)
        if art:
          word.coord = art.coord
        yield word
      else:
        word = GeogWord(line)
        word.document = title
        yield word


class GeotagDocumentEvaluator(TestFileEvaluator):
  def output_results(self):
    Results.output_geotag_document_results()


class WikipediaGeotagDocumentEvaluator(GeotagDocumentEvaluator):
  def __init__(self, opts):
    super(WikipediaGeotagDocumentEvaluator, self).__init__(opts)
    NBRegion.generate_all_nonempty_regions()
    errprint("Number of non-empty regions: %s" % NBRegion.num_non_empty_regions)
    errprint("Number of empty regions: %s" % NBRegion.num_empty_regions)

  def yield_documents(self, filename):
    for art in ArticleTable.articles_by_split['dev']:
      assert art.split == 'dev'
      yield art

    #title = None
    #words = []
    #for line in uchompopen(filename, errors='replace'):
    #  if rematch('Article title: (.*)$', line):
    #    if title:
    #      yield (title, words)
    #    title = m_[1]
    #    words = []
    #  elif rematch('Link: (.*)$', line):
    #    args = m_[1].split('|')
    #    trueart = args[0]
    #    linkword = trueart
    #    if len(args) > 1:
    #      linkword = args[1]
    #    words.append(linkword)
    #  else:
    #    words.append(line)
    #if title:
    #  yield (title, words)

  def evaluate_document(self, article):
    if not article.finished:
      # This can (and does) happen when --max-time-per-stage is set,
      # so that the counts for many articles don't get read in.
      if not Opts.max_time_per_stage:
        warning("Can't evaluate unfinished article %s" % article)
      Results.record_geotag_document_other_stat('Skipped articles')
      return False
    truelat, truelong = coord_to_nbregion_indices(article.coord)
    true_nbreg = NBRegion.find_region_for_coord(article.coord)
    if debug > 0:
      errprint("Evaluating article %s with %s articles in region" %
               (article, true_nbreg.nbdist.num_arts))
    article_pq = PriorityQueue()
    for (inds, nbregion) in NBRegion.yield_all_nonempty_regions():
      if debug > 1:
        (latind, longind) = inds
        coord = region_indices_to_coord(latind, longind)
        errprint("Nonempty region at indices %s,%s = coord %s, num_articles = %s"
                 % (latind, longind, coord, nbregion.nbdist.num_arts))
      kldiv = article.kl_divergence(nbregion.nbdist,
                                    Opts.strategy == 'partial-kl-divergence')
      #errprint("For region %s, KL divergence = %s" % (inds, kldiv))
      article_pq.add_task(kldiv, inds)
    rank = 1
    raised = False
    while True:
      try:
        latind, longind = article_pq.get_top_priority()
        if latind == truelat and longind == truelong:
          Results.record_geotag_document_result(rank)
          break
        rank += 1
      except IndexError:
        raised = True
        Results.record_geotag_document_result(rank)
        Results.record_geotag_document_other_stat('No training articles in region')
        break
    errprint("For article %s, true region at rank %s" %
             (article, rank))
    assert raised == (true_nbreg.nbdist.num_arts == 0)
    return True


############################################################################
#                               Process files                              #
############################################################################

# Read in the list of stopwords from the given filename.
def read_stopwords(filename):
  errprint("Reading stopwords from %s..." % filename)
  for line in uchompopen(filename):
    stopwords.add(line)


def read_article_data(filename):
  redirects = []

  def process(art):
    if art.namespace != 'Main':
      return
    if art.redir:
      redirects.append(art)
    elif art.coord:
      ArticleTable.record_article(art.title, art)
      if art.split == 'training':
        NBRegion.add_article_to_region(art)

  read_article_data_file(filename, process, article_type=NBArticle,
                         max_time_per_stage=Opts.max_time_per_stage)

  for x in redirects:
    redart = ArticleTable.lookup_article(x.redir)
    if redart:
      ArticleTable.record_article(x.title, redart, is_redirect=True)


# Parse the result of a previous run of --output-counts and generate
# a unigram distribution for Naive Bayes matching.  We do a simple version
# of Good-Turing smoothing where we assign probability mass to unseen
# words equal to the probability mass of all words seen once, and rescale
# the remaining probabilities accordingly.

def read_word_counts(filename):

  def one_article_probs():
    if total_tokens == 0: return
    art = ArticleTable.lookup_article(title)
    if not art:
      warning("Skipping article %s, not in table" % title)
      return
    art.compute_distribution(wordhash, total_tokens)

  errprint("Reading word counts from %s..." % filename)
  status = StatusMessage('article')
  total_tokens = 0

  title = None
  for line in uchompopen(filename):
    if line.startswith('Article title: '):
      m = re.match('Article title: (.*)$', line)
      if title:
        one_article_probs()
      # Stop if we've reached the maximum
      if status.item_processed() >= Opts.max_time_per_stage:
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
      if not Opts.preserve_case_words: word = word.lower()
      count = int(m.group(2))
      if word in stopwords and Opts.ignore_stopwords_in_article_dists: continue
      word = internasc(word)
      total_tokens += count
      wordhash[word] += count
  else:
    one_article_probs()

  GlobalDist.finish_article_distributions()


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
      errprint("Skipping location %s (div %s/%s/%s) without coordinates" %
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
    errprint("Saw location %s (div %s/%s/%s) with coordinates %s" %
             (loc.name, div1, div2, div3, loc.coord))

  # Record the location.  For each name for the location (its
  # canonical name and all alternates), add the location to the list of
  # locations associated with the name.  Record the name in lowercase
  # for ease in matching.
  for name in [loc.name] + loc.altnames:
    loname = name.lower()
    if debug > 1:
      errprint("Noting lower_toponym_to_location for toponym %s, canonical name %s"
               % (name, loc.name))
    lower_toponym_to_location[loname] += [loc]

  # We start out looking for articles whose distance is very close,
  # then widen until we reach Opts.max_dist_for_close_match.
  maxdist = 5
  while maxdist <= Opts.max_dist_for_close_match:
    match = find_match_for_locality(loc, maxdist)
    if match: break
    maxdist *= 2

  if not match: 
    if debug > 1:
      errprint("Unmatched name %s" % loc.name)
    return
  
  # Record the match.
  loc.match = match
  match.location = loc
  if debug > 1:
    errprint("Matched location %s (coord %s) with article %s, dist=%s"
             % (loc.name, loc.coord, match,
                spheredist(loc.coord, match.coord)))

# Read in the data from the World gazetteer in FILENAME and find the
# Wikipedia article matching each entry in the gazetteer.  For localities,
# add them to the region-map that covers the earth if ADD_TO_REGION_MAP is
# true.
def read_world_gazetteer_and_match(filename):
  errprint("Matching gazetteer entries in %s..." % filename)
  status = StatusMessage('gazetteer entry')

  # Match each entry in the gazetteer
  for line in uchompopen(filename):
    if debug > 1:
      errprint("Processing line: %s" % line)
    match_world_gazetteer_entry(line)
    if status.item_processed() >= Opts.max_time_per_stage:
      break

  for division in path_to_division.itervalues():
    if debug > 1:
      errprint("Processing division named %s, path %s"
               % (division.name, division.path))
    division.compute_boundary()
    match = find_match_for_division(division)
    if match:
      if debug > 1:
        errprint("Matched article %s for division %s, path %s" %
                 (match, division.name, division.path))
      division.match = match
      match.location = division
    else:
      if debug > 1:
        errprint("Couldn't find match for division %s, path %s" %
                 (division.name, division.path))

# If given a directory, yield all the files in the directory; else just
# yield the file.
def yield_directory_files(dir):
  if os.path.isdir(dir): 
    for fname in os.listdir(dir):
      fullname = os.path.join(dir, fname)
      yield fullname
  else:
    yield dir
  
# Given an evaluation file, count the toponyms seen and add to the global count
# in toponyms_seen_in_eval_files.
def count_toponyms_in_file(fname):
  def count_toponyms(geogword):
    toponyms_seen_in_eval_files[geogword.word.lower()] += 1
  process_eval_file(fname, count_toponyms, compute_context=False,
                    only_toponyms=True)

############################################################################
#                                  Main code                               #
############################################################################

class WikiDisambigProgram(NLPProgram):

  def populate_options(self, op):
    op.add_option("-t", "--gazetteer-type", type='choice', default="world",
                  choices=['world', 'db'],
                  help="""Type of gazetteer file specified using --gazetteer;
default '%default'.""")
    op.add_option("-s", "--stopwords-file",
                  help="""File containing list of stopwords.""",
                  metavar="FILE")
    op.add_option("-a", "--article-data-file",
                  help="""File containing info about Wikipedia articles.""",
                  metavar="FILE")
    op.add_option("-g", "--gazetteer-file",
                  help="""File containing gazetteer information to match.""",
                  metavar="FILE")
    op.add_option("-c", "--counts-file",
                  help="""File containing output from a prior run of
--output-counts, listing for each article the words in the article and
associated counts.""",
                  metavar="FILE")
    op.add_option("-p", "--pickle-file",
                  help="""Serialize the result of processing the word-coords
file to the given file.""",
                  metavar="FILE")
    op.add_option("-u", "--unpickle-file",
                  help="""Read the result of serializing the word-coords file
to the given file.""",
                  metavar="FILE")
    op.add_option("-e", "--eval-file",
                  help="""File or directory containing files to evaluate on.
Each file is read in and then disambiguation is performed.""",
                  metavar="FILE")
    op.add_option("-f", "--eval-format", type='choice',
                  default="wiki", choices=['tr-conll', 'wiki', 'raw-text'],
                  help="""Format of evaluation file(s).  Default '%default'.""")
    op.add_option("--preserve-case-words", action='store_true',
                  default=False,
                  help="""Don't fold the case of words used to compute and
match against article distributions.  Note that this does not apply to
toponyms; currently, toponyms are always matched case-insensitively.""")
    op.add_option("--ignore-stopwords-in-article-dists", action='store_true',
                  default=False,
                  help="""Ignore stopwords when computing word
distributions.""")
    op.add_option("--max-dist-for-close-match", type='float', default=80,
                  help="""Maximum number of miles allowed when looking for a
close match.  Default %default.""")
    op.add_option("--max-dist-for-outliers", type='float', default=200,
                  help="""Maximum number of miles allowed between a point and
any others in a division.  Points farther away than this are ignored as
"outliers" (possible errors, etc.).  Default %default.""")
    op.add_option("--naive-bayes-context-len", type='int', default=10,
                  help="""Number of words on either side of a toponym to use
in Naive Bayes matching.  Default %default.""")
    op.add_option("-m", "--mode", type='choice', default='match-only',
                  choices=['geotag-toponyms',
                           'geotag-documents',
                           'match-only', 'pickle-only'],
                  help="""Action to perform.

'match-only' means to only do the stage that involves finding matches between
gazetteer locations and Wikipedia articles (mostly useful when debugging
output is enabled).

'pickle-only' means only to generate the pickled version of the data, for
reading in by a separate process (e.g. the Java code).

'geotag-documnts' finds the proper location for each document (or article)
in the test set.

'geotag-toponyms' finds the proper location for each toponym in the test set.
The test set is specified by --eval-file.  Default '%default'.""")
    op.add_option("--strategy", type='choice', default='baseline',
                  choices=['baseline',
                           'kl-divergence',
                           'partial-kl-divergence',
                           'naive-bayes-with-baseline',
                           'naive-bayes-no-baseline'],
                  help="""Strategy to use for Geotagging.
'baseline' means just use the baseline strategy (see --baseline-type);
'naive-bayes-with-baseline' means also use the words around the toponym to
be disambiguated, in a Naive-Bayes scheme, using the baseline as the prior
probability; 'naive-bayes-no-baseline' means use uniform prior probability.
Default '%default'.""")
    op.add_option("--baseline-type", type='choice',
                  default="internal-link",
                  choices=['internal-link', 'random', 'num-articles'],
                  help="""Strategy to use to compute the baseline.

'internal-link' means use number of internal links pointing to the article or
region.

'random' means choose randomly.

'num-articles' (only in region-type matching) means use number of articles
in region.

Default '%default'.""")
    op.add_option("--baseline-weight", type='float', metavar="WEIGHT",
                  default=0.5,
                  help="""Relative weight to assign to the baseline (prior
probability) when doing weighted Naive Bayes.  Default %default.""")
    op.add_option("--naive-bayes-weighting", type='choice',
                  default="equal",
                  choices=['equal', 'equal-words', 'distance-weighted'],
                  help="""Strategy for weighting the different probabilities
that go into Naive Bayes.  If 'equal', do pure Naive Bayes, weighting the
prior probability (baseline) and all word probabilities the same.  If
'equal-words', weight all the words the same but weight the baseline
according to --baseline-weight, assigning the remainder to the words.  If
'distance-weighted', use the --baseline-weight for the prior probability
and weight the words according to distance from the toponym.""")
    op.add_option("--width-of-nbregion", type='int', default=1,
                  help="""Width of the Naive Bayes region used for region-based
Naive Bayes disambiguation, in tiling regions.  Default %default.""")
    op.add_option("-r", "--region-size", type='float', default=100.0,
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

  def implement_main(self, opts, op, args):
    global Opts
    Opts = opts
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
      coord_to_tiling_region_indices(Coord(maximum_latitude,
                                           maximum_longitude))
    minimum_latind, minimum_longind = \
      coord_to_tiling_region_indices(Coord(minimum_latitude,
                                           minimum_longitude))

    if opts.width_of_nbregion <= 0:
      op.error("Width of Naive Bayes region must be positive")

    ### Start reading in the files and operating on them ###

    if opts.mode == 'pickle-only' or opts.mode.startswith('geotag'):
      self.need('stopwords_file')
      read_stopwords(opts.stopwords_file)
      if opts.mode == 'geotag-toponyms' and opts.strategy == 'baseline':
        pass
      elif not opts.unpickle_file and not opts.counts_file:
        op.error("Must specify either unpickle file or counts file")

    if opts.mode != 'pickle-only':
      self.need('gazetteer_file')

    if opts.eval_format == 'raw-text':
      # FIXME!!!!
      op.error("Raw-text reading not implemented yet")

    if opts.mode == 'geotag-documents' and opts.eval_format == 'wiki':
      pass # No need for evaluation file, uses the counts file
    # FIXME!! Fix this limitation.  Should allow raw text files.
    elif opts.mode == 'geotag-documents' and opts.eval_format != 'wiki':
      op.error("Can only geotag articles in Wikipedia format")
    elif opts.mode.startswith('geotag'):
      self.need('eval_file', 'evaluation file(s)')

    if opts.mode == 'pickle-only':
      if not opts.pickle_file:
        self.need('pickle_file')

    self.need('article_data_file')
    read_article_data(opts.article_data_file)

    #errprint("Processing evaluation file(s) %s for toponym counts..." % opts.eval_file)
    #process_dir_files(opts.eval_file, count_toponyms_in_file)
    #errprint("Number of toponyms seen: %s" % len(toponyms_seen_in_eval_files))
    #errprint("Number of toponyms seen more than once: %s" % \
    #  len([foo for (foo,count) in toponyms_seen_in_eval_files.iteritems() if
    #       count > 1]))
    #output_reverse_sorted_table(toponyms_seen_in_eval_files,
    #                            outfile=sys.stderr)

    # Read in (or unpickle) and maybe pickle the words-counts file
    if opts.mode == 'pickle-only' or opts.mode.startswith('geotag'):
      if opts.unpickle_file:
        global article_probs
        infile = open(opts.unpickle_file)
        #FIXME: article_probs = cPickle.load(infile)
        infile.close()
      elif opts.counts_file:
        read_word_counts(opts.counts_file)
      if opts.pickle_file:
        outfile = open(opts.pickle_file, "w")
        #FIXME: cPickle.dump(article_probs, outfile)
        outfile.close()

    if opts.mode == 'pickle-only': return

    read_world_gazetteer_and_match(opts.gazetteer_file)

    if opts.mode == 'match-only': return

    if opts.mode == 'geotag-toponyms':
      # Generate strategy object
      if opts.strategy == 'baseline':
        if opts.baseline_type == 'internal-link':
          strategy = LinkBaselineStrategy()
        else:
          # FIXME!!!!!
          op.error("Non-internal-link baseline strategies not implemented")
      elif opts.strategy == 'naive-bayes-no-baseline':
        strategy = NaiveBayesStrategy(use_baseline=False)
      else:
        strategy = NaiveBayesStrategy(use_baseline=True)

      # Generate reader object
      if opts.eval_format == 'tr-conll':
        evalobj = TRCoNLLGeotagToponymEvaluator(opts, strategy)
      else:
        evalobj = WikipediaGeotagToponymEvaluator(opts, strategy)
    else:
      evalobj = WikipediaGeotagDocumentEvaluator(opts)
      # Hack: When running in --mode=geotag-documents and --eval-format=wiki,
      # we don't need an eval file because we use the article counts we've
      # already loaded.  But we will get an error if we don't set this to
      # a file.
      if not opts.eval_file:
        opts.eval_file = opts.article_data_file

    errprint("Processing evaluation file/dir %s..." % opts.eval_file)
    evalobj.evaluate_and_output_results(yield_directory_files(opts.eval_file))

WikiDisambigProgram()
