#!/usr/bin/env python

#######
####### disambig.py
#######
####### Copyright (c) 2010 Ben Wing.
#######

import sys
import re
import os
import os.path
import math
from math import log
import collections
import traceback
import cPickle
from itertools import *
import random
import gc
from nlputil import *
from process_article_data import *
from kl_divergence import *
from word_distribution import *
import xml.dom.minidom as md
import time

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

# Debug params.  Different params indicate different info to output.
# Specified using --debug.  Multiple params are separated by commas or spaces.
# Params can be boolean, if given alone, or valueful, if given as
# PARAM=VALUE.  Certain params are list-valued; multiple values are specified
# by including the parameter multiple times, or by separating values by
# a semicolon or colon.
debug = listdict()

list_debug_params = []

# Register a list-valued debug param.
def register_list_debug_param(param):
  list_debug_params.append(param)

############################################################################
#                               Structures                                 #
############################################################################

def print_structure(struct, indent=0):
  indstr = ' '*indent
  if not struct or not isinstance(struct, list):
    errprint("%s%s" % (indstr, struct))
  else:
    if isinstance(struct[0], basestring):
      errprint("%s%s:" % (indstr, struct[0]))
      indstr += '  '
      indent += 2
      struct = struct[1:]
    for s in struct:
      if isinstance(s, list):
        print_structure(s, indent + 2)
      elif isinstance(s, tuple):
        (key, val) = s
        if isinstance(val, list):
          errprint("%s%s:" % (indstr, key))
          print_structure(val, indent + 2)
        else:
          errprint("%s%s: %s" % (indstr, key, val))
      else:
        errprint("%s%s" % (indstr, s))

############################################################################
#                       Coordinates and regions                            #
############################################################################

# The coordinates of a point are spherical coordinates, indicating a
# latitude and longitude.  Latitude ranges from -90 degrees (south) to
# +90 degrees (north), with the Equator at 0 degrees.  Longitude ranges
# from -180 degrees (west) to +179.9999999.... degrees (east). -180 and +180
# degrees correspond to the same north-south parallel, and we arbitrarily
# choose -180 degrees over +180 degrees.  0 degrees longitude has been
# arbitrarily chosen as the north-south parallel that passes through
# Greenwich, England (near London).  Note that longitude wraps around, but
# latitude does not.  Furthermore, the distance between latitude lines is
# always the same (about 69 miles per degree), but the distance between
# longitude lines varies according to the latitude, ranging from about
# 69 miles per degree at the Equator to 0 miles at the North and South Pole.
#
# We divide the earth's surface into "tiling regions", using the value
# of --region-size, which is specified in miles; we convert it to degrees
# using 'miles_per_degree', which is derived from the value for the
# Earth's radius in miles.  In addition, we form a square of tiling regions
# in order to create a "statistical region", which is used to compute a
# distribution over words.  The numbe of tiling regions on a side is
# determined by --width-of-stat-region.  Note that if this is greater than
# 1, different statistical regions will overlap.
#
# To specify a region, we use region indices, which are derived from
# coordinates by dividing by degrees_per_region.  Hence, if for example
# degrees_per_region is 2, then region indices are in the range [-45,+45]
# for latitude and [-90,+90) for longitude.  In general, an arbitrary
# coordinate will have fractional region indices; however, the region
# indices of the corners of a region (tiling or statistical) will be
# integers.  Normally, we use the southwest corner to specify a region.
#
# Near the edges, tiling regions may be truncated.  Statistical regions
# will wrap around longitudinally, and will still have the same number
# of tiling regions, but may be smaller.

# Size of each region in degrees.  Determined by the --region-size option
# (however, that option is expressed in miles).
degrees_per_region = 0.0

# Minimum, maximum latitude/longitude in indices (integers used to index the
# set of regions that tile the earth)
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

# Height of highest bar in meters
kml_max_height = 2000000

# Minimum and maximum colors
kml_mincolor = (255, 255, 0)    # yellow
kml_maxcolor = (255, 0, 0)      # red

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

  def __repr__(self):
    return "Boundary(%s)" % self.__str__()

  def struct(self):
    return ["Boundary", ('boundary', '%s-%s' % (self.botleft, self.topright))]

  def __contains__(self, coord):
    if not (coord.lat >= self.botleft.lat and
            coord.lat <= self.topright.lat):
      return False
    if self.botleft.long <= self.topright.long:
      return (coord.long >= self.botleft.long and
              coord.long <= self.topright.long)
    else:
      # Handle case where boundary overlaps the date line.
      return ((coord.long >= self.botleft.long and
               coord.long <= self.topright.long + 360.) or
              (coord.long >= self.botleft.long - 360. and
               coord.long <= self.topright.long))

  def square_area(self):
    (lat1, lon1) = (self.botleft.lat, self.botleft.long)
    (lat2, lon2) = (self.topright.lat, self.topright.long)
    lat1 = (lat1 / 180.) * math.pi
    lat2 = (lat2 / 180.) * math.pi
    lon1 = (lon1 / 180.) * math.pi
    lon2 = (lon2 / 180.) * math.pi

    return ((earth_radius_in_miles ** 2) *
            abs(math.sin(lat1) - math.sin(lat2)) *
            abs(lon1 - lon2))

  # Iterate over the regions that overlap the boundary.  If
  # 'nonempty_word_dist' is True, only yield regions with a non-empty
  # word distribution; else, yield all non-empty regions.
  def iter_nonempty_tiling_regions(self):
    (latind1, longind1) = coord_to_tiling_region_indices(self.botleft)
    (latind2, longind2) = coord_to_tiling_region_indices(self.topright)
    for i in xrange(latind1, latind2 + 1):
      if longind1 <= longind2:
        it = xrange(longind1, longind2 + 1)
      else:
        it = chain(xrange(longind1, maximum_longind + 1),
                   xrange(minimum_longind, longind2 + 1))
      for j in it:
        if (i, j) in StatRegion.tiling_region_to_articles:
          yield (i, j)

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

# Convert a coordinate to the indices of the southwest corner of the
# corresponding tiling region.
def coord_to_tiling_region_indices(coord):
  latind = int(math.floor(coord.lat / degrees_per_region))
  longind = int(math.floor(coord.long / degrees_per_region))
  return (latind, longind)

# Convert a coordinate to the indices of the southwest corner of the
# corresponding statistical region.
def coord_to_stat_region_indices(coord):
  # When Opts.width_of_stat_region = 1, don't subtract anything.
  # When Opts.width_of_stat_region = 2, subtract 0.5*degrees_per_region.
  # When Opts.width_of_stat_region = 3, subtract degrees_per_region.
  # When Opts.width_of_stat_region = 4, subtract 1.5*degrees_per_region.
  # In general, subtract (Opts.width_of_stat_region-1)/2.0*degrees_per_region.

  # Compute the indices of the southwest region
  subval = (Opts.width_of_stat_region-1)/2.0*degrees_per_region
  lat = coord.lat - subval
  long = coord.long - subval

  latind, longind = coord_to_tiling_region_indices(Coord(lat, long))
  return (latind, longind)

# Convert region indices to the corresponding coordinate.  This can also
# be used to find the coordinate of the southwest corner of a tiling region
# or statistical region, as both are identified by the region indices of
# their southwest corner.
def region_indices_to_coord(latind, longind, coerce_within_bounds=False):
  return Coord(latind * degrees_per_region, longind * degrees_per_region,
      coerce_within_bounds=coerce_within_bounds)

# Add 'offset' to both latind and longind and then convert to a
# coordinate.  Coerce the coordinate to be within bounds.
def offset_region_indices_to_coord(latind, longind, offset):
  return region_indices_to_coord(latind + offset, longind + offset,
      coerce_within_bounds=True)

# Convert region indices of a tiling region to the coordinate of the
# near (i.e. southwest) corner of the region.
def tiling_region_indices_to_near_corner_coord(latind, longind):
  return region_indices_to_coord(latind, longind)

# Convert region indices of a tiling region to the coordinate of the
# center of the region.
def tiling_region_indices_to_center_coord(latind, longind):
  return offset_region_indices_to_coord(latind, longind, 0.5)

# Convert region indices of a tiling region to the coordinate of the
# far (i.e. northeast) corner of the region.
def tiling_region_indices_to_far_corner_coord(latind, longind):
  return offset_region_indices_to_coord(latind, longind, 1)

# Convert region indices of a tiling region to the coordinate of the
# near (i.e. southwest) corner of the region.
def stat_region_indices_to_near_corner_coord(latind, longind):
  return region_indices_to_coord(latind, longind)

# Convert region indices of a statistical region to the coordinate of the
# center of the region.
def stat_region_indices_to_center_coord(latind, longind):
  return offset_region_indices_to_coord(latind, longind,
      Opts.width_of_stat_region/2.0)

# Convert region indices of a statistical region to the coordinate of the
# far (i.e. northeast) corner of the region.
def stat_region_indices_to_far_corner_coord(latind, longind):
  return offset_region_indices_to_coord(latind, longind,
      Opts.width_of_stat_region)

# Convert region indices of a statistical region to the coordinate of the
# northwest corner of the region.
def stat_region_indices_to_nw_corner_coord(latind, longind):
  return region_indices_to_coord(latind + Opts.width_of_stat_region, longind,
      coerce_within_bounds=True)

# Convert region indices of a statistical region to the coordinate of the
# southeast corner of the region.
def stat_region_indices_to_se_corner_coord(latind, longind):
  return region_indices_to_coord(latind, longind + Opts.width_of_stat_region,
      coerce_within_bounds=True)

# Convert region indices of a statistical region to the coordinate of the
# southwest corner of the region.
def stat_region_indices_to_sw_corner_coord(latind, longind):
  return stat_region_indices_to_near_corner_coord(latind, longind)

# Convert region indices of a statistical region to the coordinate of the
# northeast corner of the region.
def stat_region_indices_to_ne_corner_coord(latind, longind):
  return stat_region_indices_to_far_corner_coord(latind, longind)

############################################################################
#                             Word distributions                           #
############################################################################

# Distribution over words corresponding to a statistical region.  The following
# fields are defined in addition to base class fields:
#
#   articles: Articles used in computing the distribution.
#   num_arts_for_word_dist: Number of articles included in word distribution.
#   num_arts_for_links: Number of articles included in incoming-link
#                       computation.
#   incoming_links: Total number of incoming links, or None if unknown.

class RegionWordDist(WordDist):
  __slots__ = WordDist.__slots__ + ['num_arts_for_links',
                                    'incoming_links',
                                    'num_arts_for_word_dist']

  def __init__(self):
    super(RegionWordDist, self).__init__()
    self.num_arts_for_links = 0
    self.incoming_links = 0
    self.num_arts_for_word_dist = 0

  def is_empty_for_word_dist(self):
    return self.num_arts_for_word_dist == 0

  def is_empty(self):
    return self.num_arts_for_links == 0

  # Add the given articles to the total distribution seen so far
  def add_articles(self, articles):
    incoming_links = 0
    if debug['lots']:
      errprint("Region dist, number of articles = %s" % num_arts_for_word_dist)
    old_total_tokens = self.total_tokens
    num_arts_for_links = 0
    num_arts_for_word_dist = 0
    incoming_links = 0
    for art in articles:
      if art.incoming_links: # Might be None, for unknown link count
        incoming_links += art.incoming_links
      num_arts_for_links += 1
      if not art.dist:
        if Opts.max_time_per_stage == 0 and Opts.num_training_docs == 0:
          warning("Saw article %s without distribution" % art)
        continue
      assert art.dist.finished
      if art.split != 'training':
        continue
      self.add_word_distribution(art.dist)
      num_arts_for_word_dist += 1
    self.num_arts_for_links += num_arts_for_links
    self.num_arts_for_word_dist += num_arts_for_word_dist
    self.incoming_links += incoming_links
    if num_arts_for_word_dist and debug['lots']:
      errprint("""--> Finished processing, number articles handled = %s/%s,
    skipped articles = %s, total tokens = %s/%s, incoming links = %s/%s""" %
               (num_arts_for_word_dist,
                self.num_arts_for_word_dist,
                num_arts_for_links - num_arts_for_word_dist,
                self.total_tokens - old_total_tokens,
                self.total_tokens, incoming_links, self.incoming_links))

  def add_locations(self, locs):
    arts = [loc.match for loc in locs if loc.match]
    self.add_articles(arts)

  def finish(self, minimum_word_count=0):
    super(RegionWordDist, self).finish(minimum_word_count=minimum_word_count)

    if debug['lots']:
      errprint("""For region dist, num articles = %s, total tokens = %s,
    unseen_mass = %s, incoming links = %s, overall unseen mass = %s""" %
               (self.num_arts_for_word_dist, self.total_tokens,
                self.unseen_mass, self.incoming_links,
                self.overall_unseen_mass))

  # For a document described by its distribution 'worddist', return the
  # log probability log p(worddist|reg) using a Naive Bayes algorithm.  Use 
  # options from 'opts' to determine specifics of how the algorithm works
  # (how to compute a prior probability and how to weight it).
  def get_nbayes_logprob(self, worddist, opts):
    logprob = 0.0
    for (word, count) in worddist.counts.iteritems():
      val = self.lookup_word(word)
      if val <= 0:
        # FIXME: Need to figure out why this happens (perhaps the word was
        # never seen anywhere in the training data? But I thought we have
        # a case to handle that) and what to do instead.
        errprint("Warning! For word %s, prob %s out of range" % (word, val))
      else:
        logprob += log(val)
    # FIXME: Also use baseline (prior probability)
    return logprob


############################################################################
#                             Region distributions                         #
############################################################################

# Distribution over regions, as might be attached to a word.  If we have a
# set of regions, each with a word distribution, then we can imagine
# conceptually inverting the process to generate a region distribution over
# words.  Basically, for a given word, look to see what its probability is
# in all regions; normalize, and we have a region distribution.

# Fields defined:
#
#   word: Word for which the region is computed
#   regionprobs: Hash table listing probabilities associated with regions

class RegionDist(object):
  __slots__ = ['word', 'regionprobs', 'normalized']

  cached_dists = None

  def __init__(self, word=None, regionprobs=None):
    # It's expensive to compute the value for a given word so we cache word
    # distributions.
    if regionprobs:
      self.regionprobs = regionprobs
    else:
      self.regionprobs = {}
    if not word:
      return
    self.word = word
    totalprob = 0.0
    # Compute and store un-normalized probabilities for all regions
    for reg in \
        StatRegion.iter_nonempty_regions(nonempty_word_dist=True):
      prob = reg.worddist.lookup_word(word)
      # Another way of handling zero probabilities.
      ## Zero probabilities are just a bad idea.  They lead to all sorts of
      ## pathologies when trying to do things like "normalize".
      #if prob == 0.0:
      #  prob = 1e-50
      self.regionprobs[reg] = prob
      totalprob += prob
    # Normalize the probabilities; but if all probabilities are 0, then
    # we can't normalize, so leave as-is. (FIXME When can this happen?
    # It does happen when you use --mode=generate-kml and specify words
    # that aren't seen.  In other circumstances, the smoothing ought to
    # ensure that 0 probabilities don't exist?  Anything else I missed?)
    if totalprob:
      self.normalized = True
      for (reg, prob) in self.regionprobs.iteritems():
        self.regionprobs[reg] /= totalprob
    else:
      self.normalized = False

  def get_ranked_regions(self):
    return [(reg, prob) for (reg, prob) in sorted(self.regionprobs.iteritems(),
                                          key=lambda x:x[1], reverse=True)]

  # Return a region distribution over a given word, using a least-recently-used
  # cache to optimize access.
  @classmethod
  def get_region_dist(cls, word):
    if not cls.cached_dists:
      cls.cached_dists = LRUCache(maxsize=Opts.lru_cache_size)
    dist = cls.cached_dists.get(word, None)
    if not dist:
      dist = RegionDist(word)
      cls.cached_dists[word] = dist
    return dist

  # Return a region distribution over a distribution over words.  This works
  # by adding up the distributions of the individual words, weighting by
  # the count of the each word.
  @classmethod
  def get_region_dist_for_word_dist(cls, worddist):
    regprobs = floatdict()
    for (word, count) in worddist.counts.iteritems():
      dist = cls.get_region_dist(word)
      for (reg, prob) in dist.regionprobs.iteritems():
        regprobs[reg] += count*prob
    totalprob = sum(regprobs.itervalues())
    for (reg, prob) in regprobs.iteritems():
      regprobs[reg] /= totalprob
    return RegionDist(regionprobs=regprobs)

  # Convert region to a KML file showing the distribution
  def generate_kml_file(self, filename):

    if Opts.kml_transform == 'log':
      def xform(x): return log(x)
    elif Opts.kml_transform == 'logsquared':
      def xform(x): return -log(x)*log(x)
    else:
      def xform(x): return x

    minxformprob = xform(min(prob for prob in self.regionprobs.itervalues()))
    maxxformprob = xform(max(prob for prob in self.regionprobs.itervalues()))

    # Generate KML for a single region
    def one_reg_kml(reg, prob):
      latind, longind = reg.latind, reg.longind
      offprob = xform(prob) - minxformprob
      fracprob = offprob / (maxxformprob - minxformprob)
      swcoord = stat_region_indices_to_near_corner_coord(latind, longind)
      necoord = stat_region_indices_to_far_corner_coord(latind, longind)
      nwcoord = Coord(necoord.lat, swcoord.long)
      secoord = Coord(swcoord.lat, necoord.long)
      center = stat_region_indices_to_center_coord(latind, longind)
      coordtext = '\n'
      for coord in (swcoord, nwcoord, necoord, secoord, swcoord):
        lat = (center.lat + coord.lat) / 2
        long = (center.long + coord.long) / 2
        coordtext += '%s,%s,%s\n' % (long, lat, fracprob*kml_max_height)
      if reg.most_popular_article:
        name = reg.most_popular_article.title
      else:
        name = ''

      # Placemark indicating name
      yield ['Placemark', [],
          ['name', [], name],
          ['Region', [],
            ['LatLonAltBox', [],
              ['north', [], '%s' % ((center.lat + necoord.lat) / 2)],
              ['south', [], '%s' % ((center.lat + swcoord.lat) / 2)],
              ['east', [], '%s' % ((center.long + necoord.long) / 2)],
              ['west', [], '%s' % ((center.long + swcoord.long) / 2)]],
            ['Lod', [],
              ['minLodPixels', [], '16']]],
          ['styleURL', [], '#bar'],
          ['Point', [],
            ['coordinates', [], '%s,%s' % (center.long, center.lat)]]
          ]

      # Interpolate colors
      color = [0, 0, 0]
      for i in range(3):
        color[i] = (kml_mincolor[i] +
            fracprob*(kml_maxcolor[i] - kml_mincolor[i]))
      # Original color dc0155ff
      #rgbcolor = 'dc0155ff'
      rgbcolor = 'ff%02x%02x%02x' % tuple(reversed(color))

      # Yield cylinder indicating probability by height and color
      yield ['Placemark', [],
          ['name', [], '%s POLYGON' % name],
          ['styleUrl', [], '#bar'],
          ['Style', [],
            ['PolyStyle', [],
              ['color', [], rgbcolor],
              ['colorMode', [], 'normal']]],
          ['Polygon', [],
            ['extrude', [], '1'],
            ['tessellate', [], '1'],
            ['altitudeMode', [], 'relativeToGround'],
            ['outerBoundaryIs', [],
              ['LinearRing', [],
                ['coordinates', [], coordtext]]]
            ]
          ]

    def yield_reg_kml():
      for reg, prob in self.regionprobs.iteritems():
        for kml in one_reg_kml(reg, prob):
          yield kml

    allregkml = [x for x in yield_reg_kml()]

    kml = ['kml',
        [('xmlns', 'http://www.opengis.net/kml/2.2'),
         ('xmlns:gx', 'http://www.google.com/kml/ext/2.2'),
         ('xmlns:kml', 'http://www.opengis.net/kml/2.2'),
         ('xmlns:atom', 'http://www.w3.org/2005/Atom')],
        ['Document', [],
          ['Style', [('id', 'bar')],
            ['PolyStyle', [],
              ['outline', [], '0']],
            ['IconStyle', [], ['Icon', []]]],
          ['Style', [('id', 'downArrowIcon')],
            ['IconStyle', [],
              ['Icon', [],
                ['href', [],
                  'http://maps.google.com/mapfiles/kml/pal4/icon28.png']]]],
          ['Folder', [],
            ['name', [], self.word],
            ['open', [], '1'],
            ['description', [],
              "Region distribution for word '%s'" % self.word],
            ['LookAt', [],
              ['latitude', [], '42'],
              ['longitude', [], '-102'],
              ['altitude', [], '0'],
              ['range', [], '5000000'],
              ['tilt', [], '53.454348562403'],
              ['heading', [], '0']]] + allregkml]]

    output_xml_file(filename, kml)


############################################################################
#                           Geographic locations                           #
############################################################################

############ statistical regions ############

# This class contains values used in computing the distribution over all
# locations in the statistical region surrounding the locality in question.
# The statistical region is currently defined as a square of NxN tiling
# regions, for N = Opts.width_of_stat_region.
# The following fields are defined: 
#
#   latind, longind: Region indices of southwest-most tiling region in
#                    statistical region.
#   worddist: Distribution corresponding to region.

class StatRegion(object):
  __slots__ = ['latind', 'longind', 'worddist', 'most_popular_article',
               'mostpopart_links']
  
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

  # Mapping from center of statistical region to corresponding region object.
  # A "statistical region" is made up of a square of tiling regions, with
  # the number of regions on a side determined by `Opts.width_of_stat_region'.  A
  # word distribution is associated with each statistical region.
  corner_to_stat_region = {}

  empty_stat_region = None # Can't compute this until class is initialized
  all_regions_computed = False
  num_empty_regions = 0
  num_non_empty_regions = 0
  total_num_arts_for_word_dist = 0
  total_num_arts_for_links = 0

  def __init__(self, latind, longind):
    self.latind = latind
    self.longind = longind
    self.worddist = RegionWordDist()
    self.most_popular_article = None
    self.mostpopart_links = 0

  def boundstr(self):
    if self.latind is not None:
      near = stat_region_indices_to_near_corner_coord(self.latind, self.longind)
      far = stat_region_indices_to_far_corner_coord(self.latind, self.longind)
      return "%s-%s" % (near, far)
    else:
      return "nowhere"

  def __str__(self):
    unfinished = "" if self.worddist.finished else ", unfinished"
    contains = ""
    if self.most_popular_article:
      contains = ", most-pop-art %s(%d links)" % (
          self.most_popular_article, self.mostpopart_links)

    return "StatRegion(%s%s%s, %d articles(dist), %d articles(links), %d links)" % (
        self.boundstr(), unfinished, contains,
        self.worddist.num_arts_for_word_dist, self.worddist.num_arts_for_links,
        self.worddist.incoming_links)

  def __repr__(self):
    return self.__str__().encode("utf-8")

  def shortstr(self):
    str = "Region %s" % self.boundstr()
    mostpop = self.most_popular_article
    if mostpop:
      str += ", most-popular %s" % mostpop.shortstr()
    return str

  def struct(self):
    foo = ["StatRegion"]
    foo += [('bounds', self.boundstr())]
    if not self.worddist.finished:
      foo += [('finished', True)]
    if self.most_popular_article:
      foo += [('most-popular-article', self.most_popular_article.struct())]
      foo += [('most-popular-article-links', self.mostpopart_links)]
    foo += [('num-articles-dist', self.worddist.num_arts_for_word_dist)]
    foo += [('num-articles-link', self.worddist.num_arts_for_links)]
    foo += [('incoming-links', self.worddist.incoming_links)]
    return foo

  # Generate the distribution for a statistical region from the tiling regions.
  def generate_dist(self):

    reglat = self.latind
    reglong = self.longind

    if debug['lots']:
      errprint("Generating distribution for statistical region centered at %s"
               % region_indices_to_coord(reglat, reglong))

    # Accumulate counts for the given region
    def process_one_region(latind, longind):
      arts = StatRegion.tiling_region_to_articles.get((latind, longind), None)
      if not arts:
        return
      if debug['lots']:
        errprint("--> Processing tiling region %s" %
                 region_indices_to_coord(latind, longind))
      self.worddist.add_articles(arts)
      for art in arts:
        if art.incoming_links > self.mostpopart_links:
          self.mostpopart_links = art.incoming_links
          self.most_popular_article = art

    # Process the tiling regions making up the statistical region;
    # but be careful around the edges.  Truncate the latitude, wrap the
    # longitude.
    for i in range(reglat, min(maximum_latind + 1,
                               reglat + Opts.width_of_stat_region)):
      for j in range(reglong, reglong + Opts.width_of_stat_region):
        jj = j
        if jj > maximum_longind: jj -= 360.
        process_one_region(i, jj)

    self.worddist.finish(minimum_word_count=Opts.minimum_word_count)

  # Find the correct StatRegion for the given coordinates.
  # If none, create the region.
  @classmethod
  def find_region_for_coord(cls, coord):
    latind, longind = coord_to_stat_region_indices(coord)
    return cls.find_region_for_region_indices(latind, longind)

  # Find the StatRegion with the given indices at the southwest point.
  # If none, create the region unless 'no_create' is True.  Otherwise, if
  # 'no_create_empty' is True and the region is empty, a default empty
  # region is returned.
  @classmethod
  def find_region_for_region_indices(cls, latind, longind, no_create=False,
                                     no_create_empty=False):
    statreg = cls.corner_to_stat_region.get((latind, longind), None)
    if not statreg:
      if no_create:
        return None
      if cls.all_regions_computed:
        if not cls.empty_stat_region:
          cls.empty_stat_region = cls(None, None)
          cls.empty_stat_region.worddist.finish()
        return cls.empty_stat_region
      statreg = cls(latind, longind)
      statreg.generate_dist()
      empty = statreg.worddist.is_empty()
      if empty:
        cls.num_empty_regions += 1
      else:
        cls.num_non_empty_regions += 1
      if not empty or not no_create_empty:
        cls.corner_to_stat_region[(latind, longind)] = statreg
    return statreg

  # Generate all StatRegions that are non-empty.  Don't do anything if
  # called multiple times.
  @classmethod
  def initialize_regions(cls):
    if cls.all_regions_computed:
      return

    errprint("Generating all non-empty statistical regions...")
    status = StatusMessage('statistical region')

    for i in xrange(minimum_latind, maximum_latind + 1):
      for j in xrange(minimum_longind, maximum_longind + 1):
        reg = cls.find_region_for_region_indices(i, j, no_create_empty=True)
        if debug['region'] and not reg.worddist.is_empty():
          errprint("--> (%d,%d): %s" % (i, j, reg))
        status.item_processed()
    cls.all_regions_computed = True

    cls.total_num_arts_for_links = 0
    cls.total_num_arts_for_word_dist = 0
    for reg in StatRegion.iter_nonempty_regions():
      cls.total_num_arts_for_word_dist += reg.worddist.num_arts_for_word_dist
      cls.total_num_arts_for_links += reg.worddist.num_arts_for_links

    errprint("Number of non-empty regions: %s" % cls.num_non_empty_regions)
    errprint("Number of empty regions: %s" % cls.num_empty_regions)
    errprint("Percent non-empty regions: %g" %
        (float(cls.num_non_empty_regions) /
          (cls.num_empty_regions + cls.num_non_empty_regions)))
    training_arts_with_word_counts = (
        ArticleTable.num_word_count_articles_by_split['training'])
    errprint("Training articles per non-empty region: %g" %
        (float(training_arts_with_word_counts) / 
          cls.num_non_empty_regions))
    # Save some memory by clearing this after it's not needed
    cls.tiling_region_to_articles = None
    ArticleTable.clear_training_article_distributions()

  # Add the given article to the region map, which covers the earth in regions
  # of a particular size to aid in computing the regions used in region-based
  # Naive Bayes.
  @classmethod
  def add_article_to_region(cls, article):
    latind, longind = coord_to_tiling_region_indices(article.coord)
    cls.tiling_region_to_articles[(latind, longind)] += [article]

  # Iterate over all non-empty regions.  If 'nonempty_word_dist' is given,
  # distributions must also have a non-empty word distribution; otherwise,
  # they just need to have at least one point in them. (Not all points
  # have word distributions, esp. when --max-time-per-stage is set so
  # that we only load the word distributions for a fraction of the whole
  # set of articles with distributions.)
  @classmethod
  def iter_nonempty_regions(cls, nonempty_word_dist=False):
    assert cls.all_regions_computed
    for val in cls.corner_to_stat_region.itervalues():
      if (val.worddist.is_empty_for_word_dist() if nonempty_word_dist
          else val.worddist.is_empty()):
        continue
      yield val

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
#   stat_region: The statistical region surrounding this location, including
#             all necessary information to determine the region-based
#             distribution.

class Locality(Location):
  # This is an optimization that causes space to be allocated in the most
  # efficient possible way for exactly these attributes, and no others.

  __slots__ = Location.__slots__ + ['coord', 'stat_region']

  def __init__(self, name, coord):
    self.name = name
    self.coord = coord
    self.altnames = []
    self.match = None
    self.stat_region = None

  def __str__(self, no_article=False):
    match = ""
    if not no_article:
      match = ", match=%s" % self.match
    return 'Locality %s (%s) at %s%s' % \
      (self.name, self.div and '/'.join(self.div.path), self.coord, match)

  def __repr__(self):
    return self.__str__().encode("utf-8")

  def shortstr(self):
    return "Locality %s (%s)" % (
        self.name, self.div and '/'.join(self.div.path))

  def struct(self, no_article=False): 
    foo = ["Locality"]
    foo += [('name', self.name)]
    foo += [('in division', self.div and '/'.join(self.div.path))]
    foo += [('at coordinate', self.coord)]
    if not no_article:
      if self.match:
        foo += [('matching', self.match.struct())]
      else:
        foo += [('matching', 'none')]
    return foo

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
#   worddist: For region-based Naive Bayes disambiguation, a distribution
#           over the division's article and all locations within the region.

class Division(Location):
  __slots__ = Location.__slots__ + \
    ['level', 'path', 'locs', 'goodlocs', 'boundary', 'worddist']

  # For each division, map from division's path to Division object.
  path_to_division = {}

  # For each tiling region, list of divisions that have territory in it
  tiling_region_to_divisions = listdict()

  def __init__(self, path):
    self.name = path[-1]
    self.altnames = []
    self.path = path
    self.level = len(path)
    self.locs = []
    self.match = None
    self.worddist = None

  def __str__(self, no_article=False):
    match = ""
    if not no_article:
      match = ", match=%s" % self.match
    return 'Division %s (%s)%s, boundary=%s' % \
      (self.name, '/'.join(self.path), match, self.boundary)

  def __repr__(self):
    return self.__str__().encode("utf-8")

  def shortstr(self):
    str = "Division %s" % self.name
    if self.level > 1:
      str += " (%s)" % ('/'.join(self.path))
    return str

  def struct(self, no_article=False): 
    foo = ["Division"]
    foo += [('name', self.name)]
    foo += [('path', '/'.join(self.path))]
    if not no_article:
      if self.match:
        foo += [('matching', self.match.struct())]
      else:
        foo += [('matching', 'none')]
    foo += [('boundary', self.boundary.struct())]
    return foo

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
    def iter_non_outliers():
      # If not enough points, just return them; otherwise too much possibility
      # that all of them, or some good ones, will be considered outliers.
      if len(self.locs) <= 5:
        for p in self.locs: yield p
        return
      # FIXME: Actually look for outliers.
      for p in self.locs: yield p
      #for p in self.locs:
      #  # Find minimum distance to all other points and check it.
      #  mindist = min(spheredist(p, x) for x in self.locs if x is not p)
      #  if mindist <= Opts.max_dist_for_outliers: yield p

    if debug['lots']:
      errprint("Computing boundary for %s, path %s, num points %s" %
               (self.name, self.path, len(self.locs)))
               
    self.goodlocs = list(iter_non_outliers())
    # If we've somehow discarded all points, just use the original list
    if not len(self.goodlocs):
      if debug['some']:
        warning("All points considered outliers?  Division %s, path %s" %
                (self.name, self.path))
      self.goodlocs = self.locs
    # FIXME! This will fail for a division that crosses the International
    # Date Line.
    topleft = Coord(min(x.coord.lat for x in self.goodlocs),
                    min(x.coord.long for x in self.goodlocs))
    botright = Coord(max(x.coord.lat for x in self.goodlocs),
                     max(x.coord.long for x in self.goodlocs))
    self.boundary = Boundary(topleft, botright)

  def generate_worddist(self):
    self.worddist = RegionWordDist()
    self.worddist.add_locations([self])
    self.worddist.add_locations(self.goodlocs)
    self.worddist.finish(minimum_word_count=Opts.minimum_word_count)

  def __contains__(self, coord):
    return coord in self.boundary

  # Find the division for a point in the division with a given path,
  # add the point to the division.  Create the division if necessary.
  # Return the corresponding Division.
  @classmethod
  def find_division_note_point(cls, loc, path):
    higherdiv = None
    if len(path) > 1:
      # Also note location in next-higher division.
      higherdiv = cls.find_division_note_point(loc, path[0:-1])
    # Skip divisions where last element in path is empty; this is a
    # reference to a higher-level division with no corresponding lower-level
    # division.
    if not path[-1]: return higherdiv
    if path in cls.path_to_division:
      division = cls.path_to_division[path]
    else:
      # If we haven't seen this path, create a new Division object.
      # Record the mapping from path to division, and also from the
      # division's "name" (name of lowest-level division in path) to
      # the division.
      division = cls(path)
      division.div = higherdiv
      cls.path_to_division[path] = division
      Gazetteer.lower_toponym_to_division[path[-1].lower()] += [division]
    division.locs += [loc]
    return division

  # Finish all computations related to Divisions, after we've processed
  # all points (and hence all points have been added to the appropriate
  # Divisions).
  @classmethod
  def finish_all(cls):
    divs_by_area = []
    for division in cls.path_to_division.itervalues():
      if debug['lots']:
        errprint("Processing division named %s, path %s"
                 % (division.name, division.path))
      division.compute_boundary()
      match = ArticleTable.find_match_for_division(division)
      if match:
        if debug['lots']:
          errprint("Matched article %s for division %s, path %s" %
                   (match, division.name, division.path))
        division.match = match
        match.location = division
      else:
        if debug['lots']:
          errprint("Couldn't find match for division %s, path %s" %
                   (division.name, division.path))
      for inds in division.boundary.iter_nonempty_tiling_regions():
        cls.tiling_region_to_divisions[inds] += [division]
      if debug['region']:
        divs_by_area += [(division, division.boundary.square_area())]
    if debug['region']:
      for (div, area) in sorted(divs_by_area, key=lambda x:x[1], reverse=True):
        errprint("%.2f square miles: %s" % (area, div))

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

  # For each toponym, list of Wikipedia articles matching the name.
  lower_toponym_to_article = listdict()

  # Mapping from lowercased article names to Article objects
  lower_name_to_articles = listdict()

  # List of articles in each split.
  articles_by_split = listdict()

  # Num of articles with word-count information but not in table.
  num_articles_with_word_counts_but_not_in_table = 0

  # Num of articles with word-count information (whether or not in table).
  num_articles_with_word_counts = 0

  # Num of articles in each split with word-count information seen.
  num_word_count_articles_by_split = intdict()

  # Num of articles in each split with a computed distribution.
  # (Not the same as the previous since we don't compute the distribution of articles in
  # either the test or dev set depending on which one is used.)
  num_dist_articles_by_split = intdict()

  # Total # of word tokens for all articles in each split.
  word_tokens_by_split = intdict()

  # Total # of incoming links for all articles in each split.
  incoming_links_by_split = intdict()

  # Look up an article named NAME and return the associated article.
  # Note that article names are case-sensitive but the first letter needs to
  # be capitalized.
  @classmethod
  def lookup_article(cls, name):
    assert name
    return cls.name_to_article.get(capfirst(name), None)

  # Record the article as having NAME as one of its names (there may be
  # multiple names, due to redirects).  Also add to related lists mapping
  # lowercased form, short form, etc.
  @classmethod
  def record_article_name(cls, name, art):
    # Must pass in properly cased name
    assert name == capfirst(name)
    cls.name_to_article[name] = art
    loname = name.lower()
    cls.lower_name_to_articles[loname] += [art]
    (short, div) = compute_short_form(loname)
    if div:
      cls.lower_name_div_to_articles[(short, div)] += [art]
    cls.short_lower_name_to_articles[short] += [art]
    if art not in cls.lower_toponym_to_article[loname]:
      cls.lower_toponym_to_article[loname] += [art]
    if short != loname and art not in cls.lower_toponym_to_article[short]:
      cls.lower_toponym_to_article[short] += [art]

  # Record either a normal article ('artfrom' same as 'artto') or a
  # redirect ('artfrom' redirects to 'artto').
  @classmethod
  def record_article(cls, artfrom, artto):

    cls.record_article_name(artfrom.title, artto)
    redir = artfrom is not artto
    split = artto.split
    fromlinks = get_adjusted_incoming_links(artfrom)
    cls.incoming_links_by_split[split] += fromlinks
    if not redir:
      splitcoll = cls.articles_by_split[split]
      if isinstance(splitcoll, set):
        splitcoll.add(artto)
      else:
        splitcoll.append(artto)
    elif fromlinks:
      # Add count of links pointing to a redirect to count of links
      # pointing to the article redirected to, so that the total incoming
      # link count of an article includes any redirects to that article.
      artto.incoming_links = get_adjusted_incoming_links(artto) + fromlinks

  @classmethod
  def finish_article_distributions(cls):
    # Figure out the value of OVERALL_UNSEEN_MASS for each article.
    for (split, table) in cls.articles_by_split.iteritems():
      totaltoks = 0
      numarts = 0
      for art in table:
        if art.dist:
          art.dist.finish(minimum_word_count=Opts.minimum_word_count)
          totaltoks += art.dist.total_tokens
          numarts += 1
      cls.num_dist_articles_by_split[split] = numarts
      cls.word_tokens_by_split[split] = totaltoks

  @classmethod
  def clear_training_article_distributions(cls):
    for art in cls.articles_by_split['training']:
      art.dist = None

  # Find Wikipedia article matching name NAME for location LOC.  NAME
  # will generally be one of the names of LOC (either its canonical
  # name or one of the alternate name).  CHECK_MATCH is a function that
  # is passed two aruments, the location and the Wikipedia artile name,
  # and should return True if the location matches the article.
  # PREFER_MATCH is used when two or more articles match.  It is passed
  # three argument, the location and two Wikipedia article names.  It
  # should return TRUE if the first is to be preferred to the second.
  # Return the name of the article matched, or None.

  @classmethod
  def find_one_wikipedia_match(cls, loc, name, check_match, prefer_match):

    loname = name.lower()

    # Look for any articles with same name (case-insensitive) as the location,
    # check for matches
    for art in cls.lower_name_to_articles[loname]:
      if check_match(loc, art): return art

    # Check whether there is a match for an article whose name is
    # a combination of the location's name and one of the divisions that
    # the location is in (e.g. "Augusta, Georgia" for a location named
    # "Augusta" in a second-level division "Georgia").
    if loc.div:
      for div in loc.div.path:
        for art in cls.lower_name_div_to_articles[(loname, div.lower())]:
          if check_match(loc, art): return art

    # See if there is a match with any of the articles whose short name
    # is the same as the location's name
    arts = cls.short_lower_name_to_articles[loname]
    if arts:
      goodarts = [art for art in arts if check_match(loc, art)]
      if len(goodarts) == 1:
        return goodarts[0] # One match
      elif len(goodarts) > 1:
        # Multiple matches: Sort by preference, return most preferred one
        if debug['lots']:
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

  @classmethod
  def find_wikipedia_match(cls, loc, check_match, prefer_match):
    # Try to find a match for the canonical name of the location
    match = cls.find_one_wikipedia_match(loc, loc.name, check_match,
                                         prefer_match)
    if match: return match

    # No match; try each of the alternate names in turn.
    for altname in loc.altnames:
      match = cls.find_one_wikipedia_match(loc, altname, check_match,
                                           prefer_match)
      if match: return match

    # No match.
    return None

  # Find Wikipedia article matching locality LOC; the two coordinates must
  # be at most MAXDIST away from each other.

  @classmethod
  def find_match_for_locality(cls, loc, maxdist):

    def check_match(loc, art):
      dist = spheredist(loc.coord, art.coord)
      if dist <= maxdist:
        return True
      else:
        if debug['lots']:
          errprint("Found article %s but dist %s > %s" %
                   (art, dist, maxdist))
        return False

    def prefer_match(loc, art1, art2):
      return spheredist(loc.coord, art1.coord) < \
        spheredist(loc.coord, art2.coord)

    return cls.find_wikipedia_match(loc, check_match, prefer_match)

  # Find Wikipedia article matching division LOC; the article coordinate
  # must be inside of the division's boundaries.

  @classmethod
  def find_match_for_division(cls, loc):

    def check_match(loc, art):
      if art.coord and art.coord in loc:
        return True
      else:
        if debug['lots']:
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
      if l1 is not None and l2 is not None:
        return l1 > l2
      else:
        # FIXME: Do something smart here -- maybe check that location is farther
        # in the middle of the bounding box (does this even make sense???)
        return True

    return cls.find_wikipedia_match(loc, check_match, prefer_match)


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
#   dist: Object containing word distribution of this article.
#   location: Corresponding location for this article.
#   stat_region: StatRegion object corresponding to this article.

class StatArticle(Article):
  __slots__ = Article.__slots__ + ['dist', 'location', 'stat_region']

  def __init__(self, **args):
    super(StatArticle, self).__init__(**args)
    self.location = None
    self.stat_region = None
    self.dist = None

  def __str__(self):
    coordstr = " at %s" % self.coord if self.coord else ""
    if self.location:
      coordstr += (", matching location %s" %
                   self.location.__str__(no_article=True))
    redirstr = ", redirect to %s" % self.redir if self.redir else ""
    divs = self.find_covering_divisions()
    top_divs = [div.__str__(no_article=True) for div in divs if div.level == 1]
    if top_divs:
      topdivstr = ", in top-level divisions %s" % (', '.join(top_divs))
    else:
      topdivstr = ", not in any top-level divisions"
    return '%s(%s)%s%s%s' % (self.title, self.id, coordstr, redirstr, topdivstr)

  def __repr__(self):
    return "Article(%s)" % self.__str__().encode("utf-8")

  def shortstr(self):
    str = "%s" % self.title
    if self.location:
      str += ", matching %s" % self.location.shortstr()
    divs = self.find_covering_divisions()
    top_divs = [div.name for div in divs if div.level == 1]
    if top_divs:
      str += ", in top-level divisions %s" % (', '.join(top_divs))
    return str

  def struct(self): 
    foo = ["StatArticle"]
    foo += [('title', self.title)]
    foo += [('id', self.id)]
    if self.coord:
      foo += [('location', self.coord)]
    if self.location:
      foo += [('matching', self.location.struct(no_article=True))]
    if self.redir:
      foo += [('redirect to', self.redir.struct())]
    divs = self.find_covering_divisions()
    top_divs = [div.struct(no_article=True) for div in divs if div.level == 1]
    if top_divs:
      foo += [('top level divisions', top_divs)]
    else:
      foo += [('top level divisions', "none")]
    return foo

  def distance_to_coord(self, coord):
    return spheredist(self.coord, coord)

  def matches_coord(self, coord):
    if self.distance_to_coord(coord) <= Opts.max_dist_for_close_match:
      return True
    if self.location and type(self.location) is Division and \
        self.location.matches_coord(coord): return True
    return False

  # Determine the region word-distribution object for a given article:
  # Create and populate one if necessary.
  def find_regworddist(self):
    loc = self.location
    if loc and type(loc) is Division:
      if not loc.worddist:
        loc.generate_worddist()
      return loc.worddist
    if not self.stat_region:
      self.stat_region = StatRegion.find_region_for_coord(self.coord)
    return self.stat_region.worddist

  # Find the divisions that cover the given article.
  def find_covering_divisions(self):
    inds = coord_to_tiling_region_indices(self.coord)
    divs = Division.tiling_region_to_divisions[inds]
    return [div for div in divs if self.coord in div]


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
      if reason is not None:
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
    errprint("Number of instances = %s" % self.total_instances)
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

  def record_result(self, correct, reason, num_candidates):
    super(EvalWithCandidateList, self).record_result(correct, reason)
    self.total_instances_by_num_candidates[num_candidates] += 1
    if correct:
      self.correct_instances_by_num_candidates[num_candidates] += 1
    else:
      self.incorrect_instances_by_num_candidates[num_candidates] += 1

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

class GeotagDocumentEval(EvalWithRank):
  def __init__(self, max_rank_for_credit=10):
    super(GeotagDocumentEval, self).__init__(max_rank_for_credit)
    # "True dist" means actual distance in km's or whatever.
    # "Degree dist" is the distance in degrees.

    self.true_dists = []
    self.degree_dists = []
    self.oracle_true_dists = []
    self.oracle_degree_dists = []

  def record_result(self, rank, pred_true_dist, pred_degree_dist):
    super(GeotagDocumentEval, self).record_result(rank)
    self.true_dists += [pred_true_dist]
    self.degree_dists += [pred_degree_dist]

  def record_oracle_result(self, oracle_true_dist, oracle_degree_dist):
    self.oracle_true_dists += [oracle_true_dist]
    self.oracle_degree_dists += [oracle_degree_dist]

  def output_incorrect_results(self):
    super(GeotagDocumentEval, self).output_incorrect_results()
    self.true_dists.sort()
    self.degree_dists.sort()
    self.oracle_true_dists.sort()
    self.oracle_degree_dists.sort()
    def miles_and_km(val):
      km_per_mile = 1.609
      return "%.2f miles (%.2f km)" % (val, val*km_per_mile)
    errprint("  Mean true error distance = %s" %
             miles_and_km(mean(self.true_dists)))
    errprint("  Median true error distance = %s" %
             miles_and_km(median(self.true_dists)))
    errprint("  Mean degree error distance = %.2f degrees" %
             mean(self.degree_dists))
    errprint("  Median degree error distance = %.2f degrees" %
             median(self.degree_dists))
    errprint("  Mean oracle true error distance = %s" %
             miles_and_km(mean(self.oracle_true_dists)))
    errprint("  Median oracle true error distance = %s" %
             miles_and_km(median(self.oracle_true_dists)))

####### Results for geotagging toponyms
class GeotagToponymResults(object):

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

  def __init__(self):
    # Overall statistics
    self.all_toponym = \
        EvalWithCandidateList(self.incorrect_geotag_toponym_reasons) 

    # Statistics when toponym not same as true name of location
    self.diff_surface = \
        EvalWithCandidateList(self.incorrect_geotag_toponym_reasons)

    # Statistics when toponym not same as true name or short form of location
    self.diff_short = \
        EvalWithCandidateList(self.incorrect_geotag_toponym_reasons)

  def record_geotag_toponym_result(self, correct, toponym, trueloc, reason,
                                   num_candidates):
    self.all_toponym.record_result(correct, reason, num_candidates)
    if toponym != trueloc:
      self.diff_surface.record_result(correct, reason, num_candidates)
      (short, div) = compute_short_form(trueloc)
      if toponym != short:
        self.diff_short.record_result(correct, reason, num_candidates)

  def output_geotag_toponym_results(self):
    errprint("Results for all toponyms:")
    self.all_toponym.output_results()
    errprint("")
    errprint("Results for toponyms when different from true location name:")
    self.diff_surface.output_results()
    errprint("")
    errprint("Results for toponyms when different from either true location name")
    errprint("  or its short form:")
    self.diff_short.output_results()
    output_resource_usage()

####### Results for geotagging documents/articles
class GeotagDocumentResults(object):

  def __init__(self):
    self.all_document = GeotagDocumentEval()

    # naitr = "num articles in true region"
    self.docs_by_naitr = TableByRange([1, 10, 25, 100], GeotagDocumentEval)

    # Results for documents where the location is at a certain distance
    # from the center of the true statistical region.  The key is measured in
    # fractions of a tiling region (determined by 'dist_fraction_increment',
    # e.g. if dist_fraction_increment = 0.25 then values in the range of
    # [0.25, 0.5) go in one bin, [0.5, 0.75) go in another, etc.).  We measure
    # distance is two ways: True distance (in miles or whatever) and "degree
    # distance", as if degrees were a constant length both latitudinally
    # and longitudinally.
    self.dist_fraction_increment = 0.25
    self.docs_by_degree_dist_to_true_center = \
        collections.defaultdict(GeotagDocumentEval)
    self.docs_by_true_dist_to_true_center = \
        collections.defaultdict(GeotagDocumentEval)

    # Similar, but distance between location and center of top predicted
    # region.
    self.dist_fractions_for_error_dist = [
        0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4, 6, 8,
        12, 16, 24, 32, 48, 64, 96, 128, 192, 256,
        # We're never going to see these
        384, 512, 768, 1024, 1536, 2048]
    self.docs_by_degree_dist_to_pred_center = \
        TableByRange(self.dist_fractions_for_error_dist, GeotagDocumentEval)
    self.docs_by_true_dist_to_pred_center = \
        TableByRange(self.dist_fractions_for_error_dist, GeotagDocumentEval)

    self.true_error_dists = []
    self.deg_error_dists = []

  def record_geotag_document_result(self, rank, coord, pred_latind,
                                    pred_longind, num_arts_in_true_region,
                                    return_stats=False):
    def degree_dist(c1, c2):
      return math.sqrt((c1.lat - c2.lat)**2 + (c1.long - c2.long)**2)

    pred_center = stat_region_indices_to_center_coord(pred_latind, pred_longind)
    pred_truedist = spheredist(coord, pred_center)
    pred_degdist = degree_dist(coord, pred_center)

    self.all_document.record_result(rank, pred_truedist, pred_degdist)
    naitr = self.docs_by_naitr.get_collector(num_arts_in_true_region)
    naitr.record_result(rank, pred_truedist, pred_degdist)

    true_latind, true_longind = coord_to_stat_region_indices(coord)
    true_center = stat_region_indices_to_center_coord(true_latind, true_longind)
    true_truedist = spheredist(coord, true_center)
    true_degdist = degree_dist(coord, true_center)
    fracinc = self.dist_fraction_increment
    rounded_true_truedist = fracinc * (true_truedist // fracinc)
    rounded_true_degdist = fracinc * (true_degdist // fracinc)

    self.all_document.record_oracle_result(true_truedist, true_degdist)
    self.docs_by_true_dist_to_true_center[rounded_true_truedist]. \
        record_result(rank, pred_truedist, pred_degdist)
    self.docs_by_degree_dist_to_true_center[rounded_true_degdist]. \
        record_result(rank, pred_truedist, pred_degdist)

    self.docs_by_true_dist_to_pred_center.get_collector(pred_truedist). \
        record_result(rank, pred_truedist, pred_degdist)
    self.docs_by_degree_dist_to_pred_center.get_collector(pred_degdist). \
        record_result(rank, pred_truedist, pred_degdist)

    if return_stats:
      return {'pred_center':pred_center,
              'pred_truedist':pred_truedist,
              'pred_degdist':pred_degdist,
              'true_center':true_center,
              'true_truedist':true_truedist,
              'true_degdist':true_degdist}

  def record_geotag_document_other_stat(self, othertype):
    self.all_document.record_other_stat(othertype)

  def output_geotag_document_results(self, all_results=False):
    errprint("")
    errprint("Results for all documents/articles:")
    self.all_document.output_results()
    #if all_results:
    if False:
      errprint("")
      for (lower, upper, obj) in self.docs_by_naitr.iter_ranges():
        errprint("")
        errprint("Results for documents/articles where number of articles")
        errprint("  in true region is in the range [%s,%s]:" %
                 (lower, upper - 1 if type(upper) is int else upper))
        obj.output_results()
      errprint("")
      for (truedist, obj) in \
          sorted(self.docs_by_true_dist_to_true_center.iteritems(),
                 key=lambda x:x[0]):
        lowrange = truedist * Opts.miles_per_region
        highrange = ((truedist + self.dist_fraction_increment) *
                     Opts.miles_per_region)
        errprint("")
        errprint("Results for documents/articles where distance to center")
        errprint("  of true region in miles is in the range [%.2f,%.2f):" %
                 (lowrange, highrange))
        obj.output_results()
      errprint("")
      for (degdist, obj) in \
          sorted(self.docs_by_degree_dist_to_true_center.iteritems(),
                 key=lambda x:x[0]):
        lowrange = degdist * degrees_per_region
        highrange = ((degdist + self.dist_fraction_increment) *
                     degrees_per_region)
        errprint("")
        errprint("Results for documents/articles where distance to center")
        errprint("  of true region in degrees is in the range [%.2f,%.2f):" %
                 (lowrange, highrange))
        obj.output_results()
    # FIXME: Output median and mean of true and degree error dists; also
    # maybe move this info info EvalByRank so that we can output the values
    # for each category
    errprint("")
    output_resource_usage()


############################################################################
#                             Main geotagging code                         #
############################################################################


# Construct the list of possible candidate articles for a given toponym
def construct_candidates(toponym):
  lotop = toponym.lower()
  articles = ArticleTable.lower_toponym_to_article[lotop]
  locs = (Gazetteer.lower_toponym_to_location[lotop] +
          Gazetteer.lower_toponym_to_division[lotop])
  for loc in locs:
    if loc.match and loc.match not in articles:
      articles += [loc.match]
  return articles

def word_is_toponym(word):
  word = word.lower()
  return (word in ArticleTable.lower_toponym_to_article or
          word in Gazetteer.lower_toponym_to_location or
          word in Gazetteer.lower_toponym_to_division)


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

# Abstract class for reading documents from a test file and evaluating on
# them.
class TestFileEvaluator(object):
  def __init__(self, opts, strategy, stratname):
    self.opts = opts
    self.strategy = strategy
    self.stratname = stratname
    self.documents_processed = 0

  def iter_documents(self, filename):
    pass

  def would_skip_document(self, doc, doctag):
    # Return True if document would be processed and evaluated; False
    # if skipped.
    return False

  def evaluate_document(self, doc, doctag):
    # Return True if document was actually processed and evaluated; False
    # if skipped.
    return True

  def output_results(self, final=False):
    pass

  def evaluate_and_output_results(self, files):
    def output_final_results():
      errprint("")
      errprint("Final results for strategy %s: All %d documents processed:" %
               (self.stratname, status.num_processed()))
      errprint("Ending operation at %s" % (time.ctime()))
      self.output_results(final=True)
      errprint("Ending final results for strategy %s" % self.stratname)

    status = StatusMessage('document')
    last_elapsed = 0
    last_processed = 0
    skip_initial = self.opts.skip_initial_test_docs
    skip_n = 0
    for filename in files:
      errprint("Processing evaluation file %s..." % filename)
      for doc in self.iter_documents(filename):
        # errprint("Processing document: %s" % doc)
        num_processed = status.num_processed()
        doctag = '#%d' % (1+num_processed)
        if self.would_skip_document(doc, doctag):
          errprint("Skipped document %s" % doc)
        else:
          do_skip = False
          if skip_initial:
            skip_initial -= 1
            do_skip = True
          elif skip_n:
            skip_n -= 1
            do_skip = True
          else:
            skip_n = self.opts.every_nth_test_doc - 1
          if do_skip:
            errprint("Passed over document %s" % doctag)
          else:
            assert self.evaluate_document(doc, doctag)
          status.item_processed()
          new_elapsed = status.elapsed_time()
          new_processed = status.num_processed()

          # If max # of docs reached, stop
          if (self.opts.num_test_docs and
              new_processed >= self.opts.num_test_docs):
            errprint("")
            errprint("Finishing evaluation after %d documents" %
                new_processed)
            output_final_results()
            return

          # If five minutes and ten documents have gone by, print out results
          if (new_elapsed - last_elapsed >= 300 and
              new_processed - last_processed >= 10):
            errprint("Results after %d documents (strategy %s):" %
                (status.num_processed(), self.stratname))
            self.output_results(final=False)
            errprint("End of results after %d documents (strategy %s):" %
                (status.num_processed(), self.stratname))
            last_elapsed = new_elapsed
            last_processed = new_processed

    output_final_results()
  
class GeotagToponymStrategy(object):
  def need_context(self):
    pass

  def compute_score(self, geogword, art):
    pass

# Find each toponym explicitly mentioned as such and disambiguate it
# (find the correct geographic location) using the "link baseline", i.e.
# use the location with the highest number of incoming links.
class BaselineGeotagToponymStrategy(GeotagToponymStrategy):
  def __init__(self, opts, baseline_strategy):
    self.opts = opts
    self.baseline_strategy = baseline_strategy

  def need_context(self):
    return False

  def compute_score(self, geogword, art):
    if self.baseline_strategy == 'internal-link':
      if self.opts.context_type == 'region':
        return get_adjusted_incoming_links(art.find_regworddist())
      else:
        return get_adjusted_incoming_links(art)
    elif self.baseline_strategy == 'num-articles':
      if self.opts.context_type == 'region':
        return art.find_regworddist().num_arts_for_links
      else:
        location = art.location
        if type(location) is Division:
          return len(location.locs)
        else:
          return 1
    else:
      return random.random()

# Find each toponym explicitly mentioned as such and disambiguate it
# (find the correct geographic location) using Naive Bayes, possibly
# in conjunction with the baseline.
class NaiveBayesToponymStrategy(GeotagToponymStrategy):
  def __init__(self, opts, use_baseline=True):
    self.opts = opts
    self.use_baseline = use_baseline

  def need_context(self):
    return True

  def compute_score(self, geogword, art):
    # FIXME FIXME!!! We are assuming that the baseline is 'internal-link',
    # regardless of its actual settings.
    thislinks = get_adjusted_incoming_links(art)

    if self.opts.context_type == 'article':
      distobj = art.dist
    else:
      distobj = art.find_regworddist()
    totalprob = 0.0
    total_word_weight = 0.0
    if not self.use_baseline:
      word_weight = 1.0
      baseline_weight = 0.0
    elif self.opts.naive_bayes_weighting == 'equal':
      word_weight = 1.0
      baseline_weight = 1.0
    else:
      baseline_weight = self.opts.baseline_weight
      word_weight = 1 - baseline_weight
    for (dist, word) in geogword.context:
      if not self.opts.preserve_case_words: word = word.lower()
      wordprob = distobj.lookup_word(word)

      # Compute weight for each word, based on distance from toponym
      if self.opts.naive_bayes_weighting == 'equal' or \
         self.opts.naive_bayes_weighting == 'equal-words':
        thisweight = 1.0
      else:
        thisweight = 1.0/(1+dist)

      total_word_weight += thisweight
      totalprob += thisweight*log(wordprob)
    if debug['some']:
      errprint("Computed total word log-likelihood as %s" % totalprob)
    # Normalize probability according to the total word weight
    if total_word_weight > 0:
      totalprob /= total_word_weight
    # Combine word and prior (baseline) probability acccording to their
    # relative weights
    totalprob *= word_weight
    totalprob += baseline_weight*log(thislinks)
    if debug['some']:
      errprint("Computed total log-likelihood as %s" % totalprob)
    return totalprob

class GeotagToponymEvaluator(TestFileEvaluator):
  def __init__(self, opts, strategy, stratname):
    super(GeotagToponymEvaluator, self).__init__(opts, strategy, stratname)
    self.results = GeotagToponymResults()
    
  # Given an evaluation file, read in the words specified, including the
  # toponyms.  Mark each word with the "document" (e.g. article) that it's
  # within.
  def iter_geogwords(self, filename):
    pass

  # Retrieve the words yielded by iter_geowords() and separate by "document"
  # (e.g. article); yield each "document" as a list of such Geogword objects.
  # If self.compute_context, also generate the set of "context" words used for
  # disambiguation (some window, e.g. size 20, of words around each
  # toponym).
  def iter_documents(self, filename):
    def return_word(word):
      if word.is_toponym:
        if debug['lots']:
          errprint("Saw loc %s with true coordinates %s, true location %s" %
                   (word.word, word.coord, word.location))
      else:
        if debug['tons']:
          errprint("Non-toponym %s" % word.word)
      return word

    for k, g in groupby(self.iter_geogwords(filename),
                        lambda word: word.document or 'foo'):
      if k:
        errprint("Processing document %s..." % k)
      results = [return_word(word) for word in g]

      # Now compute context for words
      nbcl = self.opts.naive_bayes_context_len
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
            # Select up to naive_bayes_context_len words on either side;
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
    articles = construct_candidates(toponym)
    bestscore = -1e308
    bestart = None
    if not articles:
      if debug['some']:
        errprint("Unable to find any possibilities for %s" % toponym)
      correct = False
    else:
      if debug['some']:
        errprint("Considering toponym %s, coordinates %s" %
                 (toponym, coord))
        errprint("For toponym %s, %d possible articles" %
                 (toponym, len(articles)))
      for art in articles:
        if debug['some']:
            errprint("Considering article %s" % art)
        thisscore = self.strategy.compute_score(geogword, art)
        if thisscore > bestscore:
          bestscore = thisscore
          bestart = art 
      if bestart:
        correct = bestart.matches_coord(coord)
      else:
        correct = False

    num_candidates = len(articles)

    if correct:
      reason = None
    else:
      if num_candidates == 0:
        reason = 'incorrect_with_no_candidates'
      else:
        good_arts = [art for art in articles if art.matches_coord(coord)]
        if not good_arts:
          reason = 'incorrect_with_no_correct_candidates'
        elif len(good_arts) > 1:
          reason = 'incorrect_with_multiple_correct_candidates'
        else:
          goodart = good_arts[0]
          if goodart.incoming_links is None:
            reason = 'incorrect_one_correct_candidate_missing_link_info'
          else:
            reason = 'incorrect_one_correct_candidate'

    errprint("Eval: Toponym %s (true: %s at %s),"
             % (toponym, geogword.location, coord), nonl=True)
    if correct:
      errprint("correct")
    else:
      errprint("incorrect, reason = %s" % reason)

    self.results.record_geotag_toponym_result(correct, toponym,
        geogword.location, reason, num_candidates)

    if debug['some'] and bestart:
      errprint("Best article = %s, score = %s, dist = %s, correct %s"
               % (bestart, bestscore, bestart.distance_to_coord(coord),
                  correct))

  def evaluate_document(self, doc, doctag):
    for geogword in doc:
       self.disambiguate_toponym(geogword)
    return True

  def output_results(self, final=False):
    self.results.output_geotag_toponym_results()


def get_adjusted_incoming_links(obj):
  incoming_links = obj.incoming_links
  if incoming_links is None:
    incoming_links = 0
    if debug['some']:
      warning("Strange, %s has no link count" % obj)
  else:
    if debug['some']:
      errprint("--> Link count is %s" % incoming_links)
  if incoming_links == 0: # Whether from unknown count or count is actually zero
    incoming_links = 0.01 # So we don't get errors from log(0)
  return incoming_links

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
  def iter_geogwords(self, filename):
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
  def iter_geogwords(self, filename):
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


class GeotagDocumentStrategy(object):
  def return_ranked_regions(self, worddist):
    pass

def find_most_common_word(worddist, pred):
  # Look for the most common word matching a given predicate.
  # But there may not be any.  max() will raise an error if given an
  # empty sequence, so insert a bogus value into the sequence with a
  # negative count.
  maxword, maxcount = \
    max(chain([(None, -1)],
              ((word, count) for word, count in worddist.counts.iteritems()
               if pred(word))),
        key = lambda x:x[1])
  return maxword

class BaselineGeotagDocumentStrategy(GeotagDocumentStrategy):
  def __init__(self, baseline_strategy):
    self.baseline_strategy = baseline_strategy
    self.cached_ranked_mps = None

  def ranked_regions_random(self, worddist):
    regions = list(StatRegion.iter_nonempty_regions())
    random.shuffle(regions)
    return [(reg, 0) for reg in regions]

  def ranked_most_popular_regions(self, worddist):
    if self.cached_ranked_mps is None:
      self.cached_ranked_mps = \
          [(reg, popularity) for (reg, popularity)
              in sorted(((reg, (get_adjusted_incoming_links(reg.worddist)
                                if self.baseline_strategy == 'internal_link'
                                else reg.worddist.num_arts_for_links))
                        for reg in StatRegion.iter_nonempty_regions()),
                       key=lambda x:x[1],
                       reverse=True)]
    return self.cached_ranked_mps

  def ranked_regions_regdist_most_common_toponym(self, worddist):
    # Look for a toponym, then a proper noun, then any word.
    maxword = find_most_common_word(worddist,
        lambda word: word and word[0].isupper() and word_is_toponym(word))
    if maxword is None:
      maxword = find_most_common_word(worddist,
          lambda word: word and word[0].isupper())
    if maxword is None:
      maxword = find_most_common_word(worddist, lambda x: True)
    return RegionDist.get_region_dist(maxword).get_ranked_regions()

  def ranked_regions_link_most_common_toponym(self, worddist):
    maxword = find_most_common_word(worddist,
        lambda word: word and word[0].isupper() and word_is_toponym(word))
    if not maxword:
      maxword = find_most_common_word(worddist,
          lambda word: word_is_toponym(word))
    if debug['commontop']:
      errprint("  maxword = %s" % maxword)
    if maxword:
      cands = construct_candidates(maxword)
      if debug['commontop']:
        errprint("  candidates = %s" % cands)
      # Sort candidate list by number of incoming links
      cands = [(cand, links) for (cand, links) in
          sorted(((cand, (get_adjusted_incoming_links(cand)))
            for cand in cands),
            key=lambda x:x[1], reverse=True)]
      if debug['commontop']:
        errprint("  sorted candidates = %s" % cands)

      def find_good_regions_for_coord(cands):
        for cand, links in cands:
          reg = StatRegion.find_region_for_coord(cand.coord)
          if reg.latind is None:
            errprint("Strange, found no region for candidate %s" % cand)
          else:
            yield reg, links

      # Convert to regions
      cands = [x for x in find_good_regions_for_coord(cands)]

      if debug['commontop']:
        errprint("  region candidates = %s" % cands)
    else:
      cands = []

    # Return an iterator over all elements in all the given sequences, omitting
    # elements seen more than once and keeping the order.
    def merge_numbered_sequences_uniquely(*seqs):
      table = {}
      for seq in seqs:
        for s, val in seq:
          if s not in table:
            table[s] = True
            yield s, val

    # Append random regions and remove duplicates
    return list(merge_numbered_sequences_uniquely(cands,
        self.ranked_regions_random(worddist)))

  def return_ranked_regions(self, worddist):
    if self.baseline_strategy == 'link-most-common-toponym':
      return self.ranked_regions_link_most_common_toponym(worddist)
    elif self.baseline_strategy == 'regdist-most-common-toponym':
      return self.ranked_regions_regdist_most_common_toponym(worddist)
    elif self.baseline_strategy == 'random':
      return self.ranked_regions_random(worddist)
    else:
      return self.ranked_most_popular_regions(worddist)


class KLDivergenceStrategy(GeotagDocumentStrategy):
  def __init__(self, partial=True, symmetric=False):
    self.partial = partial
    self.symmetric = symmetric

  def return_ranked_regions(self, worddist):
    article_pq = PriorityQueue()
    for stat_region in \
        StatRegion.iter_nonempty_regions(nonempty_word_dist=True):
      inds = (stat_region.latind, stat_region.longind)
      if debug['lots']:
        (latind, longind) = inds
        coord = region_indices_to_coord(latind, longind)
        errprint("Nonempty region at indices %s,%s = coord %s, num_articles = %s"
                 % (latind, longind, coord,
                    stat_region.worddist.num_arts_for_word_dist))

      kldiv = fast_kl_divergence(worddist, stat_region.worddist,
                                 partial=self.partial)
      if self.symmetric:
        kldiv2 = fast_kl_divergence(stat_region.worddist, worddist,
                                    partial=self.partial)
        kldiv = (kldiv + kldiv2) / 2.0
      #kldiv = worddist.test_kl_divergence(stat_region.worddist,
      #                           partial=self.partial)
      #errprint("For region %s, KL divergence %.3f" % (stat_region, kldiv))
      article_pq.add_task(kldiv, stat_region)

    regions = []
    while True:
      try:
        regions.append(article_pq.get_top_priority(return_priority=True))
      except IndexError:
        break
    if debug['kldiv']:
      # Print out the words that contribute most to the KL divergence, for
      # the top-ranked regions
      num_contrib_regions = 5
      num_contrib_words = 25
      errprint("")
      errprint("KL-divergence debugging info:")
      for i in xrange(min(len(regions), num_contrib_regions)):
        region, kldiv = regions[i]
        kldiv, contribs = worddist.slow_kl_divergence(region.worddist,
            partial=self.partial, return_contributing_words=True)
        errprint("  At rank #%s, region %s:" % (i + 1, region))
        errprint("    %30s  %s" % ('Word', 'KL-div contribution'))
        errprint("    %s" % ('-'*50))
        items = sorted(contribs.iteritems(), key=lambda x:abs(x[1]), reverse=True)
        items = items[0:num_contrib_words]
        for (word, contribval) in items:
          errprint("    %30s  %s" % (word, contribval))
        errprint("")

    return regions


# FIXME: Duplicates code from KLDivergenceStrategy

class CosineSimilarityStrategy(GeotagDocumentStrategy):
  def __init__(self, smoothed=False, partial=False):
    self.partial = partial
    self.smoothed = smoothed

  def return_ranked_regions(self, worddist):
    article_pq = PriorityQueue()
    for stat_region in \
        StatRegion.iter_nonempty_regions(nonempty_word_dist=True):
      inds = (stat_region.latind, stat_region.longind)
      if debug['lots']:
        (latind, longind) = inds
        coord = region_indices_to_coord(latind, longind)
        errprint("Nonempty region at indices %s,%s = coord %s, num_articles = %s"
                 % (latind, longind, coord,
                    stat_region.worddist.num_arts_for_word_dist))
      if self.smoothed:
        cossim = fast_smoothed_cosine_similarity(worddist, stat_region.worddist,
                                   partial=self.partial)
      else:
        cossim = fast_cosine_similarity(worddist, stat_region.worddist,
                                   partial=self.partial)
      assert cossim >= 0.0
      # Just in case of round-off problems
      assert cossim <= 1.002
      cossim = 1.002 - cossim
      article_pq.add_task(cossim, stat_region)

    regions = []
    while True:
      try:
        regions.append(article_pq.get_top_priority(return_priority=True))
      except IndexError:
        break

    return regions


# Return the probability of seeing the given document 
class NaiveBayesDocumentStrategy(GeotagDocumentStrategy):
  def __init__(self, opts, use_baseline=True):
    self.opts = opts
    self.use_baseline = use_baseline

  def return_ranked_regions(self, worddist):
    regprobs = {}

    # Determine respective weightings
    if self.use_baseline:
      if self.opts.naive_bayes_weighting == 'equal':
        word_weight = 1.0
        baseline_weight = 1.0
      else:
        baseline_weight = float(self.opts.baseline_weight)
        word_weight = 1.0 - baseline_weight
        word_weight /= worddist.total_tokens
    else:
      word_weight = 1.0
      baseline_weight = 0.0

    for reg in \
        StatRegion.iter_nonempty_regions(nonempty_word_dist=True):
      word_logprob = reg.worddist.get_nbayes_logprob(worddist, self.opts)
      baseline_logprob = \
          log(float(reg.worddist.num_arts_for_links) /
              StatRegion.total_num_arts_for_links)
      logprob = word_weight*word_logprob + baseline_weight*baseline_logprob
      regprobs[reg] = logprob
    return [(reg, prob) for (reg, prob) in sorted(regprobs.iteritems(),
                                          key=lambda x:x[1], reverse=True)]


class PerWordRegionDistributionsStrategy(GeotagDocumentStrategy):
  def return_ranked_regions(self, worddist):
    regdist = RegionDist.get_region_dist_for_word_dist(worddist)
    return regdist.get_ranked_regions()

class GeotagDocumentEvaluator(TestFileEvaluator):
  def __init__(self, opts, strategy, stratname):
    super(GeotagDocumentEvaluator, self).__init__(opts, strategy, stratname)
    self.results = GeotagDocumentResults()
    StatRegion.initialize_regions()

  def output_results(self, final=False):
    self.results.output_geotag_document_results(all_results=final)


class WikipediaGeotagDocumentEvaluator(GeotagDocumentEvaluator):
  # Debug flags:
  #
  #  gridrank: For the given test article number (starting at 1), output
  #            a grid of the predicted rank for regions around the true
  #            region.  Multiple articles can have the rank output by
  #            specifying this option multiple times, e.g.
  #
  #            --debug 'gridrank=45,gridrank=58'
  #
  #  gridranksize: Size of the grid, in numbers of articles on a side.
  #                This is a single number, and the grid will be a square
  #                centered on the true region.
  register_list_debug_param('gridrank')
  debug['gridranksize'] = '11'

  def iter_documents(self, filename):
    for art in ArticleTable.articles_by_split[self.opts.eval_set]:
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

  def would_skip_document(self, article, doctag):
    if not article.dist:
      # This can (and does) happen when --max-time-per-stage is set,
      # so that the counts for many articles don't get read in.
      if self.opts.max_time_per_stage == 0 and self.opts.num_training_docs == 0:
        warning("Can't evaluate article %s without distribution" % article)
      self.results.record_geotag_document_other_stat('Skipped articles')
      return True
    return False

  def evaluate_document(self, article, doctag):
    if self.would_skip_document(article, doctag):
      return False
    assert article.dist.finished
    true_latind, true_longind = coord_to_stat_region_indices(article.coord)
    true_statreg = StatRegion.find_region_for_coord(article.coord)
    naitr = true_statreg.worddist.num_arts_for_word_dist
    if debug['lots'] or debug['commontop']:
      errprint("Evaluating article %s with %s word-dist articles in true region" %
               (article, naitr))
    if self.opts.oracle_results:
      rank = 1
      latind = true_latind
      longind = true_longind
    else:
      regs = self.strategy.return_ranked_regions(article.dist)
      rank = 1
      for reg, val in regs:
        if reg.latind == true_latind and reg.longind == true_longind:
          break
        rank += 1
      else:
        rank = 1000000000
      latind = regs[0][0].latind
      longind = regs[0][0].longind
    want_indiv_results = (not self.opts.oracle_results and
        not self.opts.no_individual_results)
    stats = self.results.record_geotag_document_result(rank, article.coord,
        latind, longind, num_arts_in_true_region=naitr,
        return_stats=want_indiv_results)
    if naitr == 0:
      self.results.record_geotag_document_other_stat(
          'Articles with no training articles in region')
    if want_indiv_results:
      errprint("%s:Article %s:" % (doctag, article))
      errprint("%s:  %d types, %d tokens" % (
        doctag, len(article.dist.counts), article.dist.total_tokens))
      errprint("%s:  True region at rank: %s" % (doctag, rank))
      errprint("%s:  True region: %s" % (doctag, true_statreg))
      for i in xrange(5):
        errprint("%s:  Predicted region (at rank %s): %s" %
            (doctag, i+1, regs[i][0]))
      errprint("%s:  Distance %.2f miles to true region center at %s" %
               (doctag, stats['true_truedist'], stats['true_center']))
      errprint("%s:  Distance %.2f miles to predicted region center at %s" %
               (doctag, stats['pred_truedist'], stats['pred_center']))
      assert doctag[0] == '#'
      if debug['gridrank'] == True or doctag[1:] in debug['gridrank']:
        grsize = int(debug['gridranksize'])
        min_latind = true_latind - grsize // 2
        max_latind = min_latind + grsize - 1
        min_longind = true_longind - grsize // 2
        max_longind = min_longind + grsize - 1
        grid = {}
        rank = 1
        for reg, val in regs:
          if (reg.latind >= min_latind and reg.latind <= max_latind and
              reg.longind >= min_longind and reg.longind <= max_longind):
            grid[(reg.latind,reg.longind)] = (reg, val, rank)
          rank += 1

        errprint("Grid ranking, gridsize %dx%d" % (grsize, grsize))
        errprint("NW corner: %s" %
            stat_region_indices_to_nw_corner_coord(max_latind, min_longind))
        errprint("SE corner: %s" %
            stat_region_indices_to_se_corner_coord(min_latind, max_longind))
        for doit in [0, 1]:
          if doit == 0:
            errprint("Grid for ranking:")
          else:
            errprint("Grid for goodness/distance:")
          for lat in fromto(max_latind, min_latind):
            for long in fromto(min_longind, max_longind):
              regvalrank = grid.get((lat,long), None)
              if not regvalrank:
                errout(" %-8s" % "empty")
              else:
                reg, val, rank = regvalrank
                if doit == 0:
                  showit = rank
                else:
                  showit = val
                if lat == true_latind and long == true_longind:
                  errout("!%-8.6s" % showit)
                else:
                  errout(" %-8.6s" % showit)
            errout("\n")

    return True


class TitledDocument(object):
  def __init__(self, title, text):
    self.title = title
    self.text = text

class PCLTravelGeotagDocumentEvaluator(GeotagDocumentEvaluator):
  def iter_documents(self, filename):
    # Find XML nodes using depth-first search.  'match' is either a
    # string (match localName on that string) or a predicate.
    def find_node_dfs(node, match):
      if not callable(match):
        def matchpred(node):
          return node.localName == match
      else:
        matchpred = match
      def children(node):
        return node.childNodes
      return depth_first_search(node, matchpred, children)

    # Return the text within a given node subtree.
    def get_node_text(node):
      return ''.join(t.data for t in
          find_node_dfs(node, lambda x:x.nodeType == md.Attr.TEXT_NODE))

    # On error, just return, so that we don't have problems when called
    # on the whole PCL corpus dir (which includes non-XML files).
    try:
      dom = md.parse(filename)
    except:
      warning("Unable to parse XML filename: %s" % filename)
      return
    for chapter in find_node_dfs(dom, (lambda x:
        x.localName == 'div' and x.getAttribute('type') == 'chapter')):
      heads = []
      nonheads = []
      for child in chapter.childNodes:
        if child.localName == 'head':
          heads.append(child)
        else:
          nonheads.append(child)
      headtext = ''.join(get_node_text(x) for x in heads)
      text = ''.join(get_node_text(x) for x in nonheads)
      #errprint("Head text: %s" % headtext)
      #errprint("Non-head text: %s" % text)
      yield TitledDocument(headtext, text)

  def evaluate_document(self, doc, doctag):
    dist = WordDist()
    if Opts.include_stopwords_in_article_dists:
      the_stopwords = {}
    else:
      the_stopwords = stopwords
    for text in (doc.title, doc.text):
      dist.add_words(split_text_into_words(text, ignore_punc=True),
                     ignore_case=not Opts.preserve_case_words,
                     stopwords=the_stopwords)
    dist.finish(minimum_word_count=Opts.minimum_word_count)
    regs = self.strategy.return_ranked_regions(dist)
    errprint("")
    errprint("Article with title: %s" % doc.title)
    num_regs_to_show = 5
    for (rank, regval) in izip(xrange(1, 1 + num_regs_to_show), regs):
      reg, val = regval
      if debug['struct']:
        errprint("  Rank %d, goodness %g:" % (rank, val))
        print_structure(reg.struct(), indent=4)
      else:
        errprint("  Rank %d, goodness %g: %s" % (rank, val, reg.shortstr()))
    
    return True

############################################################################
#                                Segmentation                              #
############################################################################

# General idea: Keep track of best possible segmentations up to a maximum
# number of segments.  Either do it using a maximum number of segmentations
# (e.g. 100 or 1000) or all within a given factor of the best score (the
# "beam width", e.g. 10^-4).  Then given the existing best segmentations,
# we search for new segmentations with more segments by looking at all
# possible ways of segmenting each of the existing best segments, and
# finding the best score for each of these.  This is a slow process -- for
# each segmentation, we have to iterate over all segments, and for each
# segment we have to look at all possible ways of splitting it, and for
# each split we have to look at all assignments of regions to the two
# new segments.  It also seems that we're likely to consider the same
# segmentation multiple times.
#
# In the case of per-word region dists, we can maybe speed things up by
# computing the non-normalized distributions over each paragraph and then
# summing them up as necessary.

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
      ArticleTable.record_article(art, art)
      StatRegion.add_article_to_region(art)

  read_article_data_file(filename, process, article_type=StatArticle,
                         maxtime=Opts.max_time_per_stage)

  for x in redirects:
    redart = ArticleTable.lookup_article(x.redir)
    if redart:
      ArticleTable.record_article(x, redart)


# Parse the result of a previous run of --output-counts and generate
# a unigram distribution for Naive Bayes matching.  We do a simple version
# of Good-Turing smoothing where we assign probability mass to unseen
# words equal to the probability mass of all words seen once, and rescale
# the remaining probabilities accordingly.

def read_word_counts(filename):

  # This is basically a one-off debug statement because of the fact that
  # the experiments published in the paper used a word-count file generated
  # using an older algorithm for determining the geotagged coordinate of
  # a Wikipedia article.  We didn't record the corresponding article-data
  # file, so we need a way of regenerating it using the intersection of
  # articles in the article-data file we actually used for the experiments
  # and the word-count file we used.
  if debug['wordcountarts']:
    # Change this if you want a different file name
    wordcountarts_filename = 'wordcountarts-combined-article-data.txt'
    wordcountarts_file = open(wordcountarts_filename, "w")
    # See write_article_data_file() in process_article_data.py
    outfields = combined_article_data_outfields
    field_types = get_output_field_types(outfields)
    uniprint('\t'.join(outfields), outfile=wordcountarts_file)

  def one_article_probs():
    if total_tokens == 0: return
    art = ArticleTable.lookup_article(title)
    if not art:
      warning("Skipping article %s, not in table" % title)
      ArticleTable.num_articles_with_word_counts_but_not_in_table += 1
      return
    if debug['wordcountarts']:
      art.output_row(wordcountarts_file, outfields, field_types)
    ArticleTable.num_word_count_articles_by_split[art.split] += 1
    # If we are evaluating on the dev set, skip the test set and vice
    # versa, to save memory and avoid contaminating the results.
    if art.split != 'training' and art.split != Opts.eval_set:
      return
    art.dist = WordDist()
    # Don't train on test set
    art.dist.set_word_distribution(total_tokens, wordhash,
        note_globally=(art.split == 'training'))

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
      if status.item_processed(maxtime=Opts.max_time_per_stage):
        break
      if (Opts.num_training_docs and
          status.num_processed() >= Opts.num_training_docs):
        errprint("")
        errprint("Finishing reading word counts after %d documents" %
            status.num_processed())
        break

      title = m.group(1)
      wordhash = intdict()
      total_tokens = 0
    elif line.startswith('Article coordinates: ') or \
        line.startswith('Article ID: '):
      pass
    else:
      m = re.match('(.*) = ([0-9]+)$', line)
      if not m:
        warning("Strange line, can't parse: title=%s: line=%s" % (title, line))
        continue
      word = m.group(1)
      if not Opts.preserve_case_words: word = word.lower()
      count = int(m.group(2))
      if word in stopwords and not Opts.include_stopwords_in_article_dists:
        continue
      word = internasc(word)
      total_tokens += count
      wordhash[word] += count
  else:
    one_article_probs()

  if debug['wordcountarts']:
    wordcountarts_file.close()
  errprint("Finished reading distributions from %s articles." % (status.num_processed()))
  ArticleTable.num_articles_with_word_counts = status.num_processed()
  output_resource_usage()

def finish_word_counts():
  WordDist.finish_global_distribution()
  ArticleTable.finish_article_distributions()
  errprint("")
  errprint("-------------------------------------------------------------------------")
  errprint("Article count statistics:")
  total_arts_in_table = 0
  total_arts_with_word_counts = 0
  total_arts_with_dists = 0
  for (split, totaltoks) in ArticleTable.word_tokens_by_split.iteritems():
    errprint("For split '%s':" % split)
    arts_in_table = len(ArticleTable.articles_by_split[split])
    arts_with_word_counts = ArticleTable.num_word_count_articles_by_split[split]
    arts_with_dists = ArticleTable.num_dist_articles_by_split[split]
    total_arts_in_table += arts_in_table
    total_arts_with_word_counts += arts_with_word_counts
    total_arts_with_dists += arts_with_dists
    errprint("  %s articles in article table" % arts_in_table)
    errprint("  %s articles with word counts seen (and in table)" % arts_with_word_counts)
    errprint("  %s articles with distribution computed, %s total tokens, %.2f tokens/article"
        % (arts_with_dists, totaltoks,
          # Avoid division by zero
          float(totaltoks)/(arts_in_table + 1e-100)))
  errprint("Total: %s articles with word counts seen" %
           ArticleTable.num_articles_with_word_counts)
  errprint("Total: %s articles in article table" % total_arts_in_table)
  errprint("Total: %s articles with word counts seen but not in article table" %
           ArticleTable.num_articles_with_word_counts_but_not_in_table)
  errprint("Total: %s articles with word counts seen (and in table)" %
           total_arts_with_word_counts)
  errprint("Total: %s articles with distribution computed" %
           total_arts_with_dists)


class Gazetteer(object):
  # For each toponym (name of location), value is a list of Locality items,
  # listing gazetteer locations and corresponding matching Wikipedia articles.
  lower_toponym_to_location = listdict()

  # For each toponym corresponding to a division higher than a locality,
  # list of divisions with this name.
  lower_toponym_to_division = listdict()

  # Table of all toponyms seen in evaluation files, along with how many times
  # seen.  Used to determine when caching of certain toponym-specific values
  # should be done.
  #toponyms_seen_in_eval_files = intdict()


class WorldGazetteer(Gazetteer):

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

  @classmethod
  def match_world_gazetteer_entry(cls, line):
    # Split on tabs, make sure at least 11 fields present and strip off
    # extra whitespace
    fields = re.split(r'\t', line.strip()) + ['']*11
    fields = [x.strip() for x in fields[0:11]]
    (id, name, altnames, orig_script_name, typ, population, lat, long,
     div1, div2, div3) = fields

    # Skip places without coordinates
    if not lat or not long:
      if debug['lots']:
        errprint("Skipping location %s (div %s/%s/%s) without coordinates" %
                 (name, div1, div2, div3))
      return

    if lat == '0' and long == '9999':
      if debug['lots']:
        errprint("Skipping location %s (div %s/%s/%s) with bad coordinates" %
                 (name, div1, div2, div3))
      return

    # Create and populate a Locality object
    loc = Locality(name, Coord(int(lat) / 100., int(long) / 100.))
    loc.type = typ
    if altnames:
      loc.altnames = re.split(', ', altnames)
    # Add the given location to the division the location is in
    loc.div = Division.find_division_note_point(loc, (div1, div2, div3))
    if debug['lots']:
      errprint("Saw location %s (div %s/%s/%s) with coordinates %s" %
               (loc.name, div1, div2, div3, loc.coord))

    # Record the location.  For each name for the location (its
    # canonical name and all alternates), add the location to the list of
    # locations associated with the name.  Record the name in lowercase
    # for ease in matching.
    for name in [loc.name] + loc.altnames:
      loname = name.lower()
      if debug['lots']:
        errprint("Noting lower_toponym_to_location for toponym %s, canonical name %s"
                 % (name, loc.name))
      cls.lower_toponym_to_location[loname] += [loc]

    # We start out looking for articles whose distance is very close,
    # then widen until we reach Opts.max_dist_for_close_match.
    maxdist = 5
    while maxdist <= Opts.max_dist_for_close_match:
      match = ArticleTable.find_match_for_locality(loc, maxdist)
      if match: break
      maxdist *= 2

    if not match: 
      if debug['lots']:
        errprint("Unmatched name %s" % loc.name)
      return
    
    # Record the match.
    loc.match = match
    match.location = loc
    if debug['lots']:
      errprint("Matched location %s (coord %s) with article %s, dist=%s"
               % (loc.name, loc.coord, match,
                  spheredist(loc.coord, match.coord)))

  # Read in the data from the World gazetteer in FILENAME and find the
  # Wikipedia article matching each entry in the gazetteer.  For localities,
  # add them to the region-map that covers the earth if ADD_TO_REGION_MAP is
  # true.
  @classmethod
  def read_world_gazetteer_and_match(cls, filename):
    errprint("Matching gazetteer entries in %s..." % filename)
    status = StatusMessage('gazetteer entry')

    # Match each entry in the gazetteer
    for line in uchompopen(filename):
      if debug['lots']:
        errprint("Processing line: %s" % line)
      cls.match_world_gazetteer_entry(line)
      if status.item_processed(maxtime=Opts.max_time_per_stage):
        break

    Division.finish_all()
    errprint("Finished matching %s gazetteer entries." % (status.num_processed()))
    output_resource_usage()

# If given a directory, yield all the files in the directory; else just
# yield the file.
def iter_directory_files(dir):
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
    canon_options = {}

    ######### Input files
    op.add_option("--stopwords-file",
                  help="""File containing list of stopwords.""",
                  metavar="FILE")
    op.add_option("-a", "--article-data-file", "--a", action='append',
                  help="""File containing info about Wikipedia articles.""",
                  metavar="FILE")
    op.add_option("--gazetteer-file", "--gf",
                  help="""File containing gazetteer information to match.""",
                  metavar="FILE")
    op.add_option("--gazetteer-type", "--gt", type='choice', default="world",
                  choices=['world', 'db'],
                  help="""Type of gazetteer file specified using --gazetteer;
default '%default'.""")
    op.add_option("--counts-file", "--cf", action='append',
                  help="""File containing output from a prior run of
--output-counts, listing for each article the words in the article and
associated counts.""",
                  metavar="FILE")
    op.add_option("-e", "--eval-file", "--e",
                  help="""File or directory containing files to evaluate on.
Each file is read in and then disambiguation is performed.""",
                  metavar="FILE")
    op.add_option("-f", "--eval-format", "--f", type='choice',
                  default="wiki", choices=['tr-conll', 'wiki', 'raw-text',
                                           'pcl-travel'],
                  help="""Format of evaluation file(s).  Default '%default'.""")
    op.add_option("--eval-set", "--es", type='choice',
                  default="dev", choices=['dev', 'devel', 'test'],
                  help="""Set to use for evaluation when --eval-format=wiki
and --mode=geotag-documents ('dev' or 'devel' for the development set,
'test' for the test set).  Default '%default'.""")
    canon_options['eval_set'] = {'devel':'dev'}

    ########## Misc options for handling distributions
    op.add_option("--preserve-case-words", "--pcw", action='store_true',
                  default=False,
                  help="""Don't fold the case of words used to compute and
match against article distributions.  Note that this does not apply to
toponyms; currently, toponyms are always matched case-insensitively.""")
    op.add_option("--include-stopwords-in-article-dists", action='store_true',
                  default=False,
                  help="""Include stopwords when computing word
distributions.""")
    op.add_option("--naive-bayes-context-len", "--nbcl", type='int', default=10,
                  help="""Number of words on either side of a toponym to use
in Naive Bayes matching.  Default %default.""")
    op.add_option("--minimum-word-count", "--mwc", type='int', default=1,
                  help="""Minimum count of words to consider in word
distributions.  Words whose count is less than this value are ignored.""")

    ########## Misc options for controlling matching
    op.add_option("--max-dist-for-close-match", "--mdcm", type='float',
                  default=80,
                  help="""Maximum number of miles allowed when looking for a
close match.  Default %default.""")
    op.add_option("--max-dist-for-outliers", "--mdo", type='float', default=200,
                  help="""Maximum number of miles allowed between a point and
any others in a division.  Points farther away than this are ignored as
"outliers" (possible errors, etc.).  Default %default.""")

    ########## Basic options for determining operating mode and strategy
    op.add_option("-m", "--mode", "--m", type='choice',
                  default='geotag-documents',
                  choices=['geotag-toponyms',
                           'geotag-documents',
                           'generate-kml',
                           'segment-geotag-documents'],
                  help="""Action to perform.

'geotag-documents' finds the proper location for each document (or article)
in the test set.

'geotag-toponyms' finds the proper location for each toponym in the test set.
The test set is specified by --eval-file.  Default '%default'.

'segment-geotag-documents' simultaneously segments a document into sections
covering a specific location and determines that location. (Not yet
implemented.)

'generate-kml' generates KML files for some set of words, showing the
distribution over regions that the word determines.  Use '--kml-words' to
specify the words whose distributions should be outputted.  See also
'--kml-prefix' to specify the prefix of the files outputted, and
'--kml-transform' to specify the function to use (if any) to transform
the probabilities to make the distinctions among them more visible.
""")

    op.add_option("-s", "--strategy", "--s", type='choice', action='append', 
        default=None,
        choices=[
          'baseline', 'none',
          'full-kl-divergence', 'full-kldiv', 'full-kl',
          'partial-kl-divergence', 'partial-kldiv', 'partial-kl',
          'symmetric-full-kl-divergence', 'sym-kldiv', 'sym-kl',
          'symmetric-partial-kl-divergence', 'sym-partial-kldiv',
            'sym-partial-kl',
          'cosine-similarity', 'cossim',
          'partial-cosine-similarity', 'partial-cossim',
          'smoothed-cosine-similarity', 'smoothed-cossim',
          'smoothed-partial-cosine-similarity', 'smoothed-partial-cossim',
          'average-cell-probability', 'avg-cell-prob', 'acp',
          'naive-bayes-with-baseline', 'nb-base',
          'naive-bayes-no-baseline', 'nb-nobase'
          ],
        help="""Strategy/strategies to use for geotagging.
'baseline' means just use the baseline strategy (see --baseline-strategy).

'none' means don't do any geotagging.  Useful for testing the parts that
read in data and generate internal structures.

The other possible values depend on which mode is in use
(--mode=geotag-toponyms or --mode=geotag-documents).

For geotag-toponyms:

'naive-bayes-with-baseline' (or 'nb-base') means also use the words around the
toponym to be disambiguated, in a Naive-Bayes scheme, using the baseline as the
prior probability; 'naive-bayes-no-baseline' (or 'nb-nobase') means use uniform
prior probability.  Default is 'baseline'.

For geotag-documents:

'full-kl-divergence' (or 'full-kldiv') searches for the region where the KL
divergence between the article and region is smallest.
'partial-kl-divergence' (or 'partial-kldiv') is similar but uses an
abbreviated KL divergence measure that only considers the words seen in the
article; empirically, this appears to work just as well as the full KL
divergence. 'average-cell-probability' (or
'regdist') involves computing, for each word, a probability distribution over
regions using the word distribution of each region, and then combining the
distributions over all words in an article, weighted by the count the word in
the article.  Default is 'partial-kl-divergence'.

NOTE: Multiple --strategy options can be given, and each strategy will
be tried, one after the other.""")
    canon_options['strategy'] = {
        'full-kldiv':'full-kl-divergence',
        'full-kl':'full-kl-divergence',
        'partial-kldiv':'partial-kl-divergence',
        'partial-kl':'partial-kl-divergence',
        'sym-full-kldiv':'symmetric-full-kl-divergence',
        'sym-full-kl':'symmetric-full-kl-divergence',
        'sym-partial-kldiv':'symmetric-partial-kl-divergence',
        'sym-partial-kl':'symmetric-partial-kl-divergence',
        'cossim':'cosine-similarity',
        'partial-cossim':'partial-cosine-similarity',
        #'regdist':'average-cell-probability',
        #'per-word-region-distribution':'average-cell-probability',
        'avg-cell-prob':'average-cell-probability',
        'acp':'average-cell-probability',
        'nb-base':'naive-bayes-with-baseline',
        'nb-nobase':'naive-bayes-no-baseline'}

    op.add_option("--baseline-strategy", "--bs", type='choice', action='append',
                  default=None,
                  choices=['internal-link', 'link',
                           'random',
                           'num-articles', 'num-arts', 'numarts',
                           'link-most-common-toponym',
                           'region-distribution-most-common-toponym',
                           'regdist-most-common-toponym'],
                  help="""Strategy to use to compute the baseline.

'internal-link' (or 'link') means use number of internal links pointing to the
article or region.

'random' means choose randomly.

'num-articles' (or 'num-arts' or 'numarts'; only in region-type matching) means
use number of articles in region.

'link-most-common-toponym' (only in --mode=geotag-documents) means to look
for the toponym that occurs the most number of times in the article, and
then use the internal-link baseline to match it to a location.

'regdist-most-common-toponym' (only in --mode=geotag-documents) is similar,
but uses the region distribution of the most common toponym.

Default '%default'.

NOTE: Multiple --baseline-strategy options can be given, and each strategy will
be tried, one after the other.  Currently, however, the *-most-common-toponym
strategies cannot be mixed with other baseline strategies, or with non-baseline
strategies, since they require that --preserve-case-words be set internally.""")
    canon_options['baseline_strategy'] = (
        {'link':'internal-link',
         'num-arts':'num-articles',
         'numarts':'num-articles',
         'region-distribution-most-common-toponym':
           'regdist-most-common-toponym'}
        )

    op.add_option("--baseline-weight", "--bw", type='float', metavar="WEIGHT",
                  default=0.5,
                  help="""Relative weight to assign to the baseline (prior
probability) when doing weighted Naive Bayes.  Default %default.""")
    op.add_option("--naive-bayes-weighting", "--nbw", type='choice',
                  default="equal",
                  choices=['equal', 'equal-words', 'distance-weighted'],
                  help="""Strategy for weighting the different probabilities
that go into Naive Bayes.  If 'equal', do pure Naive Bayes, weighting the
prior probability (baseline) and all word probabilities the same.  If
'equal-words', weight all the words the same but collectively weight all words
against the baseline, giving the baseline weight according to --baseline-weight
and assigning the remainder to the words.  If 'distance-weighted', similar to
'equal-words' but don't weight each word the same as each other word; instead,
weight the words according to distance from the toponym.""")
    op.add_option("--width-of-stat-region", type='int', default=1,
                  help="""Width of the region used to compute a statistical
distribution for geotagging purposes, in terms of number of tiling regions.
Default %default.""")
    op.add_option("--degrees-per-region", "--dpr", type='float', default=None,
                  help="""Size (in degrees) of the tiling regions that cover
the earth.  Some number of tiling regions are put together to form the region
used to construct a statistical distribution.  No default; the default of
'--miles-per-region' is used instead.""")
    op.add_option("--miles-per-region", "--mpr", type='float', default=100.0,
                  help="""Size (in miles) of the tiling regions that cover
the earth.  Some number of tiling regions are put together to form the region
used to construct a statistical distribution.  Default %default.""")
    op.add_option("--context-type", "--ct", type='choice',
                  default="region-dist-article-links",
                  choices=['article', 'region', 'region-dist-article-links'],
                  help="""Type of context used when doing disambiguation.
There are two cases where this choice applies: When computing a word
distribution, and when counting the number of incoming internal links.
'article' means use the article itself for both.  'region' means use the
region for both. 'region-dist-article-links' means use the region for
computing a word distribution, but the article for counting the number of
incoming internal links.  Note that this only applies when
--mode='geotag-toponyms'; in --mode='geotag-documents', only regions are
considered.  Default '%default'.""")

    op.add_option("-k", "--kml-words", "--kw",
                  help="""Words to generate KML distributions for, when
--mode='generate-kml'.  Each word should be separated by a comma.  A separate
file is generated for each word, using '--kml-prefix' and adding '.kml'.""")
    op.add_option("--kml-prefix", "--kp",
                  default='kml-dist.',
                  help="""Prefix to use for KML files outputted.
Default '%default',""")
    op.add_option("--kml-transform", "--kt", "--kx", type='choice',
                  default="none",
                  choices=['none', 'log', 'logsquared'],
                  help="""Type of transformation to apply to the probabilities
when generating KML, possibly to try and make the low values more visible.
Possibilities are 'none' (no transformation), 'log' (take the log), and
'logsquared' (negative of squared log).  Default '%default'.""")

    op.add_option("--num-training-docs", "--ntrain", type='int', default=0,
                  help="""Maximum number of training documents to use.
0 means no limit.  Default %default.""")
    op.add_option("--num-test-docs", "--ntest", type='int', default=0,
                  help="""Maximum number of test documents to process.
0 means no limit.  Default %default.""")
    op.add_option("--skip-initial-test-docs", "--skip-initial", type='int', default=0,
                  help="""Skip this many test docs at beginning.  Default 0.""")
    op.add_option("--every-nth-test-doc", "--every-nth", type='int', default=1,
                  help="""Only process every Nth test doc.  Default 1, i.e. process all.""")
#    op.add_option("--skip-every-n-test-docs", "--skip-n", type='int', default=0,
#                  help="""Skip this many after each one processed.  Default 0.""")
    op.add_option("--no-individual-results", "--no-results",
                  action='store_true', default=False,
                  help="""Don't show individual results for each test document.""")
    op.add_option("--oracle-results",
                  action='store_true', default=False,
                  help="""Only compute oracle results (much faster).""")
    op.add_option("--lru-cache-size", "--lru",
                  type='int', default=400,
                  help="""Number of entries in the LRU cache.""")

    return canon_options

  def handle_arguments(self, opts, op, args):
    global Opts
    Opts = opts
    global debug
    if opts.debug:
      params = re.split(r'[:;\s]+', opts.debug)
      # Allow params with values, and allow lists of values to be given
      # by repeating the param
      for f in params:
        if '=' in f:
          (param, value) = re.split('=', f, 1)
          if param in list_debug_params:
            values = re.split('[,]', value)
            debug[param] += values
          else:
            debug[param] = value
        else:
          debug[f] = True
      WordDist.set_debug(debug)

    class Params(object):
      pass

    params = Params()
    params.need_to_read_stopwords = True # Always needed!
   
    # Canonicalize options
    if not opts.strategy:
      if opts.mode == 'geotag-documents':
        opts.strategy = ['partial-kl-divergence']
      elif opts.mode == 'geotag-toponyms':
        opts.strategy = ['baseline']
      else:
        opts.strategy = []

    if not opts.baseline_strategy:
      opts.baseline_strategy = ['internal-link']

    if 'baseline' in opts.strategy:
      need_case = False
      need_no_case = False
      for bstrat in opts.baseline_strategy:
        if bstrat.endswith('most-common-toponym'):
          need_case = True
        else:
          need_no_case = True
      if need_case:
        if len(opts.strategy) > 1 or need_no_case:
          # That's because we have to set --preserve-case-words, which we
          # generally don't want set for other strategies and which affects
          # the way we construct the training-document distributions.
          op.error("Can't currently mix *-most-common-toponym baseline strategy with other strategies")
        opts.preserve_case_words = True

    # FIXME! Can only currently handle World-type gazetteers.
    if opts.gazetteer_type != 'world':
      op.error("Currently can only handle world-type gazetteers")

    if opts.miles_per_region <= 0:
      op.error("Miles per region must be positive")
    global degrees_per_region
    if opts.degrees_per_region:
      degrees_per_region = opts.degrees_per_region
    else:
      degrees_per_region = opts.miles_per_region / miles_per_degree
    global maximum_latind, minimum_latind, maximum_longind, minimum_longind
    # The actual maximum latitude is exactly 90 (the North Pole).  But if we
    # set degrees per region to be a number that exactly divides 180, and we
    # use maximum_latitude = 90 in the following computations, then we would
    # end up with the North Pole in a region by itself, something we probably
    # don't want.
    maximum_latind, maximum_longind = \
      coord_to_tiling_region_indices(Coord(maximum_latitude - 1e-10,
                                           maximum_longitude))
    minimum_latind, minimum_longind = \
      coord_to_tiling_region_indices(Coord(minimum_latitude,
                                           minimum_longitude))

    if opts.width_of_stat_region <= 0:
      op.error("Width of statistical region must be positive")

    ### Start reading in the files and operating on them ###

    if opts.mode.startswith('geotag'):
      if (opts.mode == 'geotag-toponyms' and opts.strategy == ['baseline']):
        pass
      elif not opts.counts_file:
        op.error("Must specify counts file")

    if opts.mode == 'geotag-toponyms':
      self.need('gazetteer_file')

    if opts.eval_format == 'raw-text':
      # FIXME!!!!
      op.error("Raw-text reading not implemented yet")

    if opts.mode == 'geotag-documents':
      if opts.eval_format not in ['pcl-travel', 'wiki']:
        op.error("For --mode=geotag-documents, eval-format must be 'pcl-travel' or 'wiki'")
    elif opts.mode == 'geotag-toponyms':
      if opts.baseline_strategy.endswith('most-common-toponym'):
        op.error("--baseline-strategy=%s only compatible with --mode=geotag-documents"
            % opts.baseline_strategy)
      for stratname in opts.strategy:
        if stratname not in ['baseline', 'naive-bayes-with-baseline',
            'naive-bayes-no-baseline']:
          op.error("Strategy '%s' invalid for --mode=geotag-toponyms" %
                   stratname)
      if opts.eval_format not in ['tr-conll', 'wiki']:
        op.error("For --mode=geotag-toponyms, eval-format must be 'tr-conll' or 'wiki'")

    if opts.mode == 'geotag-documents' and opts.eval_format == 'wiki':
      pass # No need for evaluation file, uses the counts file
    elif opts.mode.startswith('geotag'):
      self.need('eval_file', 'evaluation file(s)')

    if opts.mode == 'generate-kml':
      self.need('kml_words')
    elif opts.kml_words:
      op.error('--kml-words only compatible with --mode=generate-kml')

    self.need('article_data_file')

    return params

  def implement_main(self, opts, params, args):
    if params.need_to_read_stopwords:
      read_stopwords(opts.stopwords_file)
    for fn in opts.article_data_file:
      read_article_data(fn)

    #errprint("Processing evaluation file(s) %s for toponym counts..." % opts.eval_file)
    #process_dir_files(opts.eval_file, count_toponyms_in_file)
    #errprint("Number of toponyms seen: %s" % len(toponyms_seen_in_eval_files))
    #errprint("Number of toponyms seen more than once: %s" % \
    #  len([foo for (foo,count) in toponyms_seen_in_eval_files.iteritems() if
    #       count > 1]))
    #output_reverse_sorted_table(toponyms_seen_in_eval_files,
    #                            outfile=sys.stderr)

    # Read in the words-counts file
    for fn in opts.counts_file:
      read_word_counts(fn)
    if opts.counts_file:
      finish_word_counts()

    if opts.gazetteer_file:
      WorldGazetteer.read_world_gazetteer_and_match(opts.gazetteer_file)

    if opts.mode == 'generate-kml':
      StatRegion.initialize_regions()
      words = opts.kml_words.split(',')
      for word in words:
        regdist = RegionDist.get_region_dist(word)
        if not regdist.normalized:
          warning("""Non-normalized distribution, apparently word %s not seen anywhere.
Not generating an empty KML file.""" % word)
        else:
          regdist.generate_kml_file('%s%s.kml' % (opts.kml_prefix, word))
      return

    def yield_strategies():
      for stratname in opts.strategy:
        if opts.mode == 'geotag-toponyms':
          # Generate strategy object
          if stratname == 'baseline':
            for basestratname in opts.baseline_strategy:
              yield ('baseline ' + basestratname,
                  BaselineGeotagToponymStrategy(opts, basestratname))
          else:
            strategy = NaiveBayesToponymStrategy(opts,
                use_baseline=(stratname == 'naive-bayes-with-baseline'))
            yield (stratname, strategy)

        elif opts.mode == 'geotag-documents':
          if stratname == 'baseline':
            for basestratname in opts.baseline_strategy:
              yield ('baseline ' + basestratname,
                  BaselineGeotagDocumentStrategy(basestratname))
          else:
            if stratname.startswith('naive-bayes-'):
              strategy = NaiveBayesDocumentStrategy(opts,
                  use_baseline=(stratname == 'naive-bayes-with-baseline'))
            elif stratname == 'average-cell-probability':
              strategy = PerWordRegionDistributionsStrategy()
            elif stratname == 'cosine-similarity':
              strategy = CosineSimilarityStrategy(smoothed=False, partial=False)
            elif stratname == 'partial-cosine-similarity':
              strategy = CosineSimilarityStrategy(smoothed=False, partial=True)
            elif stratname == 'smoothed-cosine-similarity':
              strategy = CosineSimilarityStrategy(smoothed=True, partial=False)
            elif stratname == 'smoothed-partial-cosine-similarity':
              strategy = CosineSimilarityStrategy(smoothed=True, partial=True)
            elif stratname == 'full-kl-divergence':
              strategy = KLDivergenceStrategy(symmetric=False, partial=False)
            elif stratname == 'partial-kl-divergence':
              strategy = KLDivergenceStrategy(symmetric=False, partial=True)
            elif stratname == 'symmetric-full-kl-divergence':
              strategy = KLDivergenceStrategy(symmetric=True, partial=False)
            elif stratname == 'symmetric-partial-kl-divergence':
              strategy = KLDivergenceStrategy(symmetric=True, partial=True)
            elif stratname == 'none':
              continue
            else:
              assert False
            yield (stratname, strategy)

    for (stratname, strategy) in yield_strategies():
      # Generate reader object
      if opts.mode == 'geotag-toponyms':
        if opts.eval_format == 'tr-conll':
          evalobj = TRCoNLLGeotagToponymEvaluator(opts, strategy, stratname)
        else:
          evalobj = WikipediaGeotagToponymEvaluator(opts, strategy, stratname)
      elif opts.mode == 'geotag-documents':
        if opts.eval_format == 'pcl-travel':
          evalobj = PCLTravelGeotagDocumentEvaluator(opts, strategy, stratname)
        else:
          evalobj = WikipediaGeotagDocumentEvaluator(opts, strategy, stratname)

      errprint("Processing evaluation file/dir %s..." % opts.eval_file)
      if opts.eval_file:
        iterfiles = iter_directory_files(opts.eval_file)
      else:
        iterfiles = ["foo"]
      evalobj.evaluate_and_output_results(iterfiles)

if __name__ == "__main__":
  WikiDisambigProgram()
