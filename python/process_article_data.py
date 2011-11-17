#!/usr/bin/env python

#######
####### process_article_data.py
#######
####### Copyright (c) 2010 Ben Wing.
#######

from nlputil import *

#!/usr/bin/env python

############################################################################
#                                  Main code                               #
############################################################################

minimum_latitude = -90.0
maximum_latitude = 90.0
minimum_longitude = -180.0
maximum_longitude = 180.0 - 1e-10

# A 2-dimensional coordinate.
#
# The following fields are defined:
#
#   lat, long: Latitude and longitude of coordinate.

class Coord(object):
  __slots__ = ['lat', 'long']

  ### If coerce_within_bounds=True, then force the values to be within
  ### the allowed range, by wrapping longitude and bounding latitude.
  def __init__(self, lat, long, coerce_within_bounds=False):
    if coerce_within_bounds:
      if lat > maximum_latitude: lat = maximum_latitude
      while long > maximum_longitude: long -= 360.
      if lat < minimum_latitude: lat = minimum_latitude
      while long < minimum_longitude: long += 360.
    self.lat = lat
    self.long = long

  def __str__(self):
    return '(%.2f,%.2f)' % (self.lat, self.long)

# A Wikipedia article.  Defined fields:
#
#   title: Title of article.
#   id: ID of article, as an int.
#   coord: Coordinates of article.
#   incoming_links: Number of incoming links, or None if unknown.
#   split: Split of article ('training', 'dev', 'test')
#   redir: If this is a redirect, article title that it redirects to; else
#          an empty string.
#   namespace: Namespace of article (e.g. 'Main', 'Wikipedia', 'File')
#   is_list_of: Whether article title is 'List of *'
#   is_disambig: Whether article is a disambiguation page.
#   is_list: Whether article is a list of any type ('List of *', disambig,
#            or in Category or Book namespaces)
class Article(object):
  __slots__ = ['title', 'id', 'coord', 'incoming_links', 'split', 'redir',
               'namespace', 'is_list_of', 'is_disambig', 'is_list']
  def __init__(self, title='unknown', id=None, coord=None, incoming_links=None,
               split='unknown', redir='', namespace='Main', is_list_of=False,
               is_disambig=False, is_list=False):
    self.title = title
    self.id = id
    self.coord = coord
    self.incoming_links = incoming_links
    self.split = split
    self.redir = redir
    self.namespace = namespace
    self.is_list_of = is_list_of
    self.is_disambig = is_disambig
    self.is_list = is_list

  def __str__(self):
    coordstr = " at %s" % self.coord if self.coord else ""
    redirstr = ", redirect to %s" % self.redir if self.redir else ""
    return '%s(%s)%s%s' % (self.title, self.id, coordstr, redirstr)

  # Output row of an article-data file, normal format.  'outfields' is
  # a list of the fields to output, and 'outfield_types' is a list of
  # corresponding types, determined by a call to get_output_field_types().
  def output_row(self, outfile, outfields, outfield_types):
    fieldvals = [t(getattr(self, f)) for f,t in zip(outfields, outfield_types)]
    uniprint('\t'.join(fieldvals), outfile=outfile)

def yesno_to_boolean(foo):
  if foo == 'yes': return True
  else:
    if foo != 'no':
      warning("Expected yes or no, saw '%s'" % foo)
    return False

def boolean_to_yesno(foo):
  if foo: return 'yes'
  else: return 'no'

def commaval_to_coord(foo):
  if foo:
    (lat, long) = foo.split(',')
    return Coord(float(lat), float(long))
  return None

def coord_to_commaval(foo):
  if foo:
    return "%s,%s" % (foo.lat, foo.long)
  return ''

def get_int_or_blank(foo):
  if not foo: return None
  else: return int(foo)

def put_int_or_blank(foo):
  if foo == None: return ''
  else: return "%s" % foo

def identity(foo):
  return foo

def tostr(foo):
  return "%s" % foo

known_fields_input = {'id':int, 'title':identity, 'split':identity,
                      'redir':identity, 'namespace':identity,
                      'is_list_of':yesno_to_boolean,
                      'is_disambig':yesno_to_boolean,
                      'is_list':yesno_to_boolean, 'coord':commaval_to_coord,
                      'incoming_links':get_int_or_blank}

known_fields_output = {'id':tostr, 'title':tostr, 'split':tostr,
                       'redir':tostr, 'namespace':tostr,
                       'is_list_of':boolean_to_yesno,
                       'is_disambig':boolean_to_yesno,
                       'is_list':boolean_to_yesno, 'coord':coord_to_commaval,
                       'incoming_links':put_int_or_blank}

combined_article_data_outfields = ['id', 'title', 'split', 'coord',
    'incoming_links', 'redir', 'namespace', 'is_list_of', 'is_disambig',
    'is_list']

def get_field_types(field_table, field_list):
  for f in field_list:
    if f not in field_table:
      warning("Saw unknown field name %s" % f)
  return [field_table.get(f, identity) for f in field_list]

def get_input_field_types(field_list):
  return get_field_types(known_fields_input, field_list)

def get_output_field_types(field_list):
  return get_field_types(known_fields_output, field_list)

# Read in the article data file.  Call PROCESS on each article.
# The type of the article created is given by ARTICLE_TYPE, which defaults
# to Article.  MAXTIME is a value in seconds, which limits the total
# processing time (real time, not CPU time) used for reading in the
# file, for testing purposes.
def read_article_data_file(filename, process, article_type=Article,
                           maxtime=0):
  errprint("Reading article data from %s..." % filename)
  status = StatusMessage('article')

  fi = uchompopen(filename)
  fields = fi.next().split('\t')
  field_types = get_input_field_types(fields)
  for line in fi:
    fieldvals = line.split('\t')
    if len(fieldvals) != len(field_types):
      warning("""Strange record at line #%s, expected %s fields, saw %s fields;
  skipping line=%s""" % (status.num_processed(), len(field_types),
                         len(fieldvals), line))
      continue
    record = dict([(str(f),t(v)) for f,v,t in zip(fields, fieldvals, field_types)])
    art = article_type(**record)
    process(art)
    if status.item_processed(maxtime=maxtime):
      break
  errprint("Finished reading %s articles." % (status.num_processed()))
  output_resource_usage()
  return fields

def write_article_data_file(outfile, outfields, articles):
  field_types = get_output_field_types(outfields)
  uniprint('\t'.join(outfields), outfile=outfile)
  for art in articles:
    art.output_row(outfile, outfields, field_types)
  outfile.close()
