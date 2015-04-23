#!/usr/bin/env python

from nlputil import *
import re
import os
import sys
import json

volume_spans = {}
volume_text = {}
volume_predictions = {}

def read_volume_text():
  for fn in os.listdir(Opts.text):
    m = re.match(r"(?:.*/)?0*([1-9][0-9]*)\.txt", fn)
    if not m:
      print "Unable to parse filename: %s" % fn
      print "File name format should be e.g. 001.txt for volume 1"
    else:
      volume_text[m.group(1)] = open(os.path.join(Opts.text, fn)).read()

remonths = "January|February|March|April|May|June|July|August|September|October|November|December"
redate = r"(?i)(%s)\]? *\[?([0-9]+)\??\]?[,.]* *\[?(186[0-9])" % remonths
redate_strict = redate + "[.-]"

def find_date(text):
  mstrict = re.search(redate_strict, text)
  m = re.search(redate, text)
  datestrict = mstrict and "%s %s, %s" % (
      mstrict.group(1).lower(), mstrict.group(2), mstrict.group(3)) or None
  datelax = m and "%s %s, %s" % (m.group(1).lower(), m.group(2), m.group(3)) or None
  if Opts.verbose and datelax and datestrict and datelax != datestrict:
    print "Mismatch: strict=%s, lax=%s" % (datestrict, datelax)
    print "--------- TEXT ----------"
    print text.strip()
    print "--------- END ----------"
  date = datestrict or datelax
  if date:
    if Opts.verbose:
      print date
    return date
  else:
    if Opts.verbose:
      print "Unable to find date:"
      #print "--------- TEXT ----------"
      #print text.strip()
      #print "--------- END ----------"
    return None

def centroid(latlons):
  latsum = 0.0
  lonsum = 0.0
  npoints = 0
  for latlon in latlons:
    if latlon:
      lat, lon = latlon
      latsum += lat
      lonsum += lon
      npoints += 1
  if npoints:
    return [latsum / npoints, lonsum / npoints]
  else:
    return None

def parse_coord(coord):
  latlons = re.split(",", coord)
  if len(latlons) != 2:
    print "Wrong number of coordinates in coord spec: %s" % coord
    return None
  else:
    lat, lon = latlons
    return float(lat.strip()), float(lon.strip())

def parse_geom(geom):
  splitgeoms = re.split("@@", geom)
  geompoints = []
  for g in splitgeoms:
    if not g:
      continue
    try:
      js = json.loads(g)
    except:
      print "Error parsing JSON of <%s>, splitgeoms=<%s>" % (g, splitgeoms)
      raise
    ty = js['type']
    coords = js['coordinates']
    if ty == "Point":
      geompoints.append(coords)
    elif ty == "MultiPolygon":
      if len(coords) > 1:
        print "Don't know what to do with top-level multiple coords: %s" % coords
      coords = coords[0]
      centroids = []
      for polygon in coords:
        centroids.append(centroid(polygon))
      geompoints.append(centroid(centroids))
    else:
      print "Unrecognized type %s in coord spec %s" % (ty, g)
  cent = centroid(geompoints)
  if cent:
    return [cent[1], cent[0]]
  else:
    return None

def read_volume_spans():
  for spanfile in os.listdir(Opts.spans):
    m = re.match(r"(.*)-([0-9]+)\.txt$", spanfile)
    if not m:
      print "Unable to parse span filename %s" % spanfile
      print 'File name format should be e.g. "Max Caldwalder-60.txt" for volume 60'
    else:
      user = m.group(1)
      vol = m.group(2)
      print "Parsing spans for user %s, volume %s" % (user, vol)
      spantext = open(Opts.spans + "/" + spanfile).read()
      splitspans = re.split(r"\|", spantext)
      spans = []
      for span in splitspans:
        spanparts = re.split(r"\$", span)
        spanbegin = int(spanparts[1])
        spanend = int(spanparts[2])
        spancoord = parse_geom(spanparts[3])
        spans.append([spanbegin, spanend, spancoord])
      io_spans = []
      voltext = volume_text[vol]
      ind = 0
      for beg, end, coord in spans:
        inside_text = voltext[beg:end].strip()
        if inside_text:
          io_spans.append(["%s-%s" % (beg, end), coord, inside_text])
      volume_spans[vol] = io_spans

def read_predicted_spans():
  for spanfile in os.listdir(Opts.predicted_spans):
    m = re.match(r"([0-9]+)\.", spanfile)
    if not m:
      print "Unable to parse span filename %s" % spanfile
      print 'File name format should be e.g. "60.predicted-spans" for volume 60'
    else:
      vol = m.group(1)
      print "Parsing predicted spans for volume %s" % vol
      filetext = open(Opts.predicted_spans + "/" + spanfile).read()
      spanno = 1
      spans = []
      for span in re.finditer(r'^-----+ BEGIN SPAN -----+$(.*?)^-----+ END SPAN -----+$', filetext, re.M | re.S):
        spantext = span.group(1)
        if not Opts.filter_regex or re.search(Opts.filter_regex, spantext):
          spans.append(["%s" % spanno, None, spantext])
        spanno += 1
      volume_spans[vol] = spans

def get_lines():
  vols = sorted([vol for vol in volume_spans], key=lambda x:int(x))
  vols_lines = []
  for vol in vols:
    print "Processing volume %s" % vol
    lines = []
    last_date = None
    for span, coord, text in volume_spans[vol]:
      date = find_date(text)
      if date:
        last_date = date
      if not date:
        date = last_date
      if not date:
        #print "No date"
        date = ""
      words = [word.replace("%", "%25").replace(":", "%3A") for word in
          split_text_into_words(text, ignore_punc=True) if word != "-"]
      countmap = intdict()
      for word in words:
        countmap[word] += 1
      textfield = ' '.join(["%s:%s" % (x,y) for x,y in countmap.iteritems()])
      include = False
      lat = None
      lon = None
      if coord:
        lat, lon = coord
      if lat is not None and lon is not None:
        if not Opts.suppress_coord_paras:
          include = True
      else:
        if Opts.include_non_coord_paras:
          include = True
          lat = 0.0
          lon = 0.0
      if include:
        line = ("vol%s.%s\t%s\t%s\t%s\t%s,%s\t%s" % (
          vol, span, vol, span, date, lat, lon, textfield))
        lines.append(line)
      #uniprint("%s\t%s,%s" % (' '.join(words), lat, lon))
    vols_lines.append(lines)
  return vols_lines

# Given an a file, split the lines based on the split fractions, creating
# training, dev and test files.
def output_split_files(filename, output_prefix,
    split_fractions, max_split_size, split_names):
  for line in uchompopen(filename, "r"):
    uniprint(line, outfile=split_files[split_gen.next()])
  for file in split_files:
    file.close()

def run(opts):
  split_names = ['training', 'dev', 'test']
  split_count = [0, 0, 0]
  split_files = [open("%s-%s.data.txt" % (opts.output, split), "w")
    for split in split_names]
  split_fractions = (
      [opts.training_fraction, opts.dev_fraction, opts.test_fraction])
  split_frac_size = sum(split_fractions)
  split_gen = next_split_set(
      split_fractions,
      [opts.max_training_size, opts.max_dev_size, opts.max_test_size])
  if Opts.predicted_spans:
    read_predicted_spans()
  else:
    read_volume_text()
    read_volume_spans()

  vols = sorted([vol for vol in volume_spans], key=lambda x:int(x))
  vols_lines = get_lines()
  if opts.split_by == 'line':
    for lines in vols_lines:
      for line in lines:
        split = split_gen.next()
        split_count[split] += 1
        uniprint(line, outfile=split_files[split])
  elif opts.split_by == 'volume':
    volsize = None
    for lines in vols_lines:
      # send() is weird. Sending a value causes the last yield in the generator
      # to return with a value, then the code loops around and eventually
      # executes another yield, whose value is returned by send(). The
      # first time, we have to send None. Because of the way next_split_set()
      # is written, we have to save the length of the chapter and send it
      # the next go around.
      split = split_gen.send(chaptersize)
      volsize = len(lines)
      for line in lines:
        split_count[split] += 1
        uniprint(line, outfile=split_files[split])
  for file in split_files:
    file.close()
  for split in xrange(3):
    print "count for %s: %s" % (split_names[split], split_count[split])
  for split in split_names:
    outschemafile = open("%s-%s.schema.txt" % (opts.output, split), "w")
    print >>outschemafile, "title\tvol\tspan\tdate\tcoord\tunigram-counts"
    print >>outschemafile, "corpus-type\tgeneric"
    print >>outschemafile, "split\t%s" % split
    outschemafile.close()

class WOTRToCorpus(NLPProgram):
  def populate_options(self, op):
    op.add_option("--verbose",
        help="""Debugging info.""",
        action="store_true")
    op.add_option("--spans",
        help="""Directory containing spans.""",
        metavar="FILE")
    op.add_option("--text",
        help="""Directory containing text files.""",
        metavar="FILE")
    op.add_option("--predicted-spans",
        help="""Directory containing predicted spans.""",
        metavar="FILE")
    op.add_option('--filter-regex',
        help="Regex to filter spans.")
    op.add_option("-o", "--output",
        help="""Output prefix for TextDB corpora.""",
        metavar="FILE")
    op.add_option("--include-non-coord-paras",
        help="""Include paragraphs without coordinates, supplying 0,0 as the
coordinate.""",
        action="store_true")
    op.add_option("--suppress-coord-paras",
        help="""Suppress paragraphs with coordinates.""",
        action="store_true")
    op.add_option("--split-by", default="line",
        choices=["line", "volume"],
        help="""How to split the paragraphs (line, volume).""")
    op.add_option("--training-fraction", type='float', default=80,
        help="""Fraction of total articles to use for training.
The absolute amount doesn't matter, only the value relative to the test
and dev fractions, as the values are normalized.  Default %default.""",
        metavar="FRACTION")
    op.add_option("--dev-fraction", type='float', default=10,
        help="""Fraction of total articles to use for dev set.
The absolute amount doesn't matter, only the value relative to the training
and test fractions, as the values are normalized.  Default %default.""",
        metavar="FRACTION")
    op.add_option("--test-fraction", type='float', default=10,
        help="""Fraction of total articles to use for test set.
The absolute amount doesn't matter, only the value relative to the training
and dev fractions, as the values are normalized.  Default %default.""",
        metavar="FRACTION")
    op.add_option("--max-training-size", type='int', default=0,
        help="""Maximum number of articles to use for training.
A value of 0 means no maximum. Default %default.""",
        metavar="SIZE")
    op.add_option("--max-dev-size", type='int', default=0,
        help="""Maximum number of articles to use for dev set.
A value of 0 means no maximum. Default %default.""",
        metavar="SIZE")
    op.add_option("--max-test-size", type='int', default=0,
        help="""Maximum number of articles to use for test set.
A value of 0 means no maximum. Default %default.""",
        metavar="SIZE")

  def handle_arguments(self, opts, op, args):
    global Opts
    Opts = opts
    if not Opts.predicted_spans:
      self.need('spans')
      self.need('text')
    self.need('output')

  def implement_main(self, opts, params, args):
    run(opts)

WOTRToCorpus()
