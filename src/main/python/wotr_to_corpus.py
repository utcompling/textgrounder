#!/usr/bin/env python

from nlputil import *
import re
import os
import sys
import json
import random

# Convert War of the Rebellion (WOTR) spans (either true, i.e. as manually
# annotated, or predicted, i.e. using a sequence model based on the manual
# annotations to generate spans for the entire WOTR corpus) to a TextDB
# corpus. The TextDB corpus will contain fields for the date, coordinate,
# unigram counts and text.
#
# If we are using true spans, we need to specify '--spans' (the annotated
# spans from Parse) and '--text' (the raw WOTR text); if we are using
# predicted spans, we need to specify '--predicted-spans'. In addition,
# we either need to specify '--output' (the prefix of the TextDB corpus)
# or '--no-write' (don't write output, useful to get stats on the number of
# spans).

volume_user = {}
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
      print 'File name format should be e.g. "Max Cadwalder-60.txt" for volume 60'
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
      if vol in volume_spans:
        existing_coord_spans = len([x for x in volume_spans[vol] if x[1]])
        new_coord_spans = len([x for x in io_spans if x[1]])
        if existing_coord_spans > new_coord_spans:
          print "Volume %s: Not overwriting existing user %s spans with fewer spans from %s (%s < %s)" % (
              vol, volume_user[vol], user, new_coord_spans,
              existing_coord_spans)
        else:
          print "Volume %s: Overwriting existing user %s spans with spans from %s because more of them (%s > %s)" % (
              vol, volume_user[vol], user, new_coord_spans,
              existing_coord_spans)
          volume_user[vol] = user
          volume_spans[vol] = io_spans
      else:
        volume_user[vol] = user
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
      countsfield = ' '.join(["%s:%s" % (x,y) for x,y in countmap.iteritems()])
      textfield = (text.replace("%", "%25").replace("\t", "%09").
          replace("\n", "%0A").replace("\r", "%0D").replace("\f", "%0C"))
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
        line = ("vol%s.%s\t%s\t%s\t%s\t%s,%s\t%s\t%s" % (
          vol, span, vol, span, date, lat, lon, countsfield, textfield))
        lines.append(line)
      #uniprint("%s\t%s,%s" % (' '.join(words), lat, lon))
    vols_lines.append(lines)
  return vols_lines

def flatten(lol):
  return [item for x in lol for item in x]

# Permute vols_lines; also flatten if '--split-by span' (necessary in
# case we permute by span, so we do this in any case).
def permute_vols_lines(vols_lines):
  random.seed("The quick brown fox jumps over the lazy dog")
  # If permute by span, we need to permute within volumes if split by volume,
  # else flatten and then permute to get permutation across volumes.
  if Opts.permute == "span":
    if Opts.split_by == "volume":
      for lines in vols_lines:
        random.shuffle(lines)
      # Also permute the volumes themselves
      random.shuffle(vols_lines)
    else:
      vols_lines = flatten(vols_lines)
      random.shuffle(vols_lines)
    return vols_lines
  # Otherwise, if permute by volume, permute volumes. Then in any case,
  # flatten in split by span.
  if Opts.permute == "volume":
    random.shuffle(vols_lines)
  if Opts.split_by == "volume":
    return vols_lines
  else:
    return flatten(vols_lines)

# Given an a file, split the lines based on the split fractions, creating
# training, dev and test files.
#def output_split_files(filename, output_prefix,
#    split_fractions, max_split_size, split_names):
#  for line in uchompopen(filename, "r"):
#    uniprint(line, outfile=split_files[split_gen.next()])
#  for file in split_files:
#    file.close()

def run():
  if Opts.predicted_spans:
    read_predicted_spans()
  else:
    read_volume_text()
    read_volume_spans()

  lc_values_str = [x for x in re.split(":", Opts.learning_curve)]
  lc_values = [float(x) for x in lc_values_str]
  last_lc = lc_values[-1]
  lc_fractions = [x/last_lc for x in lc_values]

  if Opts.fractions:
    trainfrac, devfrac, testfrac = re.split(":", Opts.fractions)
    split_fractions = [float(trainfrac), float(devfrac), float(testfrac)]
  else:
    split_fractions = (
        [Opts.training_fraction, Opts.dev_fraction, Opts.test_fraction])
  #split_frac_size = sum(split_fractions)

  if not Opts.no_write:
    outdir = os.path.dirname(Opts.output)
    if outdir and not os.path.exists(outdir):
      os.makedirs(outdir)

  vols_lines = permute_vols_lines(get_lines())
  # Total number of spans
  numspans = (len(vols_lines) if Opts.split_by == "span" else
      sum(len(x) for x in vols_lines))

  for lcval, lcfrac in zip(lc_values_str, lc_fractions):
    split_count = [0, 0, 0]
    split_names = ['training', 'dev', 'test']
    spancount = 0
    if len(lc_values_str) == 1:
      outpref = Opts.output
    else:
      outpref = "%s-%s" % (Opts.output, lcval)
      print "Learning curve prefix: %s" % outpref
    if Opts.no_write:
      split_files = [None, None, None]
    else:
      split_files = [open("%s-%s.data.txt" % (outpref, split), "w")
        for split in split_names]
    split_gen = next_split_set(
        split_fractions,
        [Opts.max_training_size, Opts.max_dev_size, Opts.max_test_size])
    if Opts.split_by == 'span':
      for line in vols_lines:
        split = split_gen.next()
        spancount += 1
        split_count[split] += 1
        if not Opts.no_write:
          uniprint(line, outfile=split_files[split])
        if float(spancount) / numspans >= lcfrac:
          break
    elif Opts.split_by == 'volume':
      volsize = None
      outerbreak = False
      for lines in vols_lines:
        # send() is weird. Sending a value causes the last yield in the generator
        # to return with a value, then the code loops around and eventually
        # executes another yield, whose value is returned by send(). The
        # first time, we have to send None. Because of the way next_split_set()
        # is written, we have to save the length of the volume and send it
        # the next go around.
        split = split_gen.send(volsize)
        volsize = len(lines)
        for line in lines:
          spancount += 1
          split_count[split] += 1
          if not Opts.no_write:
            uniprint(line, outfile=split_files[split])
          if float(spancount) / numspans >= lcfrac:
            outerbreak = True
            break
        if outerbreak:
          break
    if not Opts.no_write:
      for file in split_files:
        file.close()
    for split in xrange(3):
      print "count for %s: %s" % (split_names[split], split_count[split])
    print "total count: %s / %s" % (spancount, numspans)
    if not Opts.no_write:
      for split in split_names:
        outschemafile = open("%s-%s.schema.txt" % (outpref, split), "w")
        print >>outschemafile, "title\tvol\tspan\tdate\tcoord\tunigram-counts\ttext"
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
    op.add_option("--no-write",
        help="""Don't write anything. Useful to get counts and such.""",
        action="store_true")
    op.add_option("--split-by", default="span",
        choices=["span", "volume"],
        help="""How to split the spans when constructing the training/dev/test
splits (span, volume), default %default.""")
    op.add_option("--fractions",
        help="""Relative fraction of training/dev/test splits, as three
numbers separated by colons, e.g. '80:10:10'. The absolute amounts don't
matter, only the relative ratios.""")
    op.add_option("--learning-curve", default="100",
        help="""Output learning curves by relative fraction. The values should
be floating-point numbers separated by colons and are taken with respect to
the last value, which should represent the entire collection (e.g. use
'25:50:75:100' to get four separate corpora that represent, respectively, 25%,
50%, 75% and 100% of the full training set). The names of the corpora involve
suffixes to the prefix specified in '--output'.""")
    op.add_option("--permute", default="no",
        choices=["no", "span", "volume"],
        help="""Permute the spans before processing them, either at the level
of individual spans (span), entire volumes (volume), or no permutation (no).
Default is %default. When '--split-by volume', permutation by span only
permutes within a given volume (as well as permuting the volumes themselves).
Permutation seeds the random number generator to a specific value at the
beginning so it is repeatable.""")
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
    if not Opts.no_write:
      self.need('output')

  def implement_main(self, opts, params, args):
    run()

WOTRToCorpus()
