#!/usr/bin/env python

import sys, re
from nlputil import *
from xml.dom import minidom

def getText(nodelist):
    rc = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    return ''.join(rc)

def get_lines(infile):
  filetext = open(infile).read().replace('&mdash;', ' - ')
  xmldoc = minidom.parseString(filetext)

  chapters = [x for x in xmldoc.getElementsByTagName('div')
      if x.getAttribute("type") in ["preface", "chapter"]]
  retval = []
  for chapter in chapters:
    chapterlines = []
    heads = chapter.getElementsByTagName('head')
    headtext = ' '.join(getText(head.childNodes) for head in heads)
    if "PREFACE" in headtext:
      chnum = "PREFACE"
    else:
      m = re.search(r"CHAPTET?R\s+([A-Z]+)", headtext)
      if not m:
        errprint("Can't parse chapter number in head text: [[%s]]" % headtext)
        chnum = "unknown"
      else:
        chnum = m.group(1)
    paras = chapter.getElementsByTagName('p')
    for para in paras:
      paraid = para.getAttribute("id") or "unknown"
      text = getText(para.childNodes)
      #errprint("Saw paragraph: %s" % text)
      locs = para.getElementsByTagName('loc')
      if len(locs) > 1:
        errprint("Found multiple locations in paragraph: %s" % locs)
      lat = None
      lon = None
      if len(locs) == 1:
        loc = locs[0]
        latlong = loc.getAttribute("latlong") or loc.getAttribute("autolatlong")
        if latlong:
          m = re.match("^([-0-9.]+),([-0-9.]+)$", latlong)
          if m:
            lat = float(m.group(1))
            lon = float(m.group(2))
            #errprint("Found location (decimal): %s,%s" % (lat, lon))
          else:
            m = re.match("^([-0-9]+)'([-0-9]+)'([-0-9]+)N/([-0-9]+)'([-0-9]+)'([-0-9]+)W$",
                latlong)
            if m:
              lat = float(m.group(1)) + float(m.group(2))/60.0 + float(m.group(3))/3600.0
              lon = -(float(m.group(4)) + float(m.group(5))/60.0 + float(m.group(6))/3600.0)
              #errprint("Found location (DMS): %s,%s" % (lat, lon))
            else:
              errprint("Found unparsable location: %s" % latlong)
      words = [word.replace("%", "%25").replace(":", "%3A") for word in
          split_text_into_words(text, ignore_punc=True) if word != "-"]
      countmap = intdict()
      for word in words:
        countmap[word] += 1
      textfield = ' '.join(["%s:%s" % (x,y) for x,y in countmap.iteritems()])
      include = False
      if lat is not None and lon is not None:
        if not Opts.suppress_coord_paras:
          include = True
      else:
        if Opts.include_non_coord_paras:
          include = True
          lat = 0.0
          lon = 0.0
      if include:
        line = ("%s\tbeadle.%s\t%s\t%s,%s\t%s" % (paraid, paraid, chnum, lat,
          lon, textfield))
        chapterlines.append(line)
      #uniprint("%s\t%s,%s" % (' '.join(words), lat, lon))
    retval.append(chapterlines)

  return retval

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
  chapters_lines = get_lines(opts.input)
  if opts.split_by == 'line':
    for chapterlines in chapters_lines:
      for line in chapterlines:
        split = split_gen.next()
        split_count[split] += 1
        uniprint(line, outfile=split_files[split])
  elif opts.split_by == 'chapter':
    chaptersize = None
    for chapterlines in chapters_lines:
      # send() is weird. Sending a value causes the last yield in the generator
      # to return with a value, then the code loops around and eventually
      # executes another yield, whose value is returned by send(). The
      # first time, we have to send None. Because of the way next_split_set()
      # is written, we have to save the length of the chapter and send it
      # the next go around.
      split = split_gen.send(chaptersize)
      chaptersize = len(chapterlines)
      for line in chapterlines:
        split_count[split] += 1
        uniprint(line, outfile=split_files[split])
  elif opts.split_by == 'book':
    lines = [line for chapterlines in chapters_lines for line in chapterlines]
    numlines = len(lines)
    split_count[0] = (
        int(0.5 + float(opts.training_fraction) * numlines / split_frac_size))
    split_count[1] = (
        int(0.5 + float(opts.dev_fraction) * numlines / split_frac_size))
    split_count[2] = numlines - split_count[0] - split_count[1]
    numlines = 0
    for line in lines:
      numlines += 1
      if numlines <= split_count[0]:
        split = 0
      elif numlines <= split_count[0] + split_count[1]:
        split = 1
      else:
        split = 2
      uniprint(line, outfile=split_files[split])
  for file in split_files:
    file.close()
  for split in xrange(3):
    print "count for %s: %s" % (split_names[split], split_count[split])
  for split in split_names:
    outschemafile = open("%s-%s.schema.txt" % (opts.output, split), "w")
    print >>outschemafile, "id\ttitle\tchapter\tcoord\tunigram-counts"
    print >>outschemafile, "corpus-type\tgeneric"
    print >>outschemafile, "split\t%s" % split
    outschemafile.close()

class BeadleToCorpus(NLPProgram):
  def populate_options(self, op):
    op.add_option("-i", "--input",
        help="""Input Beadle file (XML format).""",
        metavar="FILE")
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
        choices=["line", "chapter", "book"],
        help="""How to split the paragraphs (line, chapter, book).""")
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
    self.need('input')
    self.need('output')

  def implement_main(self, opts, params, args):
    run(opts)

BeadleToCorpus()
