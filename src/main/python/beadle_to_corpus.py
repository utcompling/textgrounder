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

def run(infile, outputprefix):
  filetext = open(infile).read().replace('&mdash;', ' - ')
  outdatafile = open(outputprefix + ".data.txt", "w")
  xmldoc = minidom.parseString(filetext)

  paras = xmldoc.getElementsByTagName('p')
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
      uniprint("%s\tbeadle.%s\t%s,%s\t%s" % (paraid, paraid, lat, lon, textfield),
          outfile=outdatafile)
    #uniprint("%s\t%s,%s" % (' '.join(words), lat, lon))
  outdatafile.close()
  outschemafile = open(outputprefix + ".schema.txt", "w")
  print >>outschemafile, "id\ttitle\tcoord\tunigram-counts"
  print >>outschemafile, "corpus-type\tgeneric"
  print >>outschemafile, "split\tdev"
  outschemafile.close()

class BeadleToCorpus(NLPProgram):
  def populate_options(self, op):
    op.add_option("-i", "--input",
        help="""Input Beadle file (XML format).""",
        metavar="FILE")
    op.add_option("-o", "--output",
        help="""Output prefix for TextDB corpus.""",
        metavar="FILE")
    op.add_option("--include-non-coord-paras",
        help="""Include paragraphs without coordinates, supplying 0,0 as the
coordinate.""",
        action="store_true")
    op.add_option("--suppress-coord-paras",
        help="""Suppress paragraphs with coordinates.""",
        action="store_true")

  def handle_arguments(self, opts, op, args):
    global Opts
    Opts = opts
    self.need('input')
    self.need('output')

  def implement_main(self, opts, params, args):
    run(opts.input, opts.output)

BeadleToCorpus()
