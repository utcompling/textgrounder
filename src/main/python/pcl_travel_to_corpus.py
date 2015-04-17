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

def get_lines(infile, date):
  filetext = open(infile).read().replace('&mdash;', ' - ')
  xmldoc = minidom.parseString(filetext)

  retval = []
  paras = xmldoc.getElementsByTagName('p')
  paraid = 0
  for para in paras:
    text = getText(para.childNodes)
    paraid += 1
    words = [word.replace("%", "%25").replace(":", "%3A") for word in
        split_text_into_words(text, ignore_punc=True) if word != "-"]
    countmap = intdict()
    for word in words:
      countmap[word.lower()] += 1
    textfield = ' '.join(["%s:%s" % (x,y) for x,y in countmap.iteritems()])
    line = ("%s.%s\t%s\t%s\t%s\t%s\t%s" % (infile, paraid, infile, paraid, "0.0,0.0", date, textfield))
    retval.append(line)

  return retval

def get_date(datesfile, infile):
  for line in open(datesfile):
    line = line.strip()
    date, filename = re.split(" ", line)
    if filename == infile:
      return date
  print "Can't find date for %s" % infile
  return "unknown"

def run(opts):
  outfile = open("%s-%s.data.txt" % (opts.output, "dev"), "w")
  date = get_date(opts.dates, opts.input)
  for line in get_lines(opts.input, date):
    uniprint(line, outfile=outfile)
  outfile.close()
  outschemafile = open("%s-%s.schema.txt" % (opts.output, "dev"), "w")
  print >>outschemafile, "title\tbook\tid\tcoord\tdate\tunigram-counts"
  print >>outschemafile, "corpus-type\tgeneric"
  print >>outschemafile, "split\t%s" % "dev"
  outschemafile.close()

class PCLTravelToCorpus(NLPProgram):
  def populate_options(self, op):
    op.add_option("--dates",
        help="""File listing dates for input files (with one file per line,
in the format of a date, a space, and the file name).""",
        metavar="FILE")
    op.add_option("-i", "--input",
        help="""Input file (XML format).""",
        metavar="FILE")
    op.add_option("-o", "--output",
        help="""Output prefix for TextDB corpora.""",
        metavar="FILE")

  def handle_arguments(self, opts, op, args):
    global Opts
    Opts = opts
    self.need('input')
    self.need('dates')
    self.need('output')

  def implement_main(self, opts, params, args):
    run(opts)

PCLTravelToCorpus()
