#!/usr/bin/env python

#######
####### fix_redirects.py
#######
####### Copyright (c) 2014 Ben Wing.
#######

# This is a one-off program to fix the redirect fields so that they
# always begin with a capital letter, as is required of article titles in
# Wikipedia.

import sys
import re
from nlputil import *
from process_article_data import *

def add_latlong(beadle_file, article_data_file):
  coord_hash = {}

  def process(art):
    coord_hash[art.title] = "%s,%s" % (art.coord.lat, art.coord.long)

  errprint("Reading from %s..." % article_data_file)
  read_article_data_file(article_data_file, process, maxtime=Opts.max_time_per_stage)
  errprint("Writing to stdout...")

  lineno = 0
  def repl(m):
    loc = m.group(1)
    innards = m.group(2)
    args = {}
    for m in re.finditer(r' ([a-z]+)="([^"]*)"', innards):
      args[m.group(1)] = m.group(2)
    if "latlong" not in args:
      if "place" not in args:
        errprint("Line %s: Found loc decl without place=: <%s%s/>" % (lineno, loc, innards))
      elif args["place"] in coord_hash:
        return '<%s%s autolatlong="%s"/>' % (loc, innards, coord_hash[args["place"]])
      else:
        errprint("Line %s: Unable to find coordinate for %s" % (lineno, args["place"]))
    return "<%s%s/>" % (loc, innards)

  for line in uchompopen(beadle_file):
    lineno += 1
    uniprint(re.sub(r"<((?:part)?loc)( [^>]*?)/?>", repl, line))
  errprint("Done.")

############################################################################
#                                  Main code                               #
############################################################################

class AddLatLongProgram(NLPProgram):
  def populate_options(self, op):
    op.add_option("-b", "--beadle-file",
                  help="""File containing annotated Beadle text.""",
                  metavar="FILE")
    op.add_option("-a", "--article-data-file",
                  help="""File containing info about Wikipedia articles.""",
                  metavar="FILE")

  def handle_arguments(self, opts, op, args):
    global Opts
    Opts = opts
    self.need('article_data_file')
    self.need('beadle_file')

  def implement_main(self, opts, params, args):
    add_latlong(opts.beadle_file, opts.article_data_file)
    
AddLatLongProgram()
