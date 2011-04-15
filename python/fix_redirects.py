#!/usr/bin/env python

#######
####### fix_redirects.py
#######
####### Copyright (c) 2010 Ben Wing.
#######

# This is a one-off program to fix the redirect fields so that they
# always begin with a capital letter, as is required of article titles in
# Wikipedia.

import sys
from nlputil import *
from process_article_data import *

def fix_redirects(filename):
  articles_seen = []
  def process(art):
    if art.redir:
      art.redir = capfirst(art.redir)
    articles_seen.append(art)
  errprint("Reading from %s..." % filename)
  fields = read_article_data_file(filename, process,
                                  maxtime=Opts.max_time_per_stage)
  errprint("Writing to stdout...")
  write_article_data_file(sys.stdout, outfields=fields,
                          articles=articles_seen)
  errprint("Done.")

############################################################################
#                                  Main code                               #
############################################################################

class FixRedirectsProgram(NLPProgram):
  def argument_usage(self):
    return "article-data-file"

  def handle_arguments(self, opts, op, args):
    global Opts
    Opts = opts
    if len(args) != 1:
      op.error("Must specify exactly one article-data file as an argument")

  def implement_main(self, opts, params, args):
    fix_redirects(args[0])
    
FixRedirectsProgram()
