#!/usr/bin/python

#######
####### permute_wiki.py
#######
####### Copyright (c) 2010 Ben Wing.
#######

from __future__ import with_statement

import random
import re
import sys
from textutil import *
from process_article_data import *

"""
We want to randomly permute the articles and then reorder the articles
in the dump file accordingly.

1. Take the full list of articles, permute randomly and output again.
2. The dump consists of a prologue, a bunch of articles, and an epilogue.
   The article boundaries can be identified by regexps.
3. Split the dump into pieces, of size 8GB or so each.  Find the exact
   boundary by seeking forward to the 8GB or so boundary and then looking
   forward till we find an article boundary; this then becomes a boundary
   between splits.  Also split off the prologue and epilogue into separate
   files.
4. For each split, read the entire split into memory and make a table
   mapping article ID to a tuple of (offset, length).  Also read the entire
   permuted article list in memory (only the ID's are needed).
5. Go through the article list in order; for each ID present in the current
   split, output that article.  The result will be an output file for
   each split, with the articles in that split in order.
6. Merge the separate splits.

Note that this might be a perfect task for Hadoop; it automatically does
the splitting, sorting and merging.

"""

all_articles = []

def read_article_data(filename):
  def process(art):
    all_articles.append(art)

  infields = read_article_data_file(filename, process,
                                    max_time_per_stage=Opts.max_time_per_stage)
  return infields

def write_permutation(infields):
  random.shuffle(all_articles)
  errprint("Writing combined data to stdout ...")
  write_article_data_file(sys.stdout, outfields=infields,
                          articles=all_articles)
  errprint("Done.")


def break_up_xml_dump(infile):
  prolog = ''
  inpage = False
  for x in infile:
    if re.match('.*<page>', x):
      thispage = [x]
      inpage = True
      break
    else:
      prolog += x

  if prolog:
    yield ('prolog', prolog)

  thisnonpage = ''
  for x in infile:
    if inpage:
      if re.match('.*</page>', x):
        inpage = False
        thispage.append(x)
        thisnonpage = ''
        yield ('page', ''.join(thispage))
      else:
        thispage.append(x)
    else:
      if re.match('.*<page>', x):
        if thisnonpage:
          yield ('nonpage', thisnonpage)
        thispage = [x]
        inpage = True
      else:
        thisnonpage += x
  if inpage:
    warning("Saw <page> but no </page>")
  if thisnonpage:
    yield ('epilog', thisnonpage)


def get_id_from_page(text):
  m = re.match('(?s).*?<id>(.*?)</id>', text)
  if not m:
    warning("Can't find ID in article; beginning of article text follows:")
    maxlen = min(100, len(text))
    errprint(text[0:maxlen])
    return -1
  id = m.group(1)
  try:
    id = int(id)
  except ValueError:
    print "Exception when parsing %s, assumed non-int" % id
    return -1
  return id


def split_files(infields, split_prefix, num_splits):
  errprint("Generating this split article-table files...")
  splits = {}
  num_arts = len(all_articles)
  splitsize = (num_arts + num_splits - 1) // num_splits
  for i in xrange(num_splits):
    minval = i * splitsize
    maxval = min(num_arts, (i + 1) * splitsize)
    outarts = []
    for j in xrange(minval, maxval):
      art = all_articles[j]
      splits[art.id] = i
      outarts.append(art)
    with open("%s.%s.articles" % (split_prefix, i), 'w') as outfile:
      write_article_data_file(outfile, outfields=infields, articles=outarts)

  # Clear the big array when we're done with it
  del all_articles[:]

  splitfiles = [None]*num_splits
  for i in xrange(num_splits):
    splitfiles[i] = open("%s.%s" % (split_prefix, i), 'w')

  errprint("Splitting the dump....")
  status = StatusMessage("article")
  for (type, text) in break_up_xml_dump(sys.stdin):
    if type == 'prolog':
      with open("%s.prolog" % split_prefix, 'w') as prolog:
        prolog.write(text)
    elif type == 'epilog':
      with open("%s.epilog" % split_prefix, 'w') as epilog:
        epilog.write(text)
    elif type == 'nonpage':
      warning("Saw non-page text %s" % text)
    else:
      id = get_id_from_page(text)
      if id not in splits:
        warning("Can't find article %s in article data file" % id)
      else:
        splitfiles[splits[id]].write(text)
    if status.item_processed() >= Opts.max_time_per_stage:
      errprint("Interrupting processing")
      break

def sort_file():
  all_pages = {}
  for (type, text) in break_up_xml_dump(sys.stdin):
    if type != 'page':
      warning("Shouldn't see type '%s' in split file: %s" % (type, text))
    else:
      id = get_id_from_page(text)
      all_pages[id] = text
  for art in all_articles:
    text = all_pages.get(art.id, None)
    if text is None:
      warning("Didn't see article ID %s in XML file"  % art.id)
    else:
      sys.stdout.write(text)


############################################################################
#                                  Main code                               #
############################################################################

class PermuteWikipediaDumpProgram(NLPProgram):
  def populate_options(self, op):
    op.add_option("-a", "--article-data-file",
                  help="""File containing all the articles.""")
    op.add_option("-s", "--number-of-splits", type='int', default=8,
                  help="""Number of splits.""")
    op.add_option("--split-prefix", help="""Prefix for split files.""")
    op.add_option("-m", "--mode", type='choice',
                  default=None, choices=['permute', 'split', 'sort'],
                  help="""Format of evaluation file(s).  Default '%default'.""")

  def handle_arguments(self, opts, op, args):
    global Opts
    Opts = opts
    self.need('mode')
    self.need('article_data_file')
    if opts.mode == 'split':
      self.need('split_prefix')

  def implement_main(self, opts, params, args):
    infields = read_article_data(opts.article_data_file)
    if opts.mode == 'permute':
      write_permutation(infields)
    elif opts.mode == 'split':
      split_files(infields, opts.split_prefix, opts.number_of_splits)
    elif opts.mode == 'sort':
      sort_file()

PermuteWikipediaDumpProgram()
