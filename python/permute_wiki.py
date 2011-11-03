#!/usr/bin/env python

#######
####### permute_wiki.py
#######
####### Copyright (c) 2010 Ben Wing.
#######

from __future__ import with_statement

import random
import re
import sys
from nlputil import *
from process_article_data import *

"""
We want to randomly permute the articles and then reorder the articles
in the dump file accordingly.  A simple algorithm would be to load the
entire dump file into memory as huge list, permute the list randomly,
then write the results.  However, this might easily exceed the memory
of the computer.  So instead, we split the task into chunks, proceeding
as follows, keeping in mind that we have the list of article names
available in a separate file:

1. The dump consists of a prolog, a bunch of articles, and an epilog.
2. (The 'permute' step:) Take the full list of articles, permute randomly
   and output again.
3. (The 'split' step:) Split the dump into pieces, of perhaps a few GB each,
   the idea being that we can sort each piece separately and concatenate the
   results. (This is the 'split' step.) The idea is that we first split the
   permuted list of articles into some number of pieces (default 8), and
   create a mapping listing which split each article goes in; then we create
   a file for each split; then we read through the dump file, and each time we
   find an article, we look up its split and write it to the corresponding
   split file.  We also write the prolog and epilog into separate files.
   Note that in this step we have effectively done a rough sort of the
   articles by split, preserving the original order within each split.
4. (The 'sort' step:) Sort each split.  For each split, we read the permuted
   article list into memory to get the proper order, then we read the entire
   split into memory and output the articles in the order indicated in the
   article list.
5. Concatenate the results.

Note that this might be a perfect task for Hadoop; it automatically does
the splitting, sorting and merging.

"""

all_articles = []

def read_article_data(filename):
  def process(art):
    all_articles.append(art)

  infields = read_article_data_file(filename, process,
                                    maxtime=Opts.max_time_per_stage)
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
    if status.item_processed(maxtime=Opts.max_time_per_stage):
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
