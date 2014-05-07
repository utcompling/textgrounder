#!/usr/bin/env python

#######
####### generate_combined.py
#######
####### Copyright (c) 2010 Ben Wing.
#######

# Essentially does an SQL JOIN on the --article-data-file, --coords-file
# and --links-file, keeping only the article records with a coordinate and
# attaching the coordinate.

import sys
from nlputil import *
from process_article_data import *

# Read incoming-link info from FILENAME and add to the articles in
# ARTICLES_HASH (a mapping from titles to Article objects), incorporating
# links from redirect articles in REDIR_ARTICLES_HASH into the redirected-to
# article.
def read_incoming_link_info(filename, articles_hash, redir_articles_hash):
  errprint("Reading incoming link info from %s..." % filename)
  status = StatusMessage('article')

  for line in uchompopen(filename):
    if rematch('------------------ Count of incoming links: ------------',
               line): continue
    elif rematch('==========================================', line):
      return
    else:
      assert rematch('(.*) = ([0-9]+)$', line)
      title = m_[1]
      links = int(m_[2])
      title = capfirst(title)
      art = articles_hash.get(title, None)
      if art:
        if art.incoming_links is None:
          art.incoming_links = 0
        art.incoming_links += links
      art = redir_articles_hash.get(title, None)
      if art:
        artto_title = capfirst(art.redir)
        artto = articles_hash.get(artto_title, None)
        if artto:
          if artto.incoming_links is None:
            artto.incoming_links = 0
          artto.incoming_links += links
        else:
          errprint("Found coordinates but no article for redirected-to article %s" % artto_title)
    if status.item_processed(maxtime=Opts.max_time_per_stage):
      break

# Parse the result of a previous run of --only-coords or coords-counts for
# articles with coordinates.
def read_coordinates_file(filename):
  errprint("Reading article coordinates from %s..." % filename)
  status = StatusMessage('article')
  coords_hash = {}
  for line in uchompopen(filename):
    if rematch('Article title: (.*)$', line):
      title = m_[1]
    elif rematch('Article coordinates: (.*),(.*)$', line):
      coords_hash[title] = Coord(safe_float(m_[1]), safe_float(m_[2]))
      if status.item_processed(maxtime=Opts.max_time_per_stage):
        break
  return coords_hash

# Given an article metadata file, a file listing article coordinates and
# a file listing article incoming links (computed in three separate passes
# over the dump file), produce a new article metadata file incorporating
# the coordinates and incoming-link count, limited to those articles with
# coordinates.
def output_combined_article_data(filename, coords_file, links_file,
    split_fractions, max_split_size, split_names):
  coords_hash = read_coordinates_file(coords_file)
  # Mapping from non-redir article titles to Article objects
  articles_hash = {}
  # Mapping from redir article titles to Article objects
  redir_articles_hash = {}
  articles_seen = []

  def process(art):
    if art.namespace != 'Main':
      return
    coord = coords_hash.get(art.title, None)
    if coord:
      art.coord = coord
    if art.redir and capfirst(art.redir) in coords_hash:
      redir_articles_hash[art.title] = art
    elif coord:
      articles_hash[art.title] = art
      articles_seen.append(art)
  read_article_data_file(filename, process, maxtime=Opts.max_time_per_stage)

  read_incoming_link_info(links_file, articles_hash, redir_articles_hash)

  split_gen = next_split_set(split_fractions, max_split_size)
  for art in articles_seen:
    art.split = split_names[split_gen.next()]

  errprint("Writing combined data to stdout ...")
  write_article_data_file(sys.stdout,
    outfields = combined_article_data_outfields,
    articles = articles_seen)
  errprint("Done.")

############################################################################
#                                  Main code                               #
############################################################################

class GenerateCombinedProgram(NLPProgram):
  def populate_options(self, op):
    op.add_option("-l", "--links-file",
                  help="""File containing incoming link information for
Wikipedia articles. Output by processwiki.py --find-links.""",
                  metavar="FILE")
    op.add_option("-a", "--article-data-file",
                  help="""File containing info about Wikipedia articles.""",
                  metavar="FILE")
    op.add_option("-c", "--coords-file",
                  help="""File containing output from a prior run of
--coords-counts or --only-coords, listing all the articles with associated
coordinates.  May be filtered only for articles and coordinates.""",
                  metavar="FILE")
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
    op.add_option("--max-dev-size", type='int', default=10000,
                  help="""Maximum number of articles to use for dev set.
  A value of 0 means no maximum. Default %default.""",
                  metavar="SIZE")
    op.add_option("--max-test-size", type='int', default=10000,
                  help="""Maximum number of articles to use for test set.
  A value of 0 means no maximum. Default %default.""",
                  metavar="SIZE")

  def handle_arguments(self, opts, op, args):
    global Opts
    Opts = opts
    self.need('article_data_file')
    self.need('coords_file')
    self.need('links_file')

  def implement_main(self, opts, params, args):
    output_combined_article_data(opts.article_data_file, opts.coords_file,
                                 opts.links_file,
                                 [opts.training_fraction, opts.dev_fraction,
                                   opts.test_fraction],
                                 [opts.max_training_size, opts.max_dev_size,
                                   opts.max_test_size],
                                 ['training', 'dev', 'test'])

GenerateCombinedProgram()
