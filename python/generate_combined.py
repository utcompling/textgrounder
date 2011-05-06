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

def read_incoming_link_info(filename, articles_hash):
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
        art.incoming_links = int(links)
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

def output_combined_article_data(filename, coords_file, links_file):
  coords_hash = read_coordinates_file(coords_file)
  articles_hash = {}
  articles_seen = []

  def process(art):
    if art.namespace != 'Main':
      return
    coord = coords_hash.get(art.title, None)
    if coord:
      art.coord = coord
    elif art.redir and capfirst(art.redir) in coords_hash:
      pass
    else:
      return
    articles_hash[art.title] = art
    articles_seen.append(art)
  read_article_data_file(filename, process, maxtime=Opts.max_time_per_stage)

  read_incoming_link_info(links_file, articles_hash)

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

  def handle_arguments(self, opts, op, args):
    global Opts
    Opts = opts
    self.need('article_data_file')
    self.need('coords_file')
    self.need('links_file')

  def implement_main(self, opts, params, args):
    output_combined_article_data(opts.article_data_file, opts.coords_file,
                                 opts.links_file)
    
GenerateCombinedProgram()
