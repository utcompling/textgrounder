#!/usr/bin/env python

#######
####### convert_to_new_article_format.py
#######
####### Copyright (c) 2011 Ben Wing.
#######

# A one-off program to convert article-data files to the new order, which
# puts the most important article fields (id, name, split, coords) before
# other fields that may be specific to the type of article (e.g. Wikipedia
# article, Twitter feed, tweet, etc.).

import sys
from nlputil import *
from process_article_data import *

def output_combined_article_data(filename):
  arts_seen = []
  def note_article(art):
    arts_seen.append(art)
  # Note that the article data file indicates the field names at the
  # beginning.
  read_article_data_file(filename, note_article)
  errprint("Writing combined data to stdout ...")
  write_article_data_file(sys.stdout,
    outfields = combined_article_data_outfields,
    articles = arts_seen)
  errprint("Done.")

for filename in sys.argv[1:]:
  output_combined_article_data(filename)
