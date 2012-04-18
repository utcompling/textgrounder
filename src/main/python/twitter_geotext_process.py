#!/usr/bin/env python

#######
####### twitter_geotext_process.py
#######
####### Copyright (c) 2010 Ben Wing.
#######

import sys, re
import math
from optparse import OptionParser
from nlputil import *

############################################################################
#                              Documentation                               #
############################################################################

# This program reads in data from the Geotext corpus provided by
# Eisenstein et al., and converts it into the format used for the
# Wikigrounder experiments.

############################################################################
#                                   Code                                   #
############################################################################

# Debug level; if non-zero, output lots of extra information about how
# things are progressing.  If > 1, even more info.
debug = 0

# If true, print out warnings about strangely formatted input
show_warnings = True

#######################################################################
###                        Utility functions                        ###
#######################################################################

def uniprint(text):
  '''Print Unicode text in UTF-8, so it can be output without errors'''
  if type(text) is unicode:
    print text.encode("utf-8")
  else:
    print text

def warning(text):
  '''Output a warning, formatting into UTF-8 as necessary'''
  if show_warnings:
    uniprint("Warning: %s" % text)

#######################################################################
#                            Process files                            #
#######################################################################

vocab_id_to_token = {}
user_id_to_token = {}
user_token_to_id = {}

# Process vocabulary file
def read_vocab(filename):
  id = 0
  for line in open(filename):
    id += 1
    word = line.strip().split('\t')[0]
    #errprint("%s: %s" % (id, word))
    vocab_id_to_token["%s" % id] = word

# Process user file
def read_user_info(filename):
  id = 666000000
  for line in open(filename):
    id += 1
    userid = line.strip().split('\t')[0]
    #errprint("%s: %s" % (id, userid))
    user_id_to_token[id] = userid
    if userid in user_token_to_id:
      errprint("User %s seen twice!  Current ID=%s, former=%s" % (
        userid, id, user_token_to_id[userid]))
    user_token_to_id[userid] = id

# Process a file of data in "LDA" format
def process_lda_file(split, filename, userid_filename, artdat_file,
    counts_file):

  userids = []
  for line in open(userid_filename):
    userid, lat, long = line.strip().split('\t')
    usernum = user_token_to_id[userid]
    userids.append(usernum)
    print >>artdat_file, ("%s\t%s\t%s\t\tMain\tno\tno\tno\t%s,%s\t1" %
        (usernum, userid, split, lat, long))

  userind = 0
  for line in open(filename):
    line = line.strip()
    args = line.split()
    numtypes = args[0]
    lat = args[1]
    long = args[2]
    usernum = userids[userind]
    userind += 1
    print >>counts_file, "Article title: %s" % user_id_to_token[usernum]
    print >>counts_file, "Article ID: %s" % usernum
    for arg in args[3:]:
      wordid, count = arg.split(':')
      print >>counts_file, "%s = %s" % (vocab_id_to_token[wordid], count)

#######################################################################
#                                Main code                            #
#######################################################################

def main():
  op = OptionParser(usage="%prog [options] input_dir")
  op.add_option("-i", "--input-dir", metavar="DIR",
                help="Input dir with Geotext preprocessed files.")
  op.add_option("-o", "--output-dir", metavar="DIR",
                help="""Dir to output processed files.""")
  op.add_option("-p", "--prefix", default="geotext-twitter-",
                help="""Prefix to use for outputted files.""")
  op.add_option("-d", "--debug", metavar="LEVEL",
                help="Output debug info at given level")

  opts, args = op.parse_args()

  global debug
  if opts.debug:
    debug = int(opts.debug)
 
  if not opts.input_dir:
    op.error("Must specify input dir using -i or --input-dir")
  if not opts.output_dir:
    op.error("Must specify output dir using -i or --output-dir")
  
  prefix = "%s/%s" % (opts.output_dir, opts.prefix)
  artdat_file = open("%scombined-document-data.txt" % prefix, "w")
  print >>artdat_file, "id\ttitle\tsplit\tredir\tnamespace\tis_list_of\tis_disambig\tis_list\tcoord\tincoming_links"

  counts_file = open("%scounts-only-coord-documents.txt" % prefix, "w")

  train_file = "%s/%s" % (opts.input_dir, "train.dat")
  dev_file = "%s/%s" % (opts.input_dir, "dev.dat")
  test_file = "%s/%s" % (opts.input_dir, "test.dat")
  vocab_file = "%s/%s" % (opts.input_dir, "vocab_wc_dc")
  userid_train_file = "%s/%s" % (opts.input_dir, "user_info.train")
  userid_dev_file = "%s/%s" % (opts.input_dir, "user_info.dev")
  userid_test_file = "%s/%s" % (opts.input_dir, "user_info.test")
  userid_file = "%s/%s" % (opts.input_dir, "user_info")

  read_vocab(vocab_file)
  read_user_info(userid_file)
  process_lda_file('test', test_file, userid_test_file, artdat_file, counts_file)
  process_lda_file('training', train_file, userid_train_file, artdat_file, counts_file)
  process_lda_file('dev', dev_file, userid_dev_file, artdat_file, counts_file)
  artdat_file.close()
  counts_file.close()

main()

