#!/usr/bin/env python

#######
####### split_nfold.py
#######
####### Copyright (c) 2016 Ben Wing.
#######

# Split a file into N folds.
import sys
from nlputil import *

# Given an a file, split the lines based on the split fractions, creating
# training, dev and test files.
def output_split_files(filename, output_prefix, nfolds):
  split_fractions = [1]*nfolds
  split_lines = []
  for i in xrange(nfolds):
    split_lines.append([])
  split_gen = next_split_set(split_fractions)
  for line in uchompopen(filename, "r"):
    split_lines[split_gen.next()].append(line)
  for fold in xrange(nfolds):
    splits = range(nfolds)
    del splits[fold]
    print "Splits in training: %s" % splits
    with open("%s.train.%s" % (output_prefix, fold), "w") as training:
      for split in splits:
        for line in split_lines[split]:
          uniprint(line, outfile=training)
    with open("%s.test.%s" % (output_prefix, fold), "w") as test:
      for line in split_lines[fold]:
        uniprint(line, outfile=test)

############################################################################
#                                  Main code                               #
############################################################################

class SplitNFold(NLPProgram):
  def populate_options(self, op):
    op.add_option("--nfolds", type='int', default=10,
                  help="""Number of folds to create. Default %default.""",
                  metavar="FRACTION")
    op.add_option("-f", "--file",
                  help="""File containing lines to split.""",
                  metavar="FILE")
    op.add_option("-o", "--output-prefix",
                  help="""Prefix of files to output. Files are named
PREFIX.train.0, PREFIX.test.0, PREFIX.train.1, PREFIX.test.1, etc.""",
                  metavar="FILE")

  def handle_arguments(self, opts, op, args):
    global Opts
    Opts = opts
    self.need('file')
    self.need('output_prefix')

  def implement_main(self, opts, params, args):
    output_split_files(opts.file, opts.output_prefix, opts.nfolds)

SplitNFold()
