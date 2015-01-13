#!/usr/bin/env python

#######
####### split_lines_devtest.py
#######
####### Copyright (c) 2010 Ben Wing.
#######

import sys
from nlputil import *

# Given an a file, split the lines based on the split fractions, creating
# training, dev and test files.
def output_split_files(filename, output_prefix,
    split_fractions, max_split_size, split_names):
  split_files = [open("%s-%s.data.txt" % (output_prefix, split), "w")
    for split in split_names]
  split_gen = next_split_set(split_fractions, max_split_size)
  for line in uchompopen(filename, "r"):
    uniprint(line, outfile=split_files[split_gen.next()])
  for file in split_files:
    file.close()

############################################################################
#                                  Main code                               #
############################################################################

class SplitLinesDevTest(NLPProgram):
  def populate_options(self, op):
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
    op.add_option("--max-dev-size", type='int', default=0,
                  help="""Maximum number of articles to use for dev set.
  A value of 0 means no maximum. Default %default.""",
                  metavar="SIZE")
    op.add_option("--max-test-size", type='int', default=0,
                  help="""Maximum number of articles to use for test set.
  A value of 0 means no maximum. Default %default.""",
                  metavar="SIZE")
    op.add_option("-f", "--file",
                  help="""File containing lines to split.""",
                  metavar="FILE")
    op.add_option("-o", "--output-prefix",
                  help="""Prefix of files to output.""",
                  metavar="FILE")

  def handle_arguments(self, opts, op, args):
    global Opts
    Opts = opts
    self.need('file')
    self.need('output_prefix')

  def implement_main(self, opts, params, args):
    output_split_files(opts.file, opts.output_prefix,
                      [opts.training_fraction, opts.dev_fraction,
                          opts.test_fraction],
                      [opts.max_training_size, opts.max_dev_size,
                          opts.max_test_size],
                      ['training', 'dev', 'test'])

SplitLinesDevTest()
