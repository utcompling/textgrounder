#!/usr/bin/env python

#######
####### split_bzip.py
#######
####### Copyright (c) 2011 Ben Wing.
#######

import sys, re
import math
import fileinput
from subprocess import *
from nlputil import *
import itertools
import time
import os.path
import traceback

############################################################################
#                               Quick Start                                #
############################################################################

# This program reads in data from the specified bzipped files, concatenates
# them, splits them at newlines after a certain amount of data has been
# read, and bzips the results.  The files are assumed to contains tweets in
# JSON format, and the resulting split files after named after the date
# in the first tweet of the split.  We take some care to ensure that we
# start the file with a valid tweet, in case something invalid is in the
# file.

#######################################################################
###                        Utility functions                        ###
#######################################################################

def uniprint(text, file=sys.stdout):
  '''Print Unicode text in UTF-8, so it can be output without errors'''
  if type(text) is unicode:
    print text.encode("utf-8")
  else:
    print text

def errprint(text):
  uniprint(text, sys.stderr)

def warning(text):
  '''Output a warning, formatting into UTF-8 as necessary'''
  if show_warnings:
    uniprint("Warning: %s" % text)

#######################################################################
#                            Process files                            #
#######################################################################

def finish_outproc(outproc):
  outproc.stdin.close()
  errprint("Waiting for termination ...")
  outproc.wait()
  errprint("Waiting for termination ... done.")

def split_tweet_bzip_files(opts, args):
  status = StatusMessage("tweet")
  totalsize = 0
  outproc = None
  lines_at_start = []
  for infile in args:
    inproc = Popen("bzcat", stdin=open(infile, "rb"), stdout=PIPE)
    for full_line in inproc.stdout:
      line = full_line[:-1]
      status.item_processed()
      if not line.startswith('{"'):
        errprint("Unparsable line, not JSON?, #%s: %s" % (status.num_processed(), line))
      else:
        if not outproc or totalsize >= opts.split_size:
          json = None
          try:
            json = split_json(line)
          except Exception, exc:
            errprint("Exception parsing JSON in line #%s: %s" % (status.num_processed(), line))
            errprint("Exception is %s" % exc)
            traceback.print_exc()
          if json:
            json = json[0]
            #errprint("Processing JSON %s:" % json)
            #errprint("Length: %s" % len(json))
            for i in xrange(len(json)):
              #errprint("Saw %s=%s" % (i, json[i]))
              if json[i] == '"created_at"':
                #errprint("Saw created")
                if i + 2 >= len(json) or json[i+1] != ':' or json[i+2][0] != '"' or json[i+2][-1] != '"':
                  errprint("Something weird with JSON in line #%s, around here: %s" % (status.num_processed(), json[i-1:i+4]))
                else:
                  json_time = json[i+2][1:-1].replace(" +0000 ", " UTC ")
                  tweet_time = time.strptime(json_time,
                      "%a %b %d %H:%M:%S %Z %Y")
                  if not tweet_time:
                    errprint("Can't parse time in line #%s: %s" % (status.num_processed(), json_time))
                  else:
                    timesuff = time.strftime("%Y-%M-%d.%H%M", tweet_time)
                    def make_filename(suff):
                      return opts.output_prefix + suff + ".bz2"
                    outfile = make_filename(timesuff)
                    if os.path.exists(outfile):
                      errprint("Warning, path %s exists, not overwriting" % outfile)
                      for ind in itertools.count(1):
                        # Use _ not - because - sorts before the . of .bz2 but
                        # _ sorts after (as well as after all letters and numbers).
                        outfile = make_filename(timesuff + ("_%03d" % ind))
                        if not os.path.exists(outfile):
                          break
                    if outproc:
                      finish_outproc(outproc)
                    errprint("About to write to %s..." % outfile)
                    outproc = Popen("bzip2", stdin=PIPE, stdout=open(outfile, "wb"))
                    totalsize = 0
                break
      if outproc:
        starttext = ''.join(lines_at_start)
        if lines_at_start:
          outproc.stdin.write(starttext)
          totalsize += len(starttext)
          lines_at_start = None # So we can't add any more lines to catch bugs
        outproc.stdin.write(full_line)
        totalsize += len(full_line)
      else:
        lines_at_start += [full_line]
  if outproc:
    finish_outproc(outproc)

# A very simply JSON splitter.  Doesn't take the next step of assembling
# into dictionaries, but easily could.
def split_json(line):
  split = re.split(r'("(?:\\.|[^"])*?"|[][:{},])', line)
  split = (x for x in split if x) # Filter out empty strings
  curind = 0
  def get_nested(endnest):
    nest = []
    try:
      while True:
        item = next(split)
        if item == endnest:
          return nest
        elif item == '{':
          nest += [get_nested('}')]
        elif item == '[':
          nest += [get_nested(']')]
        else:
          nest += [item]
    except StopIteration:
      if not endnest:
        return nest
      else:
        raise
  return get_nested(None)
 
#######################################################################
#                                Main code                            #
#######################################################################

def main():
  op = OptionParser(usage="%prog [options] input_dir")
  op.add_option("-s", "--split-size", metavar="SIZE",
                type="int", default=1000000000,
                help="""Size (uncompressed) of each split.  Note that JSON
tweets compress in bzip about 8 to 1, hence 1 GB is a good uncompressed size
for Hadoop.  Default %default.""")
  op.add_option("-o", "--output-prefix", metavar="PREFIX",
                help="""Prefix to use for all splits.""")

  opts, args = op.parse_args()

  if not opts.output_prefix:
    op.error("Must specify output prefix using -o or --output-prefix")
  if not args:
    op.error("No input files specified")

  split_tweet_bzip_files(opts, args)

main()
