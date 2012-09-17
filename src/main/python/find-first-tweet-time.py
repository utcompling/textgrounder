#!/usr/bin/env python

#######
####### find-first-tweet-time.py
#######
####### Copyright (c) 2012 Ben Wing.
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
import calendar

############################################################################
#                               Quick Start                                #
############################################################################

# This program reads in data from the specified (possibly bzipped) files,
# outputting the time of the first tweet in each as a time in milliseconds
# since the Unix Epoch. (Redirect stdout but not stderr to get these
# values independent of status messages.)

#######################################################################
#                            Process files                            #
#######################################################################

def process_file(infile):
  inproc = None
  desc = None
  try:
    if infile.endswith(".bz2"):
      errprint("Opening compressed input %s..." % infile)
      # close_fds is necessary to avoid deadlock in certain circumstances
      # (see split_bzip.py).  Probably not here but won't hurt.
      inproc = Popen("bzcat", stdin=open(infile, "rb"), stdout=PIPE, close_fds=True)
      desc = inproc.stdout
    else:
      errprint("Opening input %s..." % infile)
      desc = open(infile, "rb")
    lineno = 0
    for full_line in desc:
      lineno += 1
      line = full_line[:-1]
      if not line.startswith('{"'):
        errprint("%s: Unparsable line, not JSON?, #%s: %s" % (infile, lineno, line))
      else:
        json = None
        try:
          json = split_json(line)
        except Exception, exc:
          errprint("%s: Exception parsing JSON in line #%s: %s" % (infile, lineno, line))
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
                errprint("%s: Something weird with JSON in line #%s, around here: %s" % (infile, lineno, json[i-1:i+4]))
              else:
                json_time = json[i+2][1:-1].replace(" +0000 ", " UTC ")
                tweet_time = time.strptime(json_time,
                    "%a %b %d %H:%M:%S %Z %Y")
                if not tweet_time:
                  errprint("%s: Can't parse time in line #%s: %s" % (infile, lineno, json_time))
                else:
                  print "%s\t%s" % (infile, calendar.timegm(tweet_time)*1000L)
                  return
  finally:
    if inproc:
      inproc.kill()
      inproc.wait()
    if desc: desc.close()

# A very simple JSON splitter.  Doesn't take the next step of assembling
# into dictionaries, but easily could.
#
# FIXME: This is totally unnecessary, as Python has a built-in JSON parser.
# (I didn't realize this when I wrote the function.)
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
  op = OptionParser(usage="%prog [options] FILES ...")

  opts, args = op.parse_args()

  if not args:
    op.error("No input files specified")

  for infile in args:
    process_file(infile)

main()
