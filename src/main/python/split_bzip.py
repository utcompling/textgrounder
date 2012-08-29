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

############################################################################
#                               Notes                                      #
############################################################################

# When this script was run, an example run was
#
# run-nohup ~tgp/split_bzip.py -s 1450000000 -o split.global. ../global.tweets.2011-*.bz2 &
#
# The value given to -s is the uncompressed size of each split and has been
# empirically determined to give compressed sizes slightly under 192 MB --
# useful for Hadoop as it means that each split will take slightly under 3
# HDFS blocks at the default 64MB block size.
#
# NOTE: On the Longhorn work machines with 48GB of memory, it takes about 12
# hours to process 20-21 days of global tweets and 24-25 days of spritzer
# tweets.  Given that you only have a maximum of 24 hours of time, you
# should probably not process more than about a month's worth of tweets in
# a single run. (As an alternative, if your process gets terminated due to
# running out of time or for any other reason, try removing the last,
# partially written split file and then rerunning the command with the
# additional option --skip-existing.  This will cause it to redo the same split
# but not overwrite the files that already exist.  Since bzip compression takes
# up most of the time, this should fairly rapidly scan through all of the
# already-written files and then do the rest of them.  As an example, a split
# run on spritzer output that took 24 hours to process 49 days took only
# 3.5 hours to skip through them when --skip-existing was used.)

#######################################################################
#                            Process files                            #
#######################################################################

def split_tweet_bzip_files(opts, args):
  status = StatusMessage("tweet")
  totalsize = 0
  outproc = None
  skip_tweets = False

  def finish_outproc(outproc):
    errprint("Total uncompressed size this split: %s" % totalsize)
    errprint("Total number of tweets so far: %s" % status.num_processed())
    outproc.stdin.close()
    errprint("Waiting for termination of output process ...")
    outproc.wait()
    errprint("Waiting for termination of output process ... done.")

  for infile in args:
    errprint("Opening input %s..." % infile)
    errprint("Total uncompressed size this split so far: %s" % totalsize)
    errprint("Total number of tweets so far: %s" % status.num_processed())
    # NOTE: close_fds=True turns out to be necessary to avoid a deadlock in
    # the following circumstance:
    #
    # 1) Open input from bzcat.
    # 2) Open output to bzip2.
    # 3) bzcat ends partway through a split (possibly after multiple splits,
    #    and hence multiple invocations of bzip2).
    # 4) Wait for bzcat to finish, then start another bzcat for the next file.
    # 5) End the split, close bzip2's stdin (our output pipe), and wait for
    #    bzip2 to finish.
    # 6) Blammo!  Deadlock while waiting for bzip2 to finish.
    #
    # When we opened the second bzcat, if we don't call close_fds, it
    # inherits the file descriptor of the pipe to bzip2, and that screws
    # things up.  Presumably, the file descriptor inheritance means that
    # there's still a file descriptor to the pipe to bzip2, so closing the
    # output doesn't actually cause the pipe to get closed -- hence bzip2
    # waits indefinitely for more input.
    inproc = Popen("bzcat", stdin=open(infile, "rb"), stdout=PIPE, close_fds=True)
    for full_line in inproc.stdout:
      line = full_line[:-1]
      status.item_processed()
      if not line.startswith('{"'):
        errprint("Unparsable line, not JSON?, #%s: %s" % (status.num_processed(), line))
      else:
        if totalsize >= opts.split_size or (not outproc and not skip_tweets):
          # We need to open a new file.  But keep writing the old file
          # (if any) until we see a tweet with a time in it.
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
                    # Now we're ready to create a new split.
                    skip_tweets = False
                    timesuff = time.strftime("%Y-%m-%d.%H%M-UTC", tweet_time)
                    def make_filename(suff):
                      return opts.output_prefix + suff + ".bz2"
                    outfile = make_filename(timesuff)
                    if os.path.exists(outfile):
                      if opts.skip_existing:
                        errprint("Skipping writing tweets to existing %s" % outfile)
                        skip_tweets = True
                      else:
                        errprint("Warning, path %s exists, not overwriting" % outfile)
                        for ind in itertools.count(1):
                          # Use _ not - because - sorts before the . of .bz2 but
                          # _ sorts after (as well as after all letters and numbers).
                          outfile = make_filename(timesuff + ("_%03d" % ind))
                          if not os.path.exists(outfile):
                            break
                    if outproc:
                      finish_outproc(outproc)
                      outproc = None
                    totalsize = 0
                    if not skip_tweets:
                      errprint("About to write to %s..." % outfile)
                      outfd = open(outfile, "wb")
                      outproc = Popen("bzip2", stdin=PIPE, stdout=outfd, close_fds=True)
                      outfd.close()
                break
      totalsize += len(full_line)
      if skip_tweets:
        pass
      elif outproc:
        outproc.stdin.write(full_line)
      else:
        errprint("Warning: Nowhere to write bad line #%s, skipping: %s" % (status.num_processed(), line))
    errprint("Waiting for termination of input process ...")
    inproc.stdout.close()
    # This sleep probably isn't necessary.  I put it in while attempting to
    # solve the deadlock when closing the pipe to bzip2 (see comments above
    # about close_fds).
    sleep_time = 5
    errprint("Sleeping %s seconds ..." % sleep_time)
    time.sleep(sleep_time)
    inproc.wait()
    errprint("Waiting for termination of input process ... done.")
  if outproc:
    finish_outproc(outproc)
    outproc = None

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
  op = OptionParser(usage="%prog [options] input_dir")
  op.add_option("-s", "--split-size", metavar="SIZE",
                type="int", default=1000000000,
                help="""Size (uncompressed) of each split.  Note that JSON
tweets compress in bzip about 8 to 1, hence 1 GB is a good uncompressed size
for Hadoop.  Default %default.""")
  op.add_option("-o", "--output-prefix", metavar="PREFIX",
                help="""Prefix to use for all splits.""")
  op.add_option("--skip-existing", action="store_true",
                help="""If we would try and open an existing file,
skip writing any of the corresponding tweets.""")

  opts, args = op.parse_args()

  if not opts.output_prefix:
    op.error("Must specify output prefix using -o or --output-prefix")
  if not args:
    op.error("No input files specified")

  split_tweet_bzip_files(opts, args)

main()
