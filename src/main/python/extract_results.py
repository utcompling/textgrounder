#!/usr/bin/env python

# Extract results from a number of runs. Converted from shell script
# 'tg-extract-results'. The intent was to speed things up but unfortunately
# it doesn't always work. It seems the majority of the time is spent reading
# the file from disk, and that's the same for both versions. However, it
# does noticeably speed up reading 31 relatively short files in Maverick
# (max size about 3 MB). It runs in less than half a second, vs. about
# 12 seconds for the old version. This suggests it will yield dramatic
# speedups on Maverick, exactly as I would like. It also correctly sorts by
# CPUTIME/RUNTIME/RSS.

# Given output from a run-* front end, extract the mean and median errors
# from each specified file, compute the avg mean/median error, and output
# a line giving the errors along with relevant parameters for that particular
# run.

import re
import argparse

# Equivalent of $(grep -m 1 $regex)
def findfirst(lines, regex):
  for line in lines:
    if re.search(regex, line):
      return line
  return ""

# Equivalent of $(grep $regex)
def find(lines, regex):
  ret = []
  for line in lines:
    if re.search(regex, line):
      ret.append(line)
  return ret

def sub(line, r1, r2):
  return re.sub(r1, r2, line)

def findsub(lines, find, r1, r2):
  return sub(findfirst(lines, find), r1, r2)

# Equivalent of $(sed -n "/$r1/,/$r2/p")
def findsection(lines, r1, r2):
  ret = []
  start = False
  for line in lines:
    if not start and re.search(r1, line):
      start = True
    if start:
      ret.append(line)
    if start and re.search(r2, line):
      start = False
  return ret

def safefloat(arg):
  arg = "%s" % arg
  arg = re.sub("[^-0-9.]", "", arg)
  try:
    return float(arg)
  except:
    return 0.0

parser = argparse.ArgumentParser(description="Extract results from a number of runs.")
def flag(*args):
  parser.add_argument(*args, action="store_true")
flag("--no-sort")
flag("--sort-average", "--sort-avg", "--sort")
flag("--sort-mean")
flag("--sort-median")
flag("--sort-accuracy", "--sort-acc")
flag("--sort-acc161")
flag("--sort-numeval")
flag("--sort-numtrain")
flag("--sort-file", "--sort-name")
flag("--sort-runtime")
flag("--sort-cputime")
#It's tricky to sort the way we output it; need to sort on raw number
#and then convert to human-readable, but just sorting on the "cooked"
#number works if everything is in the same units e.g. GB.
flag("--sort-rss")
flag("--omit-average", "--omit-avg")
flag("--omit-mean")
flag("--omit-median")
flag("--omit-accuracy", "--omit-acc")
flag("--omit-acc161")
flag("--omit-numeval")
flag("--omit-numtrain")
flag("--omit-file", "--omit-name")
flag("--omit-runtime")
flag("--omit-cputime")
flag("--omit-rss")
flag("--verbose")
flag("--debug")
parser.add_argument("files", nargs="+")
pp = parser.parse_args()

if not pp.verbose:
  #print "  #Eval   #Train  %Acc. Acc@161 Mean   Median  Average  Runtime Cputime   RSS    File"
  if not pp.omit_numeval:
    print "  #Eval",
  if not pp.omit_numtrain:
    print "  #Train",
  if not pp.omit_accuracy:
    print " %Acc.",
  if not pp.omit_acc161:
    print "Acc@161",
  if not pp.omit_mean:
    print " Mean ",
  if not pp.omit_median:
    print " Median",
  if not pp.omit_average:
    print "Average",
  if not pp.omit_runtime:
    print " Runtime",
  if not pp.omit_cputime:
    print " Cputime",
  if not pp.omit_rss:
    print "   RSS   ",
  if not pp.omit_file:
    print "File",
  print ""

def formattime(time0, time0prompt):
  # Convert time from "24 minutes 5 seconds" or "0 minutes 6 seconds"
  # (old style) or "24 min 5 sec" or "6 sec" (new style) to a raw version
  # of HH:MM:SS.
  time0 = sub(time0, " (hour|hr)s? ", ":")
  time0 = sub(time0, " min(ute)?s? ", ":")
  time0 = sub(time0, " sec(ond)s?", "")
  if pp.debug:
    print "%s: [%s]" % (time0prompt, time0)
  # Reformat to proper HH:MM:SS, making sure to have two digits for the
  # minutes.
  if not time0:
    return 0, "NA"
  else:
    m = re.match("(?:(.*?):)?(?:(.*?):)?(.*?)$", time0)
    # We may have only a number of seconds; check for this.
    hrs = m.group(1)
    if hrs == None:
      hrs = 0
    else:
      hrs = float(hrs)
    mins = m.group(2)
    if mins == None:
      mins = hrs
      hrs = 0
    else:
      mins = float(mins)
    secs = float(re.sub(" .*", "", m.group(3)))
    totalsecs = hrs*3600 + mins*60 + secs
    if hrs == 0 and mins == 0:
      secs = "%04.1f" % secs
    else:
      secs += 0.5
      while secs >= 60:
        secs -= 60
        mins += 1
      while mins >= 60:
        mins -= 60
        hrs += 1
      secs = "%02d" % secs
    mins = "%02d" % mins
    # secs = "%04.1f" % secs
    if hrs == 0:
      hrs = ""
    else:
      hrs = "%s:" % int(hrs)
    return totalsecs, "%s%s:%s" % (hrs, mins, secs)

def output(files):
  retval = []
  for fil in files:
    if pp.debug:
      print "[%s]" % fil
      print ""
    contents = [x.rstrip("\n") for x in open(fil).readlines()]
    args = findfirst(contents, r"^Nohup script invoked as:.* --full-id ")
    if args:
      args = re.sub(r"^.*? --full-id (\S+) .*$", r"\1", args)
    else:
      args = findfirst(contents, "^Arguments:")
      args = re.sub(r"^Arguments: ", "", args)
      args = re.sub(r"--input(-corpus)? [^ ]*/", "", args)
      args = re.sub(r"  *--*([^ ]*)  *([^-][^ ]*)", r" \1=\2", args)
      args = re.sub(r"  *--*([^ ]*)", r" \1", args)
      args = re.sub(" ", ".", args)
      args = re.sub("/", "-", args)
    results = findsection(contents, "^Final results", "^Ending final results")
    mean = findsub(results, "Mean true error", r"^.*distance = +(\S+).*", r"\1")
    median = findsub(results, "Median true error", r"^.*distance = +(\S+).*", r"\1")
    avg = None
    if mean and median:
      if pp.debug:
        print "Extracted mean: [%s]" % mean
        print "Extracted median: [%s]" % median
      avg = "%.2f" % (0.5*(float(mean) + float(median)))
    acc = findsub(results, "Percent correct at rank <= 1 =", r".*= (.*?)%.*", r"\1")
    acc161 = findsub(results, "Accuracy@161 =", r".*= (.*?)%.*", r"\1")
    numeval = findsub(results, "instances.total", r".*= (.*?) *$", r"\1")
    numtrain = findsub(results, r"bytask.*num_training_documents_by_split\.training =",
      r".*= (.*?) *$", r"\1")
    finalstats = findsection(contents, "^Ending final results", "^Program running time/p")
    # Note: This used to grep for 'Program running time:'; the elapsed time
    # below is similar but doesn't include time spent determing memory usage,
    # etc.
    runtimeraw = findsub(finalstats, "Total elapsed time since program start:",
      ".*: ", "")
    runtimesecs, runtime = formattime(runtimeraw, "Runtime0")
    cputimeraw = findsub(finalstats, "Total CPU time since program start with children:",
      ".*: ", "")
    cputimesecs, cputime = formattime(cputimeraw, "Cputime0")
    # Check for the old way of outputting and convert to new-style (618.63 MB).
    # The old style had various lines beginning "Memory usage" for different
    # usage stats, and had the resident size given as something like
    #
    # Memory usage, actual (i.e. resident set) (proc): 618,635,264 bytes
    #
    rss = findsub(finalstats, "Memory usage, actual", ".*: (.*?) bytes", r"\1")
    if rss:
      rss = re.sub(",", "", rss)
      rss = float(rss)
      rssbytes = rss
      if rss >= 1000000000:
        rss = "%.2fGB" % (rss/1000000000.0)
      elif rss >= 1000000:
        rss = "%.2fMB" % (rss/1000000.0)
      else:
        rss = "%.2fKB" % (rss/1000.0)
    else:
      # The new way, which lines like
      #
      # Memory usage: virtual: 13.51 GB, resident: 1.49 GB, Java heap: 59.61 MB
      #
      rss = findsub(finalstats, "Memory usage:", ".*resident: (.*?) (.B).*$",
        r"\1\2")
      rssbytes = 0
      if len(rss) >= 2 and rss[-1] == 'B':
        if rss[-2] == 'G':
          rssbytes = 1000000000 * float(rss[0:-2])
        elif rss[-2] == 'M':
          rssbytes = 1000000 * float(rss[0:-2])
        elif rss[-2] == 'K':
          rssbytes = 1000 * float(rss[0:-2])
    if mean or median or avg or acc:
      skip = False
    else:
      skip = True
    if not numeval:
      numeval="NA"
    if not numtrain:
      numtrain="NA"
    if not acc:
      acc="NA"
    if not acc161:
      acc161="NA"
    if not mean:
      mean="NA"
    if not median:
      median="NA"
    if not avg:
      avg="NA"
    if not rss:
      rss="NA"
    numeval="%7s" % numeval
    numtrain="%8s" % numtrain
    acc="%6s" % acc
    acc161="%6s" % acc161
    mean="%7s" % mean
    median="%7s" % median
    avg="%7s" % avg
    runtime="%8s" % runtime
    cputime="%8s" % cputime
    rss="%8s" % rss
    if pp.debug:
      print "Args: [%s]" % args
      print "Mean: [%s]" % mean
      print "Median: [%s]" % median
      print "Average: [%s]" % avg
      print "%Accuracy: [%s]" % acc
      print "Acc@161: [%s]" % acc161
      print "#Eval: [%s]" % numeval
      print "#Train: [%s]" % numtrain
      print "Runtime: [%s]" % runtime
      print "Cputime: [%s]" % cputime
      print "RSS: [%s]" % rss
    if pp.verbose:
      print args, findfirst(results, "true error")
      if avg:
        print "%s   Avg-mean-median true error distance = %s km" % (args, avg)
    elif not skip:
      #echo "$numeval $numtrain $acc $mean $median $avg $runtime $cputime $rss $args"
      line = ""
      if not pp.omit_numeval:
        line += "%s " % numeval
      if not pp.omit_numtrain:
        line += "%s " % numtrain
      if not pp.omit_accuracy:
        line += "%s " % acc
      if not pp.omit_acc161:
        line += "%s " % acc161
      if not pp.omit_mean:
        line += "%s " % mean
      if not pp.omit_median:
        line += "%s " % median
      if not pp.omit_average:
        line += "%s " % avg
      if not pp.omit_runtime:
        line += "%s " % runtime
      if not pp.omit_cputime:
        line += "%s " % cputime
      if not pp.omit_rss:
        line += "%s " % rss
      if not pp.omit_file:
        line += args
      if pp.no_sort:
        print line
        key = None
      elif pp.sort_file:
        key = args
      elif pp.sort_runtime:
        key = -runtimesecs
      elif pp.sort_cputime:
        key = -cputimesecs
      elif pp.sort_rss:
        key = -rssbytes
      elif pp.sort_average:
        key = safefloat(avg)
      elif pp.sort_mean:
        key = safefloat(mean)
      elif pp.sort_median:
        key = safefloat(median)
      elif pp.sort_accuracy:
        key = -safefloat(acc)
      elif pp.sort_acc161:
        key = -safefloat(acc161)
      elif pp.sort_numeval:
        key = -safefloat(numeval)
      elif pp.sort_numtrain:
        key = -safefloat(numtrain)
      else:
        key = safefloat(avg)
      retval.append([key, line])
  return retval

retval = output(pp.files)
if not pp.no_sort:
  retval = sorted(retval, key=lambda x:x[0])
  for line in retval:
    print line[1]

