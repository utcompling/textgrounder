#!/usr/bin/env python

from nlputil import *
import fileinput
import re

def median(values):
  values = sorted(values)
  vallen = len(values)

  if vallen % 2 == 1:
    return values[(vallen+1)/2-1]
  else:
    lower = values[vallen/2-1]
    upper = values[vallen/2]

    return (float(lower + upper)) / 2  

def mean(values):
  return float(sum(values))/len(values)

def tokm(val):
  return 1.609*val

vals = []
for line in fileinput.input():
  line = line.strip()
  m = re.match(r'.*Distance (.*?) miles to predicted region center', line)
  if not m:
    errprint("Can't parse line: %s", line)
  else:
    vals += [float(m.group(1))]

med = median(vals)
mn = mean(vals)
uniprint("Median: %g miles (%g km)" % (med, tokm(med)))
uniprint("Mean: %g miles (%g km)" % (mn, tokm(mn)))
