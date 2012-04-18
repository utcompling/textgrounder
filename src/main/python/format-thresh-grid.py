#!/usr/bin/env python

from nlputil import *
import fileinput
import re

switch_thresh_and_grid = True

errdists = dictdict()

for line in fileinput.input():
  line = line.strip()
  m = re.match(r'.*thresh: ([0-9.]*), grid: ([0-9.]*),.*true error distance.*\(([0-9.]*) km.*', line)
  if not m:
    errprint("Can't parse line: %s", line)
  else:
    thresh = float(m.group(1))
    grid = float(m.group(2))
    dist = float(m.group(3))
    if switch_thresh_and_grid:
      errdists[grid][thresh] = dist
    else:
      errdists[thresh][grid] = dist

first = True
for (thresh, dic) in key_sorted_items(errdists):
  if first:
    first = False
    errprint(r"   & %s \\" % (
        ' & '.join(["%g" % grid for grid in sorted(dic.keys())])))
    errprint(r"\hline")
  errprint(r"%g & %s \\" % (thresh,
      ' & '.join(["%g" % dist for (grid, dist) in key_sorted_items(dic)])))

