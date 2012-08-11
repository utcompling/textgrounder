#!/usr/bin/python

import fileinput
import re

abbrevs = {}
for line in open("state-abbrevs.txt"):
  line = line.strip()
  m = re.match("^([A-Z][A-Z]) (.*)$", line)
  abbrevs[m.group(1)] = m.group(2)

for line in fileinput.input():
  line = line.strip()
  while True:
    m = re.match("^(.*\([^()]*)([A-Z][A-Z])([^()]*\))$", line)
    if not m:
      print line
      break
    elif m.group(2) not in abbrevs:
      print line
      print "Strange abbrevation %s not recognized" % m.group(2)
    else:
      line = "%s%s%s" % (m.group(1), abbrevs[m.group(2)], m.group(3))
