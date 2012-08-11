#!/usr/bin/python

import fileinput
import re

for line in fileinput.input():
  line = line.strip()
  if re.match("^$", line):
    continue
  if re.match("^[A-Z][A-Z a-z]*$", line):
    curstate = line
  else:
    m = re.match("^([0-9]+|At LG) (.+) ([RD]) (@.*|-+)$", line)
    if m:
      print "Rep. %s %s %s (%s %s)" % (m.group(2), m.group(3), m.group(4),
        curstate, m.group(1))
    else:
      m = re.match("^([A-Z][A-Z]) Sen. ([A-Za-z]+) ([A-Za-z]+) -(R|D|IND)( @.*|)$", line)
      if m:
        print "Sen. %s, %s %s %s (Sen %s)" % (m.group(3), m.group(2),
            m.group(4), m.group(5)[1:] if m.group(5) else "----------",
            m.group(1))
      else:
        print "Unrecognized line: %s" % line
