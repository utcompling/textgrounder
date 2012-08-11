#!/usr/bin/python

import fileinput
import re

for line in fileinput.input():
  line = line.strip()
  if re.match("^$", line):
    continue
  m = re.match("^<li>(<a .*twitter.com/([^\"]+)\">)?([A-Z])(.*?), ([A-Z].*?) +\((ID|[DRI])-([A-Z][A-Z])\).*",
    line)
  if m:
    print "Sen. %s%s, %s %s %s (Sen %s)" % (m.group(3), m.group(4).lower(),
      m.group(5), "@"+m.group(2) if m.group(2) else "----------", m.group(6), m.group(7))
  else:
    print "Unrecognized line: %s" % line
