#!/usr/bin/python

# A one-off for fixing up an old form of us-congress-twitter.txt.
import fileinput
import re

for line in fileinput.input():
  line = line.strip()
  m = re.match("^(.*\()(prev )?([A-Z][a-z]+)(.*\))$", line)
  if not m:
    print line
  elif m.group(3) not in ["Sen", "Gov"]:
    print "%s%sRep %s%s" % (m.group(1), m.group(2) or "", m.group(3), m.group(4))
  else:
    print line
