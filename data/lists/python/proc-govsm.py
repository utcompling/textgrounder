#!/usr/bin/python

import fileinput
import re

# Process raw output from govsm.com, either House, Senate or Governors, or
# campaigns for any of the three.  Note that for campaigns for Governors,
# you will need to insert a line
#
# Welcome to the Governors Campaigns
#
# above the section containing the campaigns for governor.

campaigns = False
where = ""
party = ""
for line in fileinput.input():
  line = line.strip()
  if re.match("^Welcome to the.*House", line):
    title = "Rep"
  elif re.match("^Welcome to the.*Senate", line):
    title = "Sen"
  elif re.match("^Welcome to the.*Governors", line):
    title = "Gov"
  if re.match("^Welcome to the.*Campaigns", line):
    print "set campaigns to True"
    campaigns = True
  fields = re.split("\|\|", line)
  if len(fields) >= 10:
    first = fields[1].strip()

    last = fields[2].strip()
    last = re.sub(r"<span.*?</span>", "", last)
    if "//" in last:
      last = re.sub(r"^[^ ]* ", "", last)
    last = re.sub(r"\].*", "", last)

    lastparty = party
    party = fields[3].strip()
    party = re.sub(".*\|", "", party).strip()

    lastwhere = where
    where = fields[4].strip()
    #print "lastwhere = '%s', where = '%s'" % (lastwhere, where)

    #if campaigns and lastwhere == where and lastparty == party:
    fulltitle = "Cand-" + title
    #else:
    #  fulltitle = title

    account = fields[6].strip()
    if account in ("No", "T"):
      account = "-----------"
    else:
      #print account
      account = re.sub(r"/?\]\].*", "", account)
      #print account
      account = re.sub(r"\|?<span.*", "", account)
      #print account
      account = re.sub(r".*twitter.com.*/", "", account)
      #print account
      account = re.sub(r".*=", "", account)
      #print account
      account = "@" + account
    m = re.match("^([A-Z][A-Z])0*([0-9]+)?$", where)
    if title == "Rep" and m:
      state = m.group(1)
      district = m.group(2) or "At Large"
      office = "%s %s %s" % (title, state, district)
    else:
      office = title + " " + where
    print "%s. %s, %s %s %s (%s)" % (fulltitle, last, first, account, party, office)
  elif not line.startswith("|-"):
    print line
