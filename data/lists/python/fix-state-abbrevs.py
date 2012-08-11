#!/usr/bin/python

# Fix up state abbrevations inside of parentheses to the full state names.
# Meant to be run after a file has been converted to standard form, where the
# state in inside of parens.  Only acts inside of parens to avoid munging
# Twitter handles containing state abbrevs.

import fileinput
import re

abbrevs = {
  "AL":"Alabama",
  "AK":"Alaska",
  "AS":"American Samoa",
  "AZ":"Arizona",
  "AR":"Arkansas",
  "CA":"California",
  "CO":"Colorado",
  "CT":"Connecticut",
  "DE":"Delaware",
  "DC":"District of Columbia",
  "FM":"Fed. States of Micronesia",
  "FL":"Florida",
  "GA":"Georgia",
  "GU":"Guam",
  "HI":"Hawaii",
  "ID":"Idaho",
  "IL":"Illinois",
  "IN":"Indiana",
  "IA":"Iowa",
  "KS":"Kansas",
  "KY":"Kentucky",
  "LA":"Louisiana",
  "ME":"Maine",
  "MH":"Marshall Islands",
  "MD":"Maryland",
  "MA":"Massachusetts",
  "MI":"Michigan",
  "MN":"Minnesota",
  "MS":"Mississippi",
  "MO":"Missouri",
  "MT":"Montana",
  "NE":"Nebraska",
  "NV":"Nevada",
  "NH":"New Hampshire",
  "NJ":"New Jersey",
  "NM":"New Mexico",
  "NY":"New York",
  "NC":"North Carolina",
  "ND":"North Dakota",
  "MP":"Northern Mariana Is.",
  "OH":"Ohio",
  "OK":"Oklahoma",
  "OR":"Oregon",
  "PW":"Palau",
  "PA":"Pennsylvania",
  "PR":"Puerto Rico",
  "RI":"Rhode Island",
  "SC":"South Carolina",
  "SD":"South Dakota",
  "TN":"Tennessee",
  "TX":"Texas",
  "UT":"Utah",
  "VT":"Vermont",
  "VA":"Virginia",
  "VI":"Virgin Islands",
  "WA":"Washington",
  "WV":"West Virginia",
  "WI":"Wisconsin",
  "WY":"Wyoming",
}

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
