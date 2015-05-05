#!/usr/bin/env python

from nlputil import *
import re

prefix = "CWRED_20150415_fulltext"
file = open("%s.tab.txt" % prefix)
headers = next(file).strip()
outdatafile = open("%s-training.data.txt" % prefix, "w")  
for line in file:
  line = line.strip()
  splitline = re.split("\t", line)
  if len(splitline) > 18:
    splitline[17:] = [r'\t'.join(splitline[17:])]
  try:
    eventid, typ, typcode, startdate, enddate, stateabbr, state, dyerplace, perseusplace, geocodesource, tgn, lat, lon, linklatlon, source, editnotes, confidencecode, sentence = splitline
  except Exception as e:
    print "Error %s on line [%s]" % (e, line)
    continue
  sentence = sentence.replace('"', '')
  sentence = re.sub(r'\\n$', '', sentence)
  sentence = re.sub(r'\\t', '\t', sentence)
  sentence = re.sub(r'([a-z])([A-Z])', r'\1 \2', sentence)
  sentence = re.sub(r'([a-z])\(', r'\1 (', sentence)
  sentence = re.sub(r'\)([A-Z])', r') \1', sentence)
  #sentence = re.sub(r'--', ' -- ', sentence)
  perseusplace = perseusplace.replace('"', '')
  words = [word.replace("%", "%25").replace(":", "%3A") for word in
      split_text_into_words(sentence, ignore_punc=True) if word != "-"]
  countmap = intdict()
  for word in words:
    countmap[word] += 1
  countfield = ' '.join(["%s:%s" % (x,y) for x,y in countmap.iteritems()])
  textfield = sentence.replace("%", "%25").replace("\t", "%09")
  if lat and lon:
    coord = "%s,%s" % (lat, lon)
  else:
    coord = ""
  print >>outdatafile, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (
      eventid, typ, typcode, startdate, enddate, dyerplace, perseusplace, tgn,
      coord, countfield, textfield)

outschemafile = open("%s-training.schema.txt" % prefix, "w")
print >>outschemafile, "title\ttype\ttypecode\tstartdate\tenddate\tdyerplace\tperseusplace\ttgn\tcoord\tunigram-counts\ttext"
print >>outschemafile, "corpus-type\tgeneric"
print >>outschemafile, "split\ttraining"
outschemafile.close()
