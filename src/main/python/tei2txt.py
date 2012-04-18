#! /usr/bin/env python

import sys
import os
import re
import gzip
import fnmatch

from codecs import latin_1_decode
from unicodedata import normalize
from tei_entities import pcl_tei_entities

commaRE = re.compile(",")
nonAlpha = re.compile("[^A-Za-z]")

pte = pcl_tei_entities()

def cleanWord(word):
    word = word.lower()
    if len(word) < 2:
        word = ""
    return word

def strip_text (text):
    text = latin_1_decode(text)[0]
    text = normalize('NFD',text).encode('ascii','ignore')

    text = re.sub('&mdash+;', ' ', text)   # convert mdash to " "
#    text = re.sub('&amp;', ' and ', text)   # convert mdash to " "
    text = pte.replace_entities(text)
#    text = re.sub('&[A-Za-z]+;', '', text)   # convert ampersand stuff to ""
    text = re.sub('<[^>]*>', ' ', text)   # strip HTML markup
    text = re.sub('\s+', ' ', text)      # strip whitespace

    return text

directory_name = sys.argv[1]
output_raw_dir = sys.argv[2]

if not os.path.exists(output_raw_dir):
    os.makedirs(output_raw_dir)

files = os.listdir(directory_name)
for file in files:
    add_line = False
    write_text = False
    if fnmatch.fnmatch(file,"*.xml"):
        print "******",file
        newname = file[:-4]+".txt"
        raw_writer = open(output_raw_dir+"/"+newname,"w")
        file_reader = open(directory_name+"/"+file)
        text = ""

        header_end = False
        while not header_end:
            line = file_reader.readline()
            m = re.search('\s*\]>', line)
            if m:
                header_end = True

        for line in file_reader.readlines():
            text = line.strip()
            text = strip_text(text).strip()
            if text != "":
                raw_writer.write(text)
                raw_writer.write("\n")

        raw_writer.close()
                
