#!/usr/bin/env python

#######
####### convert_geonames_cities_to_textdb.py
#######
####### Copyright (c) 2011 Ben Wing.
#######

# A more-or-less one-off program to convert the cities15000.txt or similar
# GeoNames files to a textdb corpus for input as a document of sorts.

import sys
from nlputil import *
from process_article_data import *

country_codes = {}
def read_country_codes(filename):
  for line in uchompopen(filename):
    (iso, iso3, iso_numeric, fips, country, capital, area, population,
     continent, tld, currency_code, currency_name, phone,
     postal_code_format, postal_code_regex, languages, geonameid,
     neighbours, equivalent_fips_code) = line.split("\t")
    country_codes[iso] = country
    country_codes[iso3] = country

admin1_codes = {}
def read_admin1_codes(filename):
  for line in uchompopen(filename):
    (code, name, asciiname, geonameid) = line.split("\t")
    (iso, admin1_code) = code.split(".")
    if iso not in admin1_codes:
      admin1_codes[iso] = {}
    admin1_codes[iso][admin1_code] = name

admin2_codes = {}
def read_admin2_codes(filename):
  for line in uchompopen(filename):
    (code, name, asciiname, geonameid) = line.split("\t")
    (iso, admin1_code, admin2_code) = code.split(".")
    if iso not in admin2_codes:
      admin2_codes[iso] = {}
    if admin1_code not in admin2_codes[iso]:
      admin2_codes[iso][admin1_code] = {}
    admin2_codes[iso][admin1_code][admin2_code] = name

def convert(filename):
  for line in uchompopen(filename):
    (geonameid, name, asciiname, alternatenames, latitude, longitude,
     feature_class, feature_code, country_code, cc2, admin1_code,
     admin2_code, admin3_code, admin4_code, population, elevation, dem,
     timezone, modification_date) = line.split("\t")
    country = country_codes[country_code]
    if (country_code not in admin1_codes or
        admin1_code not in admin1_codes[country_code]):
      if admin1_code == "":
        place = "%s, unknown area, %s" % (name, country)
      else:
        place = "%s, area %s, %s" % (name, admin1_code, country)
    else:
      admin1 = admin1_codes[country_code][admin1_code]
      place = "%s, %s, %s" % (name, admin1, country)
    fields = [place, "%s,%s" % (latitude, longitude), population]
    uniprint("\t".join(fields))

def convert_geonames_cities_to_textdb(country_code_file, admin1_code_file,
    args):
  read_country_codes(country_code_file)
  read_admin1_codes(admin1_code_file)
  for filename in args:
    convert(filename)

############################################################################
#                                  Main code                               #
############################################################################

class ConvertGeonamesCitiesToTextDB(NLPProgram):
  def populate_options(self, op):
    op.add_option("-a", "--admin1-code-file",
                  help="""File containing admin1 code information, for
converting first-level administrative codes to English.""",
                  metavar="FILE")
    op.add_option("-c", "--country-code-file",
                  help="""File containing country code information, for
converting ISO country codes to English.""",
                  metavar="FILE")

  def handle_arguments(self, opts, op, args):
    global Opts
    Opts = opts
    self.need('admin1_code_file')
    self.need('country_code_file')

  def implement_main(self, opts, params, args):
    convert_geonames_cities_to_textdb(opts.country_code_file,
      opts.admin1_code_file, args)

ConvertGeonamesCitiesToTextDB()
