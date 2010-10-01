#!/usr/bin/python

#######
####### processwiki.py
#######
####### Copyright (c) 2010 Ben Wing.
#######

############################################################################
#                              Documentation                               #
############################################################################

##### Quick start

# This program processes the article dump from Wikipedia.  Dump is on
# stdin.  Outputs to stdout.  Written flexibly so that it can be modified
# to do various things.  To run it, use something like this:
#
# bzcat enwiki-20100905-pages-articles.xml.bz2 | processwiki.py > wiki-words.out

#####  How this program works

# Currently it does the following:
#
# 1. Locate the article title and text.
#
# 2. Find any coordinates specified either in Infobox or Coord templates.
#    If found, the first such coordinate in an article is output with lines
#    like 
#
#    Article title: Politics of Angola
#    Article coordinates: 13.3166666667,-169.15
#
# 3. For articles with coordinates in them, locate all the "useful" words in
#    the article text.  This ignores HTML codes like <sup>, comments,
#    stuff like [[ or ]], anything inside of <math>...</math>, etc.  It
#    tries to do intelligent things with templates (stuff inside of {{...}})
#    and internal links (inside of [[...]]), and ignores external links
#    ([http:...]).  The words are split on whitespace, ignoring punctuation
#    such as periods and commas, and the resulting words are counted up, and
#    the count of each different word is output, one per line like
#
#    Birmingham = 48
#
# There is also a debug flag.  If set, lots of additional stuff is output.
# Among them are warnings like
#
#    Warning: Nesting level would drop below 0; string = }, prevstring =  (19
#
# Note that since words are broken on spaces, there will never be a space
# in the outputted words.  Hence, the lines containing directives (e.g.
# the article title) can always be distinguished from lines containing words.
#
# Note also that the following terminology is used here, which may not be
# standard:
#
# Internal link: A link to another Wikipedia article, of the form [[...]].
# External link: A link to an external URL, of the form [...].
# Template: An expression of the form {{...}}, with arguments separated by
#           the pipe symbol |, that processes the arguments and subsitutes
#           the processed text; it may also trigger other sorts of actions.
#           Similar to the macros in C or M4.
# Macro: An expression that results in some other text getting substituted,
#        either a template {{...}} or an internal link [[...]].

##### Internal workings; how to extend the program

# Note that this program is written so that it can be flexibly extended to
# allow for different sorts of processing of the Wikipedia dump.  See the
# following description, which indicates where to change in order to
# implement different behavior.

# 5. If an article is a redirect to another article, instead of the words,
#    a single line will be output like this:
#
#    Redirect to: Computer accessibility
#
# 6. Finally, the last line output looks like this:
#
#    Notice: ending processing
#    
#
# The basic functioning of this code is controlled by an article handler class.
# The default handler class is ArticleHandler.  Usually it is
# sufficient to subclass this handler class, as it provides hooks to do
# interesting things, which by default do nothing.  You can also subclass
# ArticleHandlerForUsefulText if you want the source text processed for
# "useful text" (what the Wikipedia user sees, plus similar-quality
# hidden text).
#
# SAX is used to process the XML of the raw Wikipedia dump file.
# Simple SAX handler functions are what invokes the article handler
# functions in the article handler class.
#
# For each article, the article handler function process_article_text() is
# called to process the text of the article, and is passed the article title
# and full text, with entity expressions such as &nbsp; replaced appropriately.
# This function operates in two passes.  The first pass, performed by
# the article handler process_text_for_data(), extracts useful data, e.g.
# coordinates or links.  It returns True or False, indicating whether the
# second pass should operate.  The purpose of the second pass is to do
# processing involving the article text itself, e.g. counting up words.
# It is implemented by the article handler process_text_for_text().
# The default handler does two things:
#
# 1. Process the text, filtering out some junk
#    (see format_text_second_pass()).
# 2. Use process_source_text() to extract chunks of "actual
#    text" (as opposed to directives of various sorts), i.e. text that
#    is useful for constructing a language model that can be used
#    for classifying a document to find the most similar article.
#    Join together and then split into words.  Pass the generator
#    of words to the article handler process_text_for_words().
#  
# process_source_text() is is a generator that yields processed
# textual chunks containing only "actual text".  This function works by
# calling parse_simple_balanced_text() to parse the text into balanced chunks
# (text delimited by balanced braces or brackets, i.e. {...} or [...],
# or text without any braces or brackets), and then handling the chunks
# according to their type:
#
# -- if [[...]], use process_internal_link()
# -- if {{...}}, use process_template()
# -- if {|...|}, use process_table()
# -- if [...] but not [[...]], use process_external_link()
# -- else, return the text unchanged
#
# Each of the above functions is a generator that yields chunks of
# "actual text".  Different sorts of processing can be implemented here.
# Note also that a similar structure can and probably should be
# implemented in process_text_for_data().
#
# As mentioned above, the chunks are concatenated before being split again.
# Concatentation helps in the case of article text like
#
# ... the [[latent variable|hidden value]]s ...
#
# which will get processed into chunks
#
# '... the ', 'latent variable hidden value', 's ...'
#
# Concatenating again will generate a single word "values".
#
# The resulting text is split to find words, using split_text_into_words().
# This splits on whitespace, but is a bit smarter; it also ignores
# punctuation such as periods or commas that occurs at the end of words,
# as well as parens and quotes at word boundaries, and ignores entirely
# any word with a colon in the middle (a likely URL or other directive that
# has slipped through), and separates on # and _ (which may occur in
# internal links or such).
#
# Note also that prior to processing the text for data and again prior to
# processing the text for words, it is formatted to make it nicer to
# process and to get rid of certain sorts of non-useful text.  This is
# implemented in the functions format_text_first_pass() and
# format_text_second_pass(), respectively.  This includes things like:
#
# -- removing comments
# -- removing <math>...</math> sections, which contain differently-formatted
#    text (specifically, in TeX format), which will screw up processing
#    of templates and links and contains very little useful text
# -- handling certain sorts of embedded entity expressions, e.g. cases
#    where &amp;nbsp; appears in the raw dump file.  This corresponds to
#    cases where &nbsp; appears in the source text of the article.
#    Wikipedia's servers process the source text into HTML and then spit
#    out the HTML, which gets rendered by the browser and which will
#    handle embedded entity expressions (e.g. convert &nbsp; into a
#    non-breaking-space character).  Note that something like &nbsp;
#    appears directly in the raw dump file only when a literal
#    non-breaking-space character appears in the article source text.
# -- handling embedded HTML expressions like <sup>2</sup>, where < appears
#    in the raw dump as &lt;.  These get processed by the user's browser.
#    We handle them in a simple fashion, special-casing <br> and <ref>
#    into whitespace and just removing all the others.
# -- removing === characters in headers like ===Introduction===
# -- removing multiple single-quote characters, which indicate boldface
#    or italics

##### About generators

# The code in this program relies heavily on generators, a special type of
# Python function.  The following is a quick intro for programmers who
# might not be familiar with generators.
#
# A generator is any function containing a "yield foo" expression.
# Logically speaking, a generator function returns multiple values in
# succession rather than returning a single value.  In the actual
# implementation, the result of calling a generator function is a
# generator object, which can be iterated over in a for loop, list
# comprehension or generator expression, e.g.
#
# 1. The following uses a for loop to print out the objects returned by
#    a generator function.
#
# for x in generator():
#   print x
#
# 2. The following returns a list resulting from calling a function fun() on
#    each object returned by a generator function.
# 
# [fun(x) for x in generator()]
#
# 3. The following returns another generator expression resulting from
#    calling a function fun() on each object returned by a generator function.
#
# (fun(x) for x in generator())
#
# There are some subtleties involved in writing generators:
#
# -- A generator can contain a "return" statement, but cannot return a value.
#    Returning from a generator, whether explicitly through a "return"
#    statement or implicitly by falling off the end of the function, triggers
#    a "raise StopIteration" statement.  This terminates the iteration loop
#    over the values returned by the generator.
# -- Chaining generators, i.e. calling one generator inside of another, is
#    a bit tricky.  If you have a generator function generator(), and you
#    want to pass back the values from another generator function generator2(),
#    you cannot simply call "return generator2()", since generators can't
#    return values.  If you just write "generator2()", nothing will happen;
#    the value from generator2() gets discarded.  So you usually have to
#    write a for loop:
#
#    for foo in generator2():
#      yield foo
#
#    Note that "return generator2()" *will* work inside of a function that is
#    not a generator, i.e. has no "yield" statement in it.


############################################################################
#                                    Code                                  #
############################################################################

import sys, re
from optparse import OptionParser

from xml.sax import make_parser
from xml.sax.handler import ContentHandler

# Debug level; if non-zero, output lots of extra information about how
# things are progressing.  If > 1, even more info.
debug = 0

# If true, print out warnings about strangely formatted input
show_warnings = False

#######################################################################
#                 Chunk text into balanced sections                   #
#######################################################################

### Return chunks of balanced text, for use in handling template chunks
### and such.  The chunks consist either of text without any braces or
### brackets, chunks consisting of a brace or bracket and all the text
### up to and including the matching brace or bracket, or lone unmatched
### right braces/brackets.  Currently, if a chunk is closed with the
### wrong type of character (brace when bracket is expected or vice-versa),
### we still treat it as the closing character, but output a warning.
###
### In addition, some of the versions below will split off additional
### characters if they occur at the top level (e.g. pipe symbols or
### newlines).  In these cases, if such a character occurs, three
### successive chunks will be seen: The text up to but not including the
### dividing character, a chunk with only the character, and a chunk
### with the following text.  Note that if the dividing character occurs
### inside of bracketed or braced text, it will not divide the text.
### This way, for example, arguments of a template or internal link
### (which are separated by a pipe symbol) can be sectioned off without
### also sectioning off the arguments inside of nested templates or
### internal links.  Then, the parser can be called recursively if
### necessary to handle such expressions.

# Return braces and brackets separately from other text.
simple_balanced_re = re.compile(r'[^{}\[\]]+|[{}\[\]]')

# Return braces, brackets and pipe symbols separately from other text.
balanced_pipe_re = re.compile(r'[^{}\[\]|]+|[{}\[\]|]')

# Return braces, brackets, and newlines separately from other text.
# Useful for handling Wikipedia tables, denoted with {| ... |}.
balanced_table_re = re.compile(r'[^{}\[\]\n]+|[{}\[\]\n]')

left_match_chars = {'{':'}', '[':']'}
right_match_chars = {'}':'{', ']':'['}

def parse_balanced_text(textre, text):
  '''Parse text in TEXT containing balanced expressions surrounded by single
or double braces or brackets.  This is a generator; it successively yields
chunks of text consisting either of sections without any braces or brackets,
or balanced expressions delimited by single or double braces or brackets, or
unmatched single or double right braces or brackets.  TEXTRE is used to
separate the text into chunks; it can be used to separate out additional
top-level separators, such as vertical bar.'''
  strbuf = []
  parenlevel = 0
  prevstring = "(at beginning)"
  leftmatches = []
  for string in textre.findall(text):
    if string in right_match_chars:
      if parenlevel == 0:
        warning("Nesting level would drop below 0; string = %s, prevstring = %s" % (string, prevstring.replace('\n','\\n')))
        yield string
      else:
        strbuf.append(string)
        assert len(leftmatches) == parenlevel
        should_left = right_match_chars[string]
        the_left = leftmatches[-1]
        if should_left != the_left:
          warning("Non-matching brackets: Saw %s, expected %s; prevstring = %s" % (string, left_match_chars[the_left], prevstring.replace('\n','\\n')))
        parenlevel -= 1
        leftmatches = leftmatches[:-1]
        if parenlevel == 0:
          yield ''.join(strbuf)
          strbuf = []
    else:
      if string in left_match_chars:
        parenlevel += 1
        leftmatches.append(string)
      if parenlevel > 0:
        strbuf.append(string)
      else:
        yield string
    prevstring = string

def parse_simple_balanced_text(text):
  '''Parse text in TEXT containing balanced expressions surrounded by single
or double braces or brackets.  This is a generator; it successively yields
chunks of text consisting either of sections without any braces or brackets,
or balanced expressions delimited by single or double braces or brackets, or
unmatched single or double right braces or brackets.'''
  return parse_balanced_text(simple_balanced_re, text)

#######################################################################
###                        Utility functions                        ###
#######################################################################

def uniprint(text):
  '''Print Unicode text in UTF-8, so it can be output without errors'''
  print text.encode("utf-8")

def warning(text):
  '''Output a warning, formatting into UTF-8 as necessary'''
  if show_warnings:
    uniprint("Warning: %s" % text)

# Given a table with values that are numbers, output the table, sorted
# on the numbers from bigger to smaller.
def output_reverse_sorted_table(table):
  for x in sorted(table.items(), key=lambda x:x[1], reverse=True):
    uniprint("%s = %s" % (x[0], x[1]))
  
def find_template_params(args, strip_values):
  '''Find the parameters specified in template arguments, i.e. the arguments
to a template that are of the form KEY=VAL.  Given the arguments ARGS of a
template, return a tuple (HASH, NONPARAM) where HASH is the hash table of
KEY->VAL parameter mappings and NONPARAM is a list of all the remaining,
non-parameter arguments.  If STRIP_VALUES is true, strip whitespace off the
beginning and ending of values in the hash table (keys will always be
lowercased and have the whitespace stripped from them).'''
  hash = {}
  nonparam_args = []
  for arg in args:
    m = re.match(r'(?s)(.*?)=(.*)', arg)
    if m:
      key = m.group(1).strip().lower()
      value = m.group(2)
      if strip_values:
        value = value.strip()
      hash[key] = value
    else:
      #uniprint("Unable to process template argument %s" % arg)
      nonparam_args.append(arg) 
  return (hash, nonparam_args)

def get_macro_args(macro):
  '''Split macro MACRO (either a {{...}} or [[...]] expression)
by arguments (separated by | occurrences), but intelligently so that
arguments in nested macros are not also sectioned off.  In the case
of a template, i.e. {{...}}, the first "argument" returned will be
the template type, e.g. "Cite web" or "Coord".  At least one argument
will always be returned (in the case of an empty macro, it will be
the string "empty macro"), so that code that parses templates need
not worry about crashing on these syntactic errors.'''

  macroargs = [foo for foo in
              parse_balanced_text(balanced_pipe_re, macro[2:-2])
              if foo != '|']
  if not macroargs:
    warning("Strange macro with no arguments: %s" % macroargs)
    return ['empty macro']
  return macroargs

#######################################################################
#                         Process source text                         #
#######################################################################

# Handle the text of a given article.  Yield chunks of processed text.

class SourceTextHandler(object):
  def process_internal_link(self, text):
    yield text
    
  def process_template(self, text):
    yield text
    
  def process_table(self, text):
    yield text
    
  def process_external_link(self, text):
    yield text
    
  def process_text_chunk(self, text):
    yield text

  def process_source_text(self, text):
    # Look for all template and link expressions in the text and do something
    # sensible with them.  Yield the resulting text chunks.  The idea is that
    # when the chunks are joined back together, we will get raw text that can
    # be directly separated into words, without any remaining macros (templates,
    # internal or external links, tables, etc.) and with as much extraneous
    # junk (directives of various sorts, instead of relevant text) as possible
    # filtered out.  Note that when we process macros and extract the relevant
    # text from them, we need to recursively process that text.
  
    if debug > 1: uniprint("Entering process_source_text: [%s]" % text)
  
    for foo in parse_simple_balanced_text(text):
      if debug > 1: uniprint("parse_simple_balanced_text yields: [%s]" % foo)
  
      if foo.startswith('[['):
        gen = self.process_internal_link(foo)
  
      elif foo.startswith('{{'):
        gen = self.process_template(foo)
  
      elif foo.startswith('{|'):
        gen = self.process_table(foo)
  
      elif foo.startswith('['):
        gen = self.process_external_link(foo)
  
      else:
        gen = self.process_text_chunk(foo)
  
      for chunk in gen:
        if debug > 1: uniprint("process_source_text yields: [%s]" % chunk)
        yield chunk
  
# An article source-text handler that recursively processes text inside of
# macros.  Doesn't split templates, links or tables according to arguments
# or fields.

class RecursiveSourceTextHandler(SourceTextHandler):
  def process_internal_link(self, text):
    return self.process_source_text(text[2:-2])
    
  def process_template(self, text):
    return self.process_source_text(text[2:-2])
    
  def process_table(self, text):
    return self.process_source_text(text[2:-2])
    
  def process_external_link(self, text):
    return self.process_source_text(text[1:-1])
    
#######################################################################
#                     Process text for coordinates                    #
#######################################################################

# Accumulate a table of all the templates with coordinates in them, along
# with counts.
templates_with_coords = {}

# Accumulate a table of all templates, with counts.
all_templates = {}

def safe_float(x):
  '''Convert a string to floating point, but don't crash on errors;
instead, output a warning.'''
  try:
    return float(x)
  except:
    x = x.strip()
    if x:
      warning("Expected number, saw %s" % x)
    return 0.

def convert_dms(nsew, d, m, s):
  '''Convert a multiplier (1 or N or E, -1 for S or W) and degree/min/sec
values into a decimal +/- latitude or longitude.'''
  return nsew*(safe_float(d) + safe_float(m)/60. + safe_float(s)/3600.)

convert_ns = {'N':1, 'S':-1}
convert_ew = {'E':1, 'W':-1}

# Utility function for get_lat_long().  Extract out either latitude or
# longitude from a template of type TEMPTYPE with arguments ARGS.
# LAT is a string, either 'lat' or 'long'.  NS is a string, either 'ns'
# (for latitude) or 'ew' (for longitude).  CONVERT is a table mapping
# NSEW directions into a multiplier +1 or -1, coming from either of the
# global variables convert_ns (for latitude) or convert_ew (for longitude).
def get_lat_long_1(temptype, args, lat, ns, convert):
  if '%sd' % lat not in args:
    warning("No %sd seen for template type %s" % (lat, temptype))
  d = args.get('%sd' % lat, 0)
  m = args.get('%sm' % lat, 0)
  s = args.get('%ss' % lat, 0)
  latns = '%s%s' % (lat, ns)
  latNS = '%s%s' % (lat, ns.upper())
  if latns not in args:
    warning("No %s seen for template type %s" % (latNS, temptype))
    latmult = 1
  else:
    latmult = convert.get(args[latns], 0)
    if latmult == 0:
      warning("%s for template type %s has bad value %s" %
               (latNS, temptype, args[latns]))
  return convert_dms(latmult, d, m, s)

def get_lat_long(temptype, args):
  '''Given a template of type TEMPTYPE with arguments ARGS, assumed to have
a latitude/longitude specification in it, extract out and return a tuple
of decimal (latitude, longitude) values.'''
  lat = get_lat_long_1(temptype, args, 'lat', 'ns', convert_ns)
  long = get_lat_long_1(temptype, args, 'long', 'ew', convert_ew)
  return (lat, long)

# Utility function for get_coord().  Extract out the latitude or longitude
# values out of a Coord structure.  Return a tuple (OFFSET, VAL) for decimal
# latitude or longitude VAL and OFFSET indicating the offset of the next
# argument after the arguments used to produce the value.
def get_coord_1(args, nsew, convert_nsew):
  if args[1] in nsew:
    d = args[0]; m = 0; s = 0; i = 1
  elif args[2] in nsew:
    d = args[0]; m = args[1]; s = 0; i = 2
  elif args[3] in nsew:
    d = args[0]; m = args[1]; s = args[2]; i = 3
  else: return (0, args[0])
  return (i+1, convert_dms(convert_nsew[args[i]], d, m, s))

def get_coord(temptype, args):
  '''Parse a Coord template and return a tuple (lat,long) for latitude and
longitude.  TEMPTYPE is the template name.  ARGS is the raw arguments for
the template.  Coord templates are one of four types:

{{Coord|44.112|-87.913}}
{{Coord|44.112|N|87.913|W}}
{{Coord|44|6.72|N|87|54.78|W}}
{{Coord|44|6|43.2|N|87|54|46.8|W}}

Note that all four of the above are equivalent.

In addition, extra "template" or "coordinate" parameters can be given.
The template parameters mostly control display and are basically uninteresting.
However, the coordinate parameters contain lots of potentially useful
information that can be used as features or whatever.  See
http://en.wikipedia.org/wiki/Template:Coord for more information.

The types of coordinate parameters are:

type: country, city, city(###) where ### is the population, isle, river, etc.
      Very useful feature; can also be used to filter uninteresting info as
      some articles will have multiple coordinates in them.
scale: indicates the map scale (note that type: also specifies a default scale)
dim: diameter of viewing circle centered on coordinate (gives some sense of
     how big the feature is)
region: the "political region for terrestrial coordinates", i.e. the country
        the coordinate is in, as a two-letter ISO 3166-1 alpha-2 code, or the
        country plus next-level subdivision (state, province, etc.)
globe: which planet or satellite the coordinate is on (esp. if not the Earth)
'''
  if debug > 0: uniprint("Passed in args %s" % args)
  # Filter out optional "template arguments", add a bunch of blank arguments
  # at the end to make sure we don't get out-of-bounds errors in
  # get_coord_1()
  args = [x for x in args if '=' not in x] + ['','','','','','']
  (i, lat) = get_coord_1(args, ('N','S'), convert_ns)
  (_, long) = get_coord_1(args[i:], ('E','W'), convert_ew)
  return (lat, long)

class ExtractCoordinatesFromSource(SourceTextHandler):
  '''Given the article text TEXT of an article (in general, after first-
stage processing), extract coordinates out of templates that have coordinates
in them (Infobox, Coord, etc.).  Record each coordinate into COORD.

We don't recursively process text inside of templates or links.  If we want
to do that, change this class to inherit from RecursiveSourceTextHandler.

See process_article_text() for a description of the formatting that is
applied to the text before being sent here.'''

  coords = []

  def process_template(self, text):
    # Look for a Coord, Infobox, etc. template that may have coordinates in it
    lat = long = None
    if debug > 0: uniprint("Enter process_template: [%s]" % text)
    tempargs = get_macro_args(text)
    temptype = tempargs[0].strip()
    if debug > 0: uniprint("Template type: %s" % temptype)
    lowertemp = temptype.lower()
    # Look for a coordinate template
    if lowertemp in ('coord', 'coor d', 'coor dm', 'coor dms',
                     'coor dec', 'coorheader') \
        or lowertemp.startswith('geolinks') \
        or lowertemp.startswith('mapit'):
      (lat, long) = get_coord(temptype, tempargs[1:])
    else:
      # Look for any other template with a 'latd' parameter.  Usually
      # these will be Infobox-type templates.  Possibly we should only
      # look at templates whose lowercased name begins with "infobox".
      (paramshash, _) = find_template_params(tempargs[1:], True)
      if 'latd' in paramshash:
        templates_with_coords[lowertemp] = \
          templates_with_coords.get(lowertemp, 0) + 1
        (lat, long) = get_lat_long(temptype, paramshash)
    if lat or long:
      if debug > 0: uniprint("Saw coordinate %s,%s in template type %s" %
                (lat, long, temptype))
      self.coords.append((lowertemp,lat,long))
    yield text

#######################################################################
#                         Process text for words                      #
#######################################################################

# For a "macro" (e.g. internal link or template) with arguments, and
# a generator that returns the interesting arguments separately, process
# each of these arguments into chunks, join the chunks of an argument back
# together, and join the processed arguments, with spaces separating them.
# The idea is that for something like
#
#   The [[latent variable|hidden node]]s are ...
#
# We will ultimately get something like
#
#   The latent variable hidden nodes are ...
#
# after joining chunks. (Even better would be to correct handle something
# like
#
#    The sub[[latent variable|node]]s are ...
#
# into
#
#    The latent variable subnodes are ...
#
# But that's a major hassle, and such occurrences should be rare.)

# Process an internal link into separate chunks for each interesting
# argument.  Yield the chunks.  They will be recursively processed, and
# joined by spaces.
def yield_internal_link_args(text):
  tempargs = get_macro_args(text)
  m = re.match(r'(?s)\s*([a-zA-Z0-9_]+)\s*:(.*)', tempargs[0])
  if m:
    # Something like [[Image:...]] or [[wikt:...]] or [[fr:...]]
    namespace = m.group(1).lower()
    if namespace in ('image', 'file'):
      # For image links, filter out non-interesting args
      for arg in tempargs[1:]:
        # Ignore uninteresting args
        if re.match(r'thumb|left|(up)?right|[0-9+](\s*px)?$', arg.strip()): pass
        # For alt text, ignore the alt= but use the rest
        else:
          # Look for parameter spec
          m = re.match(r'(?s)\s*([a-zA-Z0-9_]+)\s*=(.*)', arg)
          if m:
            (param, value) = m.groups()
            if param.lower() == 'alt':
              yield value
            # Skip other parameters
          # Use non-parameter args
          else: yield arg
    elif len(namespace) == 2 or len(namespace) == 3 or namespace == 'simple':
      # A link to the equivalent page in another language; foreign words
      # probably won't help for word matching.  However, this might be
      # useful in some other way.
      pass
    else:
      # Probably either a category or wikt (wiktionary).
      # The category is probably useful; the wiktionary entry maybe.
      # In both cases, go ahead and use.
      link = m.group(2)
      # Skip "Appendix:" in "wikt:Appendix"
      m = re.match(r'(?s)\s*[Aa]ppendix\s*:(.*)', link)
      if m: yield m.group(1)
      else: yield link
      for arg in tempargs[1:]: yield arg
  else:
    # For textual internal link, use all arguments
    for chunk in tempargs: yield chunk

# Process a template into separate chunks for each interesting
# argument.  Yield the chunks.  They will be recursively processed, and
# joined by spaces.
def yield_template_args(text):
  # For a template, do something smart depending on the template.
  if debug > 1: uniprint("yield_template_args called with: %s" % text)

  # OK, this is a hack, but a useful one.  There are lots of templates that
  # look like {{Emancipation Proclamation draft}} or
  # {{Time measurement and standards}} or similar that are useful as words.
  # So we look for templates without arguments that look like this.
  # Note that we require the first word to have at least two letters, so
  # we filter out things like {{R from related word}} or similar redirection-
  # related indicators.  Note that similar-looking templates that begin with
  # a lowercase letter are sometimes useful like {{aviation lists}} or
  # {{global warming}} but often are non-useful things like {{de icon}} or
  # {{nowrap begin}} or {{other uses}}.  Potentially we could be smarter
  # about this.
  if re.match(r'{{[A-Z][a-z]+ [A-Za-z ]+}}$', text):
    yield text[2:-2]
    return

  tempargs = get_macro_args(text)
  if debug > 1: uniprint("template args: %s" % tempargs)
  temptype = tempargs[0].strip().lower()

  if debug > 0:
    all_templates[temptype] = all_templates.get(temptype, 0) + 1

  # Extract the parameter and non-parameter arguments.
  (paramhash, nonparam) = find_template_params(tempargs[1:], False)

  # For certain known template types, use the values from the interesting
  # parameter args and ignore the others.  For other template types,
  # assume the parameter are uninteresting.
  if re.match(r'v?cite', temptype):
    # A citation, a very common type of template.
    for (key,value) in paramhash.items():
      # A fairly arbitrary list of "interesting" parameters.
      if re.match(r'(last|first|authorlink)[1-9]?$', key) or \
         re.match(r'(author|editor)[1-9]?-(last|first|link)$', key) or \
         key in ('coauthors', 'others', 'title', 'trans_title',
                 'quote', 'work', 'contribution', 'chapter', 'trans_chapter',
                 'series', 'volume'):
        yield value
  elif re.match(r'infobox', temptype):
    # Handle Infoboxes.
    for (key,value) in paramhash.items():
      # A fairly arbitrary list of "interesting" parameters.
      if key in ('name', 'fullname', 'nickname', 'altname', 'former',
                 'alt', 'caption', 'description', 'title', 'title_orig',
                 'image_caption', 'imagecaption', 'map_caption', 'mapcaption',
                 # Associated with states, etc.
                 'motto', 'mottoenglish', 'slogan', 'demonym', 'capital',
                 # Add more here
                 ):
        yield value

  # For other template types, ignore all parameters and yield the
  # remaining arguments.
  # Yield any non-parameter arguments.
  for arg in nonparam:
    yield arg

# Process a table into separate chunks.  Unlike code for processing
# internal links, the chunks should have whitespace added where necessary.
def yield_table_chunks(text):
  if debug > 1: uniprint("Entering yield_table_chunks: [%s]" % text)

  # Given a single line or part of a line, and an indication (ATSTART) of
  # whether we just saw a beginning-of-line separator, split on within-line
  # separators (|| or !!) and remove table directives that can occur at
  # the beginning of a field (terminated by a |).  Yield the resulting
  # arguments as chunks.
  def process_table_chunk_1(text, atstart):
    for arg in re.split(r'(?:\|\||!!)', text):
      if atstart:
        m = re.match('(?s)[^|]*\|(.*)', arg)
        if m:
          yield m.group(1) + ' '
          continue
      yield arg
      atstart = True

  # Just a wrapper function around process_table_chunk_1() for logging
  # purposes.
  def process_table_chunk(text, atstart):
    if debug > 1: uniprint("Entering process_table_chunk: [%s], %s" % (text, atstart))
    for chunk in process_table_chunk_1(text, atstart):
      if debug > 1: uniprint("process_table_chunk yields: [%s]" % chunk)
      yield chunk

  # Strip off {| and |}
  text = text[2:-2]
  ignore_text = True
  at_line_beg = False

  # Loop over balanced chunks, breaking top-level text at newlines.
  # Strip out notations like | and |- that separate fields, and strip out
  # table directives (e.g. which occur after |-).  Pass the remainder to
  # process_table_chunk(), which will split a line on within-line separators
  # (e.g. || or !!) and strip out directives.
  for arg in parse_balanced_text(balanced_table_re, text):
    if debug > 1: uniprint("parse_balanced_text(balanced_table_re) yields: [%s]" % arg)
    # If we see a newline, reset the flags and yield the newline.  This way,
    # a whitespace will always be inserted.
    if arg == '\n':
      ignore_text = False
      at_line_beg = True
      yield arg
    if at_line_beg:
      if arg.startswith('|-'):
        ignore_text = True
        continue
      elif arg.startswith('|') or arg.startswith('!'):
        arg = arg[1:]
        if arg and arg[0] == '+': arg = arg[1:]
        # The chunks returned here are separate fields.  Make sure whitespace
        # separates them.
        yield ' '.join(process_table_chunk(arg, True))
        continue
    elif ignore_text: continue
    # Add whitespace between fields, as above.
    yield ' '.join(process_table_chunk(arg, False))

# Given raw text, split it into words, filtering out punctuation, and
# yield the words.  Also ignore words with a colon in the middle, indicating
# likely URL's and similar directives.
def split_text_into_words(text):
  # This regexp splits on whitespace, but also handles the following cases:
  # 1. Any of , ; . etc. at the end of a word
  # 2. Parens or quotes in words like (foo) or "bar"
  for word in re.split('[,;."):]*\s+[("]*', text):
    # Sometimes URL's or other junk slips through.  Much of this junk has
    # a colon in it and little useful stuff does.
    if ':' not in word:
      # Handle things like "Two-port_network#ABCD-parameters".  Do this after
      # filtering for : so URL's don't get split up.
      for word2 in re.split('[#_]', word):
        yield word2

# Extract "useful" text (generally, text that will be seen by the user,
# or hidden text of similar quality) and yield up chunks.

class ExtractUsefulText(SourceTextHandler):
  def process_and_join_arguments(self, args_of_macro):
    return ' '.join(''.join(self.process_source_text(chunk))
                    for chunk in args_of_macro)

  def process_internal_link(self, text):
    '''Process an internal link into chunks of raw text and yield them.'''
    # Find the interesting arguments of an internal link and join
    # with spaces.
    yield self.process_and_join_arguments(yield_internal_link_args(text))
  
  def process_template(self, text):
    '''Process a template into chunks of raw text and yield them.'''
    # Find the interesting arguments of a template and join with spaces.
    yield self.process_and_join_arguments(yield_template_args(text))
  
  def process_table(self, text):
    '''Process a table into chunks of raw text and yield them.'''
    for bar in yield_table_chunks(text):
      if debug > 1: uniprint("process_table yields: [%s]" % bar)
      for baz in self.process_source_text(bar):
        yield baz
  
  def process_external_link(self, text):
    '''Process an external link into chunks of raw text and yield them.'''
    # For an external link, use the anchor text of the link, if any
    splitlink = re.split(r'\s+', text[1:-1], 1)
    if len(splitlink) == 2:
      (link, linktext) = splitlink
      for chunk in self.process_source_text(linktext):
        yield chunk
  

#######################################################################
#               Formatting text to make processing easier             #
#######################################################################

# Process the text in various ways in preparation for extracting data
# from the text.
def format_text_first_pass(text):
  # Remove all comments from the text; may contain malformed stuff of
  # various sorts, and generally stuff we don't want to index
  (text, _) = re.subn(r'(?s)<!--.*?-->', '', text)

  # Get rid of all text inside of <math>...</math>, which is in a different
  # format (TeX), and mostly non-useful.
  (text, _) = re.subn(r'(?s)<math>.*?</math>', '', text)

  # Convert occurrences of &nbsp; and &ndash; and similar, which occur often
  # (note that SAX itself should handle entities like this; occurrences that
  # remain must have had the ampersand converted to &amp;)
  (text, _) = re.subn(r'&nbsp;', ' ', text)
  (text, _) = re.subn(r'&thinsp;', ' ', text)
  (text, _) = re.subn(r'&[nm]dash;', '-', text)
  (text, _) = re.subn(r'&minus;', '-', text)
  (text, _) = re.subn(r'&amp;', '&', text)
  (text, _) = re.subn(r'&times;', '*', text)
  (text, _) = re.subn(r'&hellip;', '...', text)
  (text, _) = re.subn(r'&lt;', '<', text)
  (text, _) = re.subn(r'&gt;', '>', text)

  return text

# Process the text in various ways in preparation for extracting
# the words from the text.
def format_text_second_pass(text):
  # Convert breaks into newlines
  (text, _) = re.subn(r'<br( +/)?>', r'\n', text)

  # Remove references, but convert to whitespace to avoid concatenating
  # words outside and inside a reference together
  (text, _) = re.subn(r'(?s)<ref.*?>', ' ', text)

  # Another hack: Inside of <gallery>...</gallery>, there are raw filenames.
  # Get rid of.

  def process_gallery(text):
    # Split on gallery blocks (FIXME, recursion not handled).  Putting a
    # group around the split text ensures we get it returned along with the
    # other text.
    chunks = re.split(r'(?s)(<gallery.*?>.*?</gallery>)', text)
    for chunk in chunks:
      # If a gallery, extract the stuff inside ...
      m = re.match(r'^(?s)<gallery.*?>(.*?)</gallery>$', chunk)
      if m:
        chunk = m.group(1)
        # ... then remove files and images, but keep any text after |
        (chunk, _) = re.subn(r'(?m)^(?:File|Image):[^|\n]*$', '', chunk)
        (chunk, _) = re.subn(r'(?m)^(?:File|Image):[^|\n]*\|(.*)$',
                             r'\1', chunk)
      yield chunk
  
  text = ''.join(process_gallery(text))
  
  # Remove remaining HTML codes from the text
  (text, _) = re.subn(r'(?s)<.*?>', '', text)

  # Remove multiple sequences of quotes (indicating boldface or italics)
  (text, _) = re.subn(r"''+", '', text)
 
  # Remove beginning-of-line markers indicating indentation, lists, headers,
  # etc.
  (text, _) = re.subn(r"(?m)^[*#:=]+", '', text)

  # Remove end-of-line markers indicating headers (e.g. ===Introduction===)
  (text, _) = re.subn(r"(?m)=+$", r'', text)

  return text

#######################################################################
#                SAX handler for processing raw dump files            #
#######################################################################

class WikipediaSaxHandler(ContentHandler):
  '''SAX handler for processing Wikipedia documents.  Note that SAX is a
simple interface for handling XML in a serial fashion (as opposed to a
DOM-type interface, which reads the entire XML file into memory and allows
it to be dynamically manipulated).  Given the size of the XML dump file
(around 50 or 100 GB uncompressed), we can't read it all into memory.'''
  def __init__(self, output_handler):
    self.intext = False
    self.intitle = False
    self.curtitle = None
    self.curtext = None
    self.output_handler = output_handler
    
  def startElement(self, name, attrs):
    '''Handler for beginning of XML element.'''
    if debug > 1: uniprint("startElement() saw %s/%s" % (name, attrs))
    if name == 'title':
      self.intitle = True
      self.curtitle = ""
    elif name == 'text':
      self.intext = True
      self.curtext = []

  def characters(self, text):
    '''Handler for chunks of text.  Accumulate all adjacent chunks.  When
the end element </text> is seen, process_article_text() will be called on the
combined chunks.'''
    if debug > 1: uniprint("characters() saw %s" % text)
    if self.intitle:
      self.curtitle += text
    elif self.intext:
      self.curtext.append(text)
 
  def endElement(self, name):
    '''Handler for end of XML element.'''
    if name == 'title':
      # If we saw a title, note it
      self.intitle = False
    elif name == 'text':
      # If we saw the end of the article text, join all the text chunks
      # together and call process_article_text() on it.
      self.intext = False
      self.output_handler.process_article_text(self.curtitle,
                                               ''.join(self.curtext))
      self.curtext = None
 
#######################################################################
#                           Article handlers                          #
#######################################################################



### Default handler class for processing article text.  Subclass this to
### implement your own handlers.
class ArticleHandler(object):
  # Process the text of article TITLE, with text TEXT.  The default
  # implementation does the following:
  #
  # 1. Remove comments, math, and other unuseful stuff.
  # 2. If article is a redirect, call self.process_redirect() to handle it.
  # 3. Else, call self.process_text_for_data() to extract data out.
  # 4. If that handler returned True, call self.process_text_for_text()
  #    to do processing of the text itself (e.g. for words).

  def process_article_text(self, title, text):
  
    if debug > 0:
      uniprint("Article title: %s" % title)
      uniprint("Original article text:\n%s" % text)
  
    ### Preliminary processing of text, removing stuff unuseful even for
    ### extracting data.
  
    text = format_text_first_pass(text)
  
    ### Look to see if the article is a redirect
  
    m = re.match('#REDIRECT \[\[(.*?)\]\]', text)
    if m:
      self.process_redirect(title, m.group(1))
      # NOTE: There may be additional templates specified along with a
      # redirection page, typically something like {{R from misspelling}}
      # that gives the reason for the redirection.  Currently, we ignore
      # such templates.
      return
  
    ### Extract the data out of templates; if it returns True, also process
    ### text for words
  
    if self.process_text_for_data(title, text):
      self.process_text_for_text(title, text)

  # Process the text itself, e.g. for words.  Default implementation does
  # nothing.
  def process_text_for_text(self, title, text):
    pass

  # Process an article that is a redirect.  Default implementation does
  # nothing.

  def process_redirect(self, title, redirtitle):
    pass

  # Process the text and extract data.  Return True if further processing of
  # the article should happen. (Extracting the real text in
  # process_text_for_text() currently takes up the vast majority of running
  # time, so skipping it is a big win.)
  #
  # Default implementation just returns True.

  def process_text_for_data(self, title, text):
    return True

  def finish_processing(self):
    pass



### Default handler class for processing article text, including returning
### "useful" text (what the Wikipedia user sees, plus similar-quality
### hidden text).
class ArticleHandlerForUsefulText(ArticleHandler):
  # Process the text itself, e.g. for words.  Input it text that has been
  # preprocessed as described above (remove comments, etc.).  Default
  # handler does two things:
  #
  # 1. Further process the text (see format_text_second_pass())
  # 2. Use process_source_text() to extract chunks of useful
  #    text.  Join together and then split into words.  Pass the generator
  #    of words to self.process_text_for_words().

  def process_text_for_text(self, title, text):  
    # Now process the text in various ways in preparation for extracting
    # the words from the text
    text = format_text_second_pass(text)
    # Now process the resulting text into chunks.  Join them back together
    # again (to handle cases like "the [[latent variable]]s are ..."), and
    # split to find words.
    self.process_text_for_words(
      title, split_text_into_words(
               ''.join(ExtractUsefulText().process_source_text(text))))

  # Process the real words of the text of an article.  Default implementation
  # does nothing.

  def process_text_for_words(self, title, word_generator):
    pass



# Does what processwiki.py used to do.  Basically just prints out the info
# passed in for redirects and article words; as for the implementation of
# process_text_for_data(), uses ExtractCoordinatesFromSource() to extract
# coordinates, and outputs all the coordinates seen.  Always returns True.

class PrintWordsAndCoords(ArticleHandlerForUsefulText):
  def process_text_for_words(self, title, word_generator):
    uniprint("Article title: %s" % title)
    for word in word_generator:
      if debug > 0: uniprint("Saw word: %s" % word)
      else: uniprint("%s" % word)

  def process_redirect(self, title, redirtitle):
    uniprint("Article title: %s" % title)
    uniprint("Redirect to: %s" % redirtitle)

  def process_text_for_data(self, title, text):
    handler = ExtractCoordinatesFromSource()
    for foo in handler.process_source_text(text): pass
    for (temptype,lat,long) in handler.coords:
      uniprint("Article coordinates: %s,%s" % (lat, long))
    return True

  def finish_processing(self):
    ### Output all of the templates that were seen with coordinates in them,
    ### along with counts of how many times each template was seen.
    if debug > 0:
      print("Templates with coordinates:")
      output_reverse_sorted_table(templates_with_coords)
      
      print("All templates:")
      output_reverse_sorted_table(all_templates)
  
      print "Notice: ending processing"



# Handler to output count information on words.  Only processes articles
# with coordinates in them, and only selects the first coordinate seen.
# Outputs the article title and coordinates.  Then computes the count of
# each word in the article text, after filtering text for "actual text"
# (as opposed to directives etc.), and outputs the counts.

class GetCoordsAndCounts(ArticleHandlerForUsefulText):
  def process_text_for_words(self, title, word_generator):
    wordhash = {}
    for word in word_generator:
      if word: wordhash[word] = wordhash.get(word, 0) + 1
    output_reverse_sorted_table(wordhash)

  def process_text_for_data(self, title, text):
    handler = ExtractCoordinatesFromSource()
    for foo in handler.process_source_text(text): pass
    if len(handler.coords) > 0:
      # Prefer a coordinate specified using {{Coord|...}} or similar to
      # a coordinate in an Infobox, because the latter tend to be less
      # accurate.
      for (temptype, lat, long) in handler.coords:
        if temptype.startswith('coor'):
        uniprint("Article title: %s" % title)
        uniprint("Article coordinates: %s,%s" % (lat, long))
        return True
      (temptype, lat, long) = handler.coords[0]
      uniprint("Article title: %s" % title)
      uniprint("Article coordinates: %s,%s" % (lat, long))
      return True
    else: return False

# Handler to output link information as well as coordinate information.
# Note that a link consists of two parts: The surface text and the article
# name.  For all links, we keep track of all the possible articles for a
# given surface text and their counts.  We also count all of the incoming
# links to an article (can be used for computing prior probabilities of
# an article).

# Count number of incoming links for articles
incoming_link_count = {}

# Map surface names to a hash that maps articles to counts
surface_map = {}

# Set listing articles containing coordinates
coordinate_articles = set()

# Parse the result of a previous run of --coords-counts for articles with
# coordinates
def get_coordinates(filename):
  for line in open(filename):
    line = line.strip()
    m = re.match('Article title: (.*)', line)
    if m:
      title = m.group(1)
    elif re.match('Article coordinates: ', line):
      coordinate_articles.add(title)
    
class ProcessSourceForLinks(RecursiveSourceTextHandler):
  useful_text_handler = ExtractUsefulText()
  def process_internal_link(self, text):
    tempargs = get_macro_args(text)
    m = re.match(r'(?s)\s*([a-zA-Z0-9_]+)\s*:(.*)', tempargs[0])
    if m:
      # Something like [[Image:...]] or [[wikt:...]] or [[fr:...]]
      # For now, just skip them all; eventually, might want to do something
      # useful with some, e.g. categories
      pass
    else:
      article = tempargs[0]
      # Skip links to articles without coordinates
      if coordinate_articles and article not in coordinate_articles:
        pass
      else:
        surface = ''.join(self.useful_text_handler.
                          process_source_text(tempargs[-1]))
        incoming_link_count[article] = incoming_link_count.get(article, 0) + 1
        if surface not in surface_map:
          nested_surface_map = {}
          surface_map[surface] = nested_surface_map
        else:
          nested_surface_map = surface_map[surface]
        nested_surface_map[article] = nested_surface_map.get(article, 0) + 1
 
    # Also recursively process all the arguments for links, etc.
    return self.process_source_text(text[2:-2])

class FindLinks(ArticleHandler):
  def process_text_for_data(self, title, text):
    handler = ProcessSourceForLinks()
    for foo in handler.process_source_text(text): pass
    return False

  def finish_processing(self):
    print "------------------ Count of incoming links: ---------------"
    output_reverse_sorted_table(incoming_link_count)
  
    print "==========================================================="
    print "==========================================================="
    print "==========================================================="
    print ""
    for (surface,map) in surface_map.items():
      uniprint("-------- Surface->article for %s: " % surface)
      output_reverse_sorted_table(map)



#######################################################################
#                                Main code                            #
#######################################################################


def main_process_input(wiki_handler):
  if debug > 0: print "Notice: beginning Wikipedia parsing"

  ### Create the SAX parser and run it on stdin.
  sax_parser = make_parser()
  sax_handler = WikipediaSaxHandler(wiki_handler)
  sax_parser.setContentHandler(sax_handler)
  sax_parser.parse(sys.stdin)
  wiki_handler.finish_processing()
  
def main():

  op = OptionParser(usage="%prog [options] input_dir")
  op.add_option("-p", "--words-coords",
                help="Print all words and coordinates",
                action="store_true")
  op.add_option("-l", "--find-links",
                help="""Find all links and print info about them.
Includes count of incoming links, and, for each surface form, counts of
all articles it maps to.""",
                action="store_true")
  op.add_option("-c", "--coords-counts",
                help="Print info about counts of words for all articles with coodinates.",
                action="store_true")
  op.add_option("-f", "--coords-file",
                help="""File containing output from a prior run of
--coords-counts, listing all the articles with associated coordinates.
This is used to limit the operation of --find-links to only consider links
to articles with coordinates.  Currently, if this is not done, then using
--coords-file requires at least 10GB, perhaps more, of memory in order to
store the entire table of surface->article mappings in memory. (If this
entire table is needed, it may be necessary to implement a MapReduce-style
process where smaller chunks are processed separately and then the results
combined.)""",
                metavar="FILE")
  op.add_option("-d", "--debug", metavar="LEVEL",
                help="Output debug info at given level")
  opts, args = op.parse_args()

  global debug
  if opts.debug:
    debug = int(opts.debug)
 
  if opts.coords_file:
    get_coordinates(opts.coords_file)    
  if opts.words_coords:
    main_process_input(PrintWordsAndCoords())
  elif opts.find_links:
    main_process_input(FindLinks())
  elif opts.coords_counts:
    main_process_input(GetCoordsAndCounts())

#import cProfile
#cProfile.run('main()', 'process-wiki.prof')
main()
