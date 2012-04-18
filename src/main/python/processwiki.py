#!/usr/bin/env python
# -*- coding: utf-8 -*-
#######
####### processwiki.py
#######
####### Copyright (c) 2010 Ben Wing.
#######

### FIXME:
###
### Cases to fix involving coordinates:

# 1. Nested coordinates:

#{{Infobox Australian Place
#| name     = Lauderdale
#| image    = Lauderdale Canal.JPG
#| caption  = 
#| loc-x    = 
#| loc-y    = 
#| coordinates = {{coord|42|54|40|S|147|29|34|E|display=inline,title}}
#| state    = tas
#...
#}}

import sys, re
from optparse import OptionParser
from nlputil import *
import itertools
import time
from process_article_data import *

from xml.sax import make_parser
from xml.sax.handler import ContentHandler

# Debug flags.  Different flags indicate different info to output.
debug = booldict()

# Program options
progopts = None

disambig_pages_by_id = set()

article_namespaces = ['User', 'Wikipedia', 'File', 'MediaWiki', 'Template',
                      'Help', 'Category', 'Thread', 'Summary', 'Portal',
                      'Book']

article_namespaces = {}
article_namespaces_lower = {}

article_namespace_aliases = {
  'P':'Portal', 'H':'Help', 'T':'Template',
  'CAT':'Category', 'Cat':'Category', 'C':'Category',
  'MOS':'Wikipedia', 'MoS':'Wikipedia', 'Mos':'Wikipedia'}

# Count number of incoming links for articles
incoming_link_count = intdict()

# Map anchor text to a hash that maps articles to counts
anchor_text_map = {}

# Set listing articles containing coordinates
coordinate_articles = set()

debug_cur_title = None

# Parse the result of a previous run of --coords-counts for articles with
# coordinates
def read_coordinates_file(filename):
  errprint("Reading coordinates file %s..." % filename)
  status = StatusMessage('article')
  for line in uchompopen(filename):
    m = re.match('Article title: (.*)', line)
    if m:
      title = capfirst(m.group(1))
    elif re.match('Article coordinates: ', line):
      coordinate_articles.add(title)
      if status.item_processed(maxtime=Opts.max_time_per_stage):
        break
    
# Read in redirects.  Record redirects as additional articles with coordinates
# if the article pointed to has coordinates. NOTE: Must be done *AFTER*
# reading coordinates.
def read_redirects_from_article_data(filename):
  assert coordinate_articles
  errprint("Reading redirects from article data file %s..." % filename)

  def process(art):
    if art.namespace != 'Main':
      return
    if art.redir and capfirst(art.redir) in coordinate_articles:
      coordinate_articles.add(art.title)

  read_article_data_file(filename, process, maxtime=Opts.max_time_per_stage)

# Read the list of disambiguation article ID's.
def read_disambig_id_file(filename):
  errprint("Reading disambig ID file %s..." % filename)
  status = StatusMessage("article")
  for line in uchompopen(filename):
    disambig_pages_by_id.add(line)
    if status.item_processed(maxtime=Opts.max_time_per_stage):
      break
    
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

#######################################################################
#                         Splitting the output                        #
#######################################################################

# Files to output to, when splitting output
split_output_files = None

# List of split suffixes
split_suffixes = None

# Current file to output to
cur_output_file = sys.stdout
debug_to_stderr = False

# Name of current split (training, dev, test)
cur_split_name = ''

# Generator of files to output to
split_file_gen = None

# Initialize the split output files, using PREFIX as the prefix
def init_output_files(prefix, split_fractions, the_split_suffixes):
  assert len(split_fractions) == len(the_split_suffixes)
  global split_output_files
  split_output_files = [None]*len(the_split_suffixes)
  global split_suffixes
  split_suffixes = the_split_suffixes
  for i in range(len(the_split_suffixes)):
    split_output_files[i] = open("%s.%s" % (prefix, the_split_suffixes[i]), "w")
  global split_file_gen
  split_file_gen = next_split_set(split_fractions)

# Find the next split file to output to and set CUR_OUTPUT_FILE appropriately;
# don't do anything if the user hasn't called for splitting.
def set_next_split_file():
  global cur_output_file
  global cur_split_name
  if split_file_gen:
    nextid = split_file_gen.next()
    cur_output_file = split_output_files[nextid]
    cur_split_name = split_suffixes[nextid]
  
#######################################################################
#                  Chunk text into balanced sections                  #
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

left_ref_re = r'<ref.*?>'
# Return braces and brackets separately from other text.
simple_balanced_re = re.compile(left_ref_re + r'|</ref>|[^{}\[\]<]+|[{}\[\]]|<')
#simple_balanced_re = re.compile(r'[^{}\[\]]+|[{}\[\]]')

# Return braces, brackets and pipe symbols separately from other text.
balanced_pipe_re = re.compile(left_ref_re + r'|</ref>|[^{}\[\]|<]+|[{}\[\]|]|<')
#balanced_pipe_re = re.compile(r'[^{}\[\]|]+|[{}\[\]|]')

# Return braces, brackets, and newlines separately from other text.
# Useful for handling Wikipedia tables, denoted with {| ... |}.
balanced_table_re = re.compile(left_ref_re + r'|</ref>|[^{}\[\]\n<]+|[{}\[\]\n]|<')
#balanced_table_re = re.compile(r'[^{}\[\]\n]+|[{}\[\]\n]')

left_match_chars = {'{':'}', '[':']', '<ref>':'</ref>'}
right_match_chars = {'}':'{', ']':'[', '</ref>':'<ref>'}

def parse_balanced_text(textre, text, throw_away = 0):
  '''Parse text in TEXT containing balanced expressions surrounded by single
or double braces or brackets.  This is a generator; it successively yields
chunks of text consisting either of sections without any braces or brackets,
or balanced expressions delimited by single or double braces or brackets, or
unmatched single or double right braces or brackets.  TEXTRE is used to
separate the text into chunks; it can be used to separate out additional
top-level separators, such as vertical bar.'''
  strbuf = []
  prevstring = "(at beginning)"
  leftmatches = []
  parenlevel = 0
  for string in textre.findall(text):
    if debug['debugparens']:
      errprint("pbt: Saw %s, parenlevel=%s" % (string, parenlevel))
    if string.startswith('<ref'):
      #errprint("Saw reference: %s" % string)
      if not string.endswith('>'):
        wikiwarning("Strange parsing, saw odd ref tag: %s" % string)
      if string.endswith('/>'):
        continue
      string = '<ref>'
    if string in right_match_chars:
      if parenlevel == 0:
        wikiwarning("Nesting level would drop below 0; string = %s, prevstring = %s" % (string, prevstring.replace('\n','\\n')))
        yield string
      else:
        strbuf.append(string)
        assert len(leftmatches) == parenlevel
        should_left = right_match_chars[string]
        should_pop_off = 1
        the_left = leftmatches[-should_pop_off]
        if should_left != the_left:
          if should_left == '<ref>':
            wikiwarning("Saw unmatched </ref>")
            in_ref = any([match for match in leftmatches if match == '<ref>'])
            if not in_ref:
              wikiwarning("Stray </ref>??; prevstring = %s" % prevstring.replace('\n','\\n'))
              should_pop_off = 0
            else:
              while (len(leftmatches) - should_pop_off >= 0 and
                  should_left != leftmatches[len(leftmatches)-should_pop_off]):
                should_pop_off += 1
              if should_pop_off >= 0:
                wikiwarning("%s non-matching brackets inside of <ref>...</ref>: %s ; prevstring = %s" % (should_pop_off - 1, ' '.join(left_match_chars[x] for x in leftmatches[len(leftmatches)-should_pop_off:]), prevstring.replace('\n','\\n')))
              else:
                wikiwarning("Inside of <ref> but still interpreted as stray </ref>??; prevstring = %s" % prevstring.replace('\n','\\n'))
                should_pop_off = 0
          elif the_left == '<ref>':
            wikiwarning("Stray %s inside of <ref>...</ref>; prevstring = %s" % (string, prevstring.replace('\n','\\n')))
            should_pop_off = 0
          else:
            wikiwarning("Non-matching brackets: Saw %s, expected %s; prevstring = %s" % (string, left_match_chars[the_left], prevstring.replace('\n','\\n')))
        if should_pop_off > 0:
          parenlevel -= should_pop_off
          if debug['debugparens']:
            errprint("pbt: Decreasing parenlevel by 1 to %s" % parenlevel)
          leftmatches = leftmatches[:-should_pop_off]
        if parenlevel == 0:
          yield ''.join(strbuf)
          strbuf = []
    else:
      if string in left_match_chars:
        if throw_away > 0:
          wikiwarning("Throwing away left bracket %s as a reparse strategy"
              % string)
          throw_away -= 1
        else:
          parenlevel += 1
          if debug['debugparens']:
            errprint("pbt: Increasing parenlevel by 1 to %s" % parenlevel)
          leftmatches.append(string)
      if parenlevel > 0:
        strbuf.append(string)
      else:
        yield string
    prevstring = string
  leftover = ''.join(strbuf)
  if leftover:
    wikiwarning("Unmatched left paren, brace or bracket: %s characters remaining" % len(leftover))
    wikiwarning("Remaining text: [%s]" % bound_string_length(leftover))
    wikiwarning("Reparsing:")
    for string in parse_balanced_text(textre, leftover, throw_away = parenlevel):
      yield string

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

def splitprint(text):
  '''Print text (possibly Unicode) to the appropriate output, either stdout
or one of the split output files.'''
  uniprint(text, outfile=cur_output_file)

def outprint(text):
  '''Print text (possibly Unicode) to stdout (but stderr in certain debugging
modes).'''
  if debug_to_stderr:
    errprint(text)
  else:
    uniprint(text)

def wikiwarning(foo):
  warning("Article %s: %s" % (debug_cur_title, foo))

# Output a string of maximum length, adding ... if too long
def bound_string_length(str, maxlen=60):
  if len(str) <= maxlen:
    return str
  else:
    return '%s...' % str[0:maxlen]

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
      key = m.group(1).strip().lower().replace('_','').replace(' ','')
      value = m.group(2)
      if strip_values:
        value = value.strip()
      hash[key] = value
    else:
      #errprint("Unable to process template argument %s" % arg)
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

  macroargs1 = [foo for foo in
               parse_balanced_text(balanced_pipe_re, macro[2:-2])]
  macroargs2 = []
  # Concatenate adjacent args if neither one is a |
  for x in macroargs1:
    if x == '|' or len(macroargs2) == 0 or macroargs2[-1] == '|':
      macroargs2 += [x]
    else:
      macroargs2[-1] += x
  macroargs = [x for x in macroargs2 if x != '|']
  if not macroargs:
    wikiwarning("Strange macro with no arguments: %s" % macroargs)
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
    
  def process_reference(self, text):
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
  
    if debug['lots']: errprint("Entering process_source_text: [%s]" % text)
  
    for foo in parse_simple_balanced_text(text):
      if debug['lots']: errprint("parse_simple_balanced_text yields: [%s]" % foo)
  
      if foo.startswith('[['):
        gen = self.process_internal_link(foo)
  
      elif foo.startswith('{{'):
        gen = self.process_template(foo)
  
      elif foo.startswith('{|'):
        gen = self.process_table(foo)
  
      elif foo.startswith('['):
        gen = self.process_external_link(foo)
  
      elif foo.startswith('<ref'):
        gen = self.process_reference(foo)
  
      else:
        gen = self.process_text_chunk(foo)
  
      for chunk in gen:
        if debug['lots']: errprint("process_source_text yields: [%s]" % chunk)
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
    
  def process_reference(self, text):
    return self.process_source_text(" " + text[5:-6] + " ")
    
#######################################################################
#                     Process text for coordinates                    #
#######################################################################

# Accumulate a table of all the templates with coordinates in them, along
# with counts.
templates_with_coords = intdict()

# Accumulate a table of all templates, with counts.
all_templates = intdict()

def safe_float_1(x):
  '''Subfunction of safe_float.  Always return None.'''
  if x is None:
    return None
  try:
    x = x.strip()
  except:
    pass
  try:
    return float(x)
  except:
    # In the Portuguese version at least, we have entries like
    # {{Coor title d|51.30.23|N|0.7.35|O|type:landmark|region:GB}}
    m = re.match(r'(-?[0-9]+)\.([0-9]+)\.([0-9]+)', x)
    if m:
      (deg, min, sec) = m.groups()
      return convert_dms(1, deg, min, sec)
    if x:
      wikiwarning("Expected number, saw %s" % x)
    return None

def safe_float(x, zero_on_error=False):
  '''Convert a string to floating point, but don't crash on errors;
instead, output a warning.  If 'zero_on_error', return 0 if no number could
be produced; otherwise, return None.'''
  ret = safe_float_1(x)
  if ret is None and zero_on_error: return 0.
  return ret

def get_german_style_coord(arg):
  '''Handle plain floating-point numbers as well as "German-style"
deg/min/sec/DIR indicators like 45/32/30/E.'''
  if arg is None:
    return None
  arg = arg.lstrip()
  if ' ' in arg:
    arg = re.sub(' .*$', '', arg)
  if '/' in arg:
    m = re.match('([0-9.]+)/([0-9.]+)?/([0-9.]+)?(?:/([NSEWOnsewo]))?', arg)
    if m:
      (deg, min, sec, offind) = m.groups()
      if offind:
        offind = offind.upper()
        if offind in convert_ns:
          off = convert_ns[offind]
        else:
          off = convert_ew_german[offind]
      else:
        off = 1
      return convert_dms(off, deg, min, sec)
    wikiwarning("Unrecognized DEG/MIN/SEC/HEMIS-style indicator: %s" % arg)
    return None
  else:
    return safe_float(arg)

def convert_dms(nsew, d, m, s, decimal = False):
  '''Convert a multiplier (1 or N or E, -1 for S or W) and degree/min/sec
values into a decimal +/- latitude or longitude.'''
  lat = get_german_style_coord(d)
  if lat is None:
    return None
  min = safe_float(m, zero_on_error = True)
  sec = safe_float(s, zero_on_error = True)
  if min < 0: min = -min
  if sec < 0: sec = -sec
  if min > 60:
    wikiwarning("Out-of-bounds minutes %s" % min)
    return None
  if sec > 60:
    wikiwarning("Out-of-bounds seconds %s" % sec)
    return None
  return nsew*(lat + min/60. + sec/3600.)

convert_ns = {'N':1, 'S':-1}
convert_ew = {'E':1, 'W':-1, 'L':1, 'O':-1}
# Blah!! O=Ost="east" in German but O=Oeste="west" in Spanish/Portuguese
convert_ew_german = {'E':1, 'W':-1, 'O':1}

# Get the default value for the hemisphere, as a multiplier +1 or -1.
# We need to handle the following as S latitude, E longitude:
#   -- Infobox Australia
#   -- Info/Localidade de Angola
#   -- Info/Município de Angola
#   -- Info/Localidade de Moçambique

# We need to handle the following as N latitude, W longitude:
#   -- Infobox Pittsburgh neighborhood
#   -- Info/Assentamento/Madeira
#   -- Info/Localidade da Madeira
#   -- Info/Assentamento/Marrocos
#   -- Info/Localidade dos EUA
#   -- Info/PousadaPC
#   -- Info/Antigas freguesias de Portugal
# Otherwise assume +1, so that we leave the values alone.  This is important
# because some fields may specifically use signed values to indicate the
# hemisphere directly, or use other methods of indicating hemisphere (e.g.
# "German"-style "72/50/35/W").
def get_hemisphere(temptype, is_lat):
  for x in ('infobox australia', 'info/localidade de angola',
      u'info/município de angola', u'info/localidade de moçambique'):
    if temptype.lower().startswith(x):
      if is_lat: return -1
      else: return 1
  for x in ('infobox pittsburgh neighborhood', 'info/assentamento/madeira',
      'info/assentamento/marrocos', 'info/localidade dos eua', 'info/pousadapc',
      'info/antigas freguesias de portugal'):
    if temptype.lower().startswith(x):
      if is_lat: return 1
      else: return -1
  return 1

# Get an argument (ARGSEARCH) by name from a hash table (ARGS).  Multiple
# synonymous names can be looked up by giving a list or tuple for ARGSEARCH.
# Other parameters control warning messages.
def getarg(argsearch, temptype, args, rawargs, warnifnot=True):
  if isinstance(argsearch, tuple) or isinstance(argsearch, list):
    for x in argsearch:
      val = args.get(x, None)
      if val is not None:
        return val
    if warnifnot or debug['some']:
      wikiwarning("None of params %s seen in template {{%s|%s}}" % (
        ','.join(argsearch), temptype, bound_string_length('|'.join(rawargs))))
  else:
    val = args.get(argsearch, None)
    if val is not None:
      return val
    if warnifnot or debug['some']:
      wikiwarning("Param %s not seen in template {{%s|%s}}" % (
        argsearch, temptype, bound_string_length('|'.join(rawargs))))
  return None

# Utility function for get_latd_coord().
# Extract out either latitude or longitude from a template of type
# TEMPTYPE with arguments ARGS.  LATD/LATM/LATS are lists or tuples of
# parameters to look up to retrieve the appropriate value. OFFPARAM is the
# list of possible parameters indicating the offset to the N, S, E or W.
# IS_LAT is True if a latitude is being extracted, False for longitude.
def get_lat_long_1(temptype, args, rawargs, latd, latm, lats, offparam, is_lat):
  d = getarg(latd, temptype, args, rawargs)
  m = getarg(latm, temptype, args, rawargs, warnifnot=False) 
  s = getarg(lats, temptype, args, rawargs, warnifnot=False)
  hemis = getarg(offparam, temptype, args, rawargs)
  if hemis is None:
    hemismult = get_hemisphere(temptype, is_lat)
  else:
    if is_lat:
      convert = convert_ns
    else:
      convert = convert_ew
    hemismult = convert.get(hemis, None)
    if hemismult is None:
      wikiwarning("%s for template type %s has bad value: [%s]" %
               (offparam, temptype, hemis))
      return None
  return convert_dms(hemismult, d, m, s)

latd_arguments = ('latd', 'latg', 'latdeg', 'latdegrees', 'latitudedegrees',
  'latitudinegradi', 'latgradi', 'latitudined', 'latitudegraden',
  'breitengrad', 'breddegrad', 'breddegrad')
def get_latd_coord(temptype, args, rawargs):
  '''Given a template of type TEMPTYPE with arguments ARGS (converted into
a hash table; also available in raw form as RAWARGS), assumed to have
a latitude/longitude specification in it using latd/lat_deg/etc. and
longd/lon_deg/etc., extract out and return a tuple of decimal
(latitude, longitude) values.'''
  lat = get_lat_long_1(temptype, args, rawargs,
      latd_arguments,
      ('latm', 'latmin', 'latminutes', 'latitudeminutes',
         'latitudineprimi', 'latprimi',
         'latitudineminuti', 'latminuti', 'latitudinem', 'latitudeminuten',
         'breitenminute', 'breddemin'),
      ('lats', 'latsec', 'latseconds', 'latitudeseconds',
         'latitudinesecondi', 'latsecondi', 'latitudines', 'latitudeseconden',
         'breitensekunde'),
      ('latns', 'latp', 'lap', 'latdir', 'latdirection', 'latitudinens'),
      is_lat=True)
  long = get_lat_long_1(temptype, args, rawargs,
      # Typos like Longtitude do occur in the Spanish Wikipedia at least
      ('longd', 'lond', 'longg', 'long', 'longdeg', 'londeg',
         'longdegrees', 'londegrees',
         'longitudinegradi', 'longgradi', 'longitudined',
         'longitudedegrees', 'longtitudedegrees',
         'longitudegraden',
         u'längengrad', 'laengengrad', 'lengdegrad', u'længdegrad'),
      ('longm', 'lonm', 'longmin', 'lonmin',
         'longminutes', 'lonminutes',
         'longitudineprimi', 'longprimi',
         'longitudineminuti', 'longminuti', 'longitudinem',
         'longitudeminutes', 'longtitudeminutes',
         'longitudeminuten',
         u'längenminute', u'længdemin'),
      ('longs', 'lons', 'longsec', 'lonsec',
         'longseconds', 'lonseconds',
         'longitudinesecondi', 'longsecondi', 'longitudines',
         'longitudeseconds', 'longtitudeseconds',
         'longitudeseconden',
         u'längensekunde'),
      ('longew', 'lonew', 'longp', 'lonp', 'longdir', 'londir',
         'longdirection', 'londirection', 'longitudineew'),
      is_lat=False)
  return (lat, long)

def get_built_in_lat_long_1(temptype, args, rawargs, latd, latm, lats, is_lat):
  d = getarg(latd, temptype, args, rawargs)
  m = getarg(latm, temptype, args, rawargs, warnifnot=False) 
  s = getarg(lats, temptype, args, rawargs, warnifnot=False)
  return convert_dms(mult, d, m, s)

built_in_latd_north_arguments = ('stopnin')
built_in_latd_south_arguments = ('stopnis')
built_in_longd_north_arguments = ('stopnie')
built_in_longd_south_arguments = ('stopniw')

def get_built_in_lat_coord(temptype, args, rawargs):
  '''Given a template of type TEMPTYPE with arguments ARGS (converted into
a hash table; also available in raw form as RAWARGS), assumed to have
a latitude/longitude specification in it using stopniN/etc. (where the
direction NSEW is built into the argument name), extract out and return a
tuple of decimal (latitude, longitude) values.'''
  if getarg(built_in_latd_north_arguments, temptype, args, rawargs) is not None:
    mult = 1
  elif getarg(built_in_latd_south_arguments, temptype, args, rawargs) is not None:
    mult = -1
  else:
    wikiwarning("Didn't see any appropriate stopniN/stopniS param")
    mult = 1 # Arbitrarily set to N, probably accurate in Poland
  lat = get_built_in_lat_long_1(temptype, args, rawargs,
      ('stopnin', 'stopnis'),
      ('minutn', 'minuts'),
      ('sekundn', 'sekunds'),
      mult)
  if getarg(built_in_longd_north_arguments, temptype, args, rawargs) is not None:
    mult = 1
  elif getarg(built_in_longd_south_arguments, temptype, args, rawargs) is not None:
    mult = -1
  else:
    wikiwarning("Didn't see any appropriate stopniE/stopniW param")
    mult = 1 # Arbitrarily set to E, probably accurate in Poland
  long = get_built_in_lat_long_1(temptype, args, rawargs,
      ('stopnie', 'stopniw'),
      ('minute', 'minutw'),
      ('sekunde', 'sekundw'),
      mult)
  return (lat, long)

latitude_arguments = ('latitude', 'latitud', 'latitudine',
    'breitengrad',
    # 'breite', Sometimes used for latitudes but also for other types of width
    #'lat' # Appears in non-article coordinates
    #'latdec' # Appears to be associated with non-Earth coordinates
    )
longitude_arguments = ('longitude', 'longitud', 'longitudine',
    u'längengrad', u'laengengrad',
    # u'länge', u'laenge', Sometimes used for longitudes but also for other lengths
    #'long' # Appears in non-article coordinates
    #'longdec' # Appears to be associated with non-Earth coordinates
    )

def get_latitude_coord(temptype, args, rawargs):
  '''Given a template of type TEMPTYPE with arguments ARGS, assumed to have
a latitude/longitude specification in it, extract out and return a tuple of
decimal (latitude, longitude) values.'''
  # German-style (e.g. 72/53/15/E) also occurs with 'latitude' and such,
  # so just check for it everywhere.
  lat = get_german_style_coord(getarg(latitude_arguments,
    temptype, args, rawargs))
  long = get_german_style_coord(getarg(longitude_arguments,
    temptype, args, rawargs))
  return (lat, long)

def get_infobox_ort_coord(temptype, args, rawargs):
  '''Given a template 'Infobox Ort' with arguments ARGS, assumed to have
a latitude/longitude specification in it, extract out and return a tuple of
decimal (latitude, longitude) values.'''
  # German-style (e.g. 72/53/15/E) also occurs with 'latitude' and such,
  # so just check for it everywhere.
  lat = get_german_style_coord(getarg((u'breite',),
    temptype, args, rawargs))
  long = get_german_style_coord(getarg((u'länge', u'laenge'),
    temptype, args, rawargs))
  return (lat, long)

# Utility function for get_coord().  Extract out the latitude or longitude
# values out of a Coord structure.  Return a tuple (OFFSET, VAL) for decimal
# latitude or longitude VAL and OFFSET indicating the offset of the next
# argument after the arguments used to produce the value.
def get_coord_1(args, convert_nsew):
  if args[1] in convert_nsew:
    d = args[0]; m = 0; s = 0; i = 1
  elif args[2] in convert_nsew:
    d = args[0]; m = args[1]; s = 0; i = 2
  elif args[3] in convert_nsew:
    d = args[0]; m = args[1]; s = args[2]; i = 3
  else:
    # Will happen e.g. in the style where only positive/negative are given
    return (1, convert_dms(1, args[0], 0, 0))
  return (i+1, convert_dms(convert_nsew[args[i]], d, m, s))

# FIXME!  To be more accurate, we need to look at the template parameters,
# which, despite the claim below, ARE quite interesting.  In fact, if the
# parameter 'display=title' is seen (or variant like 'display=inline,title'),
# then we have *THE* correct coordinate for the article.  So we need to
# return this fact if known, as an additional argument.  See comments
# below at extract_coordinates_from_article().

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
The template parameters mostly control display and are basically
uninteresting.  (FIXME: Not true, see above.) However, the coordinate
parameters contain lots of potentially useful information that can be
used as features or whatever.  See
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
  if debug['some']: errprint("Coord: Passed in args %s" % args)
  # Filter out optional "template arguments", add a bunch of blank arguments
  # at the end to make sure we don't get out-of-bounds errors in
  # get_coord_1()
  filtargs = [x for x in args if '=' not in x]
  if filtargs:
    filtargs += ['','','','','','']
    (i, lat) = get_coord_1(filtargs, convert_ns)
    (_, long) = get_coord_1(filtargs[i:], convert_ew)
    return (lat, long)
  else:
    (paramshash, _) = find_template_params(args, True)
    lat = paramshash.get('lat', None) or paramshash.get('latitude', None)
    long = paramshash.get('long', None) or paramshash.get('longitude', None)
    if lat is None or long is None:
      wikiwarning("Can't find latitude/longitude in {{%s|%s}}" %
              (temptype, '|'.join(args)))
    lat = safe_float(lat)
    long = safe_float(long)
    return (lat, long)

def check_for_bad_globe(paramshash):
  if debug['some']: errprint("check_for_bad_globe: Passed in args %s" % paramshash)
  globe = paramshash.get('globe', "").strip()
  if globe:
    if globe == "earth":
      wikiwarning("Interesting, saw globe=earth")
    else:
      wikiwarning("Rejecting as non-earth, in template 'Coordinate/Coord/etc.' saw globe=%s"
          % globe)
      return True
  return False

def get_coordinate_coord(extract_coords_obj, temptype, rawargs):
  '''Parse a Coordinate template and return a tuple (lat,long) for latitude and
longitude.  TEMPTYPE is the template name.  ARGS is the raw arguments for
the template.  These templates tend to occur in the German Wikipedia. Examples:

{{Coordinate|text=DMS|article=DMS|NS=51.50939|EW=-0.11832|type=city|pop=7825200|region=GB-LND}}
{{Coordinate|article=/|NS=41/00/00/N|EW=16/43/00/E|type=adm1st|region=IT-23}}
{Coordinate|NS=51/14/08.16/N|EW=6/48/37.43/E|text=DMS|name=Bronzetafel – Mittelpunkt Düsseldorfs|type=landmark|dim=50|region=DE-NW}}
{{Coordinate|NS=46.421401 &lt;!-- {{subst:CH1903-WGS84|777.367|143.725||koor=B }} --&gt;|EW=9.746124 &lt;!-- {{subst:CH1903-WGS84|777.367|143.725||koor=L }} --&gt;|region=CH-GR|text=DMS|type=isle|dim=500|name=Chaviolas}}
'''
  if debug['some']: errprint("Passed in args %s" % rawargs)
  (paramshash, _) = find_template_params(rawargs, True)
  if check_for_bad_globe(paramshash):
    extract_coords_obj.notearth = True
    return (None, None)
  lat = get_german_style_coord(getarg('ns', temptype, paramshash, rawargs))
  long = get_german_style_coord(getarg('ew', temptype, paramshash, rawargs))
  return (lat, long)

def get_coord_params(temptype, args):
  '''Parse a Coord template and return a list of tuples of coordinate
parameters (see comment under get_coord).'''
  if debug['some']: errprint("Passed in args %s" % args)
  # Filter out optional "template arguments"
  filtargs = [x for x in args if '=' not in x]
  if debug['some']: errprint("get_coord_params: filtargs: %s" % filtargs)
  hash = {}
  if filtargs and ':' in filtargs[-1]:
    for x in filtargs[-1].split('_'):
      if ':' in x:
        (key, value) = x.split(':', 1)
        hash[key] = value
  return hash

def get_geocoordenadas_coord(temptype, args):
  '''Parse a geocoordenadas template (common in the Portuguese Wikipedia) and
return a tuple (lat,long) for latitude and longitude.  TEMPTYPE is the
template name.  ARGS is the raw arguments for the template.  Typical example
is:

{{geocoordenadas|39_15_34_N_24_57_9_E_type:waterbody|39° 15′ 34&quot; N, 24° 57′ 9&quot; O}}
'''
  if debug['some']: errprint("Passed in args %s" % args)
  # Filter out optional "template arguments", add a bunch of blank arguments
  # at the end to make sure we don't get out-of-bounds errors in
  # get_coord_1()
  if len(args) == 0:
    wikiwarning("No arguments to template 'geocoordenadas'")
    return (None, None)
  else:
    # Yes, every one of the following problems occurs: Extra spaces; commas
    # used instead of periods; lowercase nsew; use of O (Oeste) for "West",
    # "L" (Leste) for "East"
    arg = args[0].upper().strip().replace(',','.')
    m = re.match(r'([0-9.]+)(?:_([0-9.]+))?(?:_([0-9.]+))?_([NS])_([0-9.]+)(?:_([0-9.]+))?(?:_([0-9.]+))?_([EWOL])(?:_.*)?$', arg)
    if not m:
      wikiwarning("Unrecognized argument %s to template 'geocoordenadas'" %
          args[0])
      return (None, None)
    else:
      (latd, latm, lats, latns, longd, longm, longs, longew) = \
          m.groups()
      return (convert_dms(convert_ns[latns], latd, latm, lats),
              convert_dms(convert_ew[longew], longd, longm, longs))

class ExtractCoordinatesFromSource(RecursiveSourceTextHandler):
  '''Given the article text TEXT of an article (in general, after first-
stage processing), extract coordinates out of templates that have coordinates
in them (Infobox, Coord, etc.).  Record each coordinate into COORD.

We don't recursively process text inside of templates or links.  If we want
to do that, change this class to inherit from RecursiveSourceTextHandler.

See process_article_text() for a description of the formatting that is
applied to the text before being sent here.'''

  def __init__(self):
    self.coords = []
    self.notearth = False

  def process_template(self, text):
    # Look for a Coord, Infobox, etc. template that may have coordinates in it
    lat = long = None
    if debug['some']: errprint("Enter process_template: [%s]" % text)
    tempargs = get_macro_args(text)
    temptype = tempargs[0].strip()
    if debug['some']: errprint("Template type: %s" % temptype)
    lowertemp = temptype.lower()
    rawargs = tempargs[1:]
    if (lowertemp.startswith('info/crater') or
        lowertemp.endswith(' crater data') or
        lowertemp.startswith('marsgeo') or
        lowertemp.startswith('encelgeo') or
        # All of the following are for heavenly bodies
        lowertemp.startswith('infobox feature on ') or
        lowertemp in (u'info/acidente geográfico de vênus',
                      u'infobox außerirdische region',
                      'infobox lunar mare', 'encelgeo-crater',
                      'infobox marskrater', 'infobox mondkrater',
                      'infobox mondstruktur')):
        self.notearth = True
        wikiwarning("Rejecting as not on Earth because saw template %s" % temptype)
        return []
    # Look for a coordinate template
    if lowertemp in ('coord', 'coordp', 'coords',
                     'koord', #Norwegian
                     'coor', 'coor d', 'coor dm', 'coor dms',
                     'coor title d', 'coor title dm', 'coor title dms',
                     'coor dec', 'coorheader') \
        or lowertemp.startswith('geolinks') \
        or lowertemp.startswith('mapit') \
        or lowertemp.startswith('koordynaty'): # Coordinates in Polish:
      (lat, long) = get_coord(temptype, rawargs)
      coord_params = get_coord_params(temptype, tempargs[1:])
      if check_for_bad_globe(coord_params):
        self.notearth = True
        return []
    elif lowertemp == 'coordinate':
      (lat, long) = get_coordinate_coord(self, temptype, rawargs)
    elif lowertemp in ('geocoordenadas', u'coördinaten'):
      # geocoordenadas is Portuguese, coördinaten is Dutch, and they work
      # the same way
      (lat, long) = get_geocoordenadas_coord(temptype, rawargs)
    else:
      # Look for any other template with a 'latd' or 'latitude' parameter.
      # Usually these will be Infobox-type templates.  Possibly we should only
      # look at templates whose lowercased name begins with "infobox".
      (paramshash, _) = find_template_params(rawargs, True)
      if getarg(latd_arguments, temptype, paramshash, rawargs, warnifnot=False) is not None:
        #errprint("seen: [%s] in {{%s|%s}}" % (getarg(latd_arguments, temptype, paramshash, rawargs), temptype, rawargs))
        (lat, long) = get_latd_coord(temptype, paramshash, rawargs)
      # NOTE: DO NOT CHANGE ORDER.  We want to check latd first and check
      # latitude afterwards for various reasons (e.g. so that cases where
      # breitengrad and breitenminute occur get found).  FIXME: Maybe we
      # don't need get_latitude_coord at all, but get_latd_coord will
      # suffice.
      elif getarg(latitude_arguments, temptype, paramshash, rawargs, warnifnot=False) is not None:
        #errprint("seen: [%s] in {{%s|%s}}" % (getarg(latitude_arguments, temptype, paramshash, rawargs), temptype, rawargs))
        (lat, long) = get_latitude_coord(temptype, paramshash, rawargs)
      elif (getarg(built_in_latd_north_arguments, temptype, paramshash,
                   rawargs, warnifnot=False) is not None or
            getarg(built_in_latd_south_arguments, temptype, paramshash,
                   rawargs, warnifnot=False) is not None):
        #errprint("seen: [%s] in {{%s|%s}}" % (getarg(built_in_latd_north_arguments, temptype, paramshash, rawargs), temptype, rawargs))
        #errprint("seen: [%s] in {{%s|%s}}" % (getarg(built_in_latd_south_arguments, temptype, paramshash, rawargs), temptype, rawargs))
        (lat, long) = get_built_in_lat_coord(temptype, paramshash, rawargs)
      elif lowertemp in ('infobox ort', 'infobox verwaltungseinheit'):
        (lat, long) = get_infobox_ort_coord(temptype, paramshash, rawargs)

    if debug['some']: wikiwarning("Saw coordinate %s,%s in template type %s" %
              (lat, long, temptype))
    if lat is None and long is not None:
      wikiwarning("Saw longitude %s but no latitude in template: %s" %
          (long, bound_string_length(text)))
    if long is None and lat is not None:
      wikiwarning("Saw latitude %s but no longitude in template: %s" %
          (lat, bound_string_length(text)))
    if lat is not None and long is not None:
      if lat == 0.0 and long == 0.0:
        wikiwarning("Rejecting coordinate because zero latitude and longitude seen")
      elif lat > 90.0 or lat < -90.0 or long > 180.0 or long < -180.0:
        wikiwarning("Rejecting coordinate because out of bounds latitude or longitude: (%s,%s)" % (lat, long))
      else:
        if lat == 0.0 or long == 0.0:
          wikiwarning("Zero value in latitude and/or longitude: (%s,%s)" %
              (lat, long))
        self.coords.append((lowertemp,lat,long))
        templates_with_coords[lowertemp] += 1
    # Recursively process the text inside the template in case there are
    # coordinates in it.
    return self.process_source_text(text[2:-2])

#category_types = [
#    ['neighbourhoods', 'neighborhood'],
#    ['neighborhoods', 'neighborhood'],
#    ['mountains', 'mountain'],
#    ['stations', ('landmark', 'railwaystation')],
#    ['rivers', 'river'],
#    ['islands', 'isle'],
#    ['counties', 'adm2nd'],
#    ['parishes', 'adm2nd'],
#    ['municipalities', 'city'],
#    ['communities', 'city'],
#    ['towns', 'city'],
#    ['villages', 'city'],
#    ['hamlets', 'city'],
#    ['communes', 'city'],
#    ['suburbs', 'city'],
#    ['universities', 'edu'],
#    ['colleges', 'edu'],
#    ['schools', 'edu'],
#    ['educational institutions', 'edu'],
#    ['reserves', '?'],
#    ['buildings', '?'],
#    ['structures', '?'],
#    ['landfills' '?'],
#    ['streets', '?'],
#    ['museums', '?'],
#    ['galleries', '?']
#    ['organizations', '?'],
#    ['groups', '?'],
#    ['lighthouses', '?'],
#    ['attractions', '?'],
#    ['border crossings', '?'],
#    ['forts', '?'],
#    ['parks', '?'],
#    ['townships', '?'],
#    ['cathedrals', '?'],
#    ['skyscrapers', '?'],
#    ['waterfalls', '?'],
#    ['caves', '?'],
#    ['beaches', '?'],
#    ['cemeteries'],
#    ['prisons'],
#    ['territories'],
#    ['states'],
#    ['countries'],
#    ['dominions'],
#    ['airports', 'airport'],
#    ['bridges'],
#    ]


class ExtractLocationTypeFromSource(RecursiveSourceTextHandler):
  '''Given the article text TEXT of an article (in general, after first-
stage processing), extract info about the type of location (if any).
Record info found in 'loctype'.'''

  def __init__(self):
    self.loctype = []
    self.categories = []

  def process_internal_link(self, text):
    tempargs = get_macro_args(text)
    arg0 = tempargs[0].strip()
    if arg0.startswith('Category:'):
      self.categories += [arg0[9:].strip()]
    return self.process_source_text(text[2:-2])

  def process_template(self, text):
    # Look for a Coord, Infobox, etc. template that may have coordinates in it
    lat = long = None
    tempargs = get_macro_args(text)
    temptype = tempargs[0].strip()
    lowertemp = temptype.lower()
    # Look for a coordinate template
    if lowertemp in ('coord', 'coor d', 'coor dm', 'coor dms',
                     'coor dec', 'coorheader') \
        or lowertemp.startswith('geolinks') \
        or lowertemp.startswith('mapit'):
      params = get_coord_params(temptype, tempargs[1:])
      if params:
        # WARNING, this returns a hash table, not a list of tuples
        # like the others do below.
        self.loctype += [['coord-params', params]]
    else:
      (paramshash, _) = find_template_params(tempargs[1:], True)
      if lowertemp == 'infobox settlement':
        params = []
        for x in ['settlementtype',
                  'subdivisiontype', 'subdivisiontype1', 'subdivisiontype2',
                  'subdivisionname', 'subdivisionname1', 'subdivisionname2',
                  'coordinatestype', 'coordinatesregion']:
          val = paramshash.get(x, None)
          if val:
            params += [(x, val)]
        self.loctype += [['infobox-settlement', params]]
      elif ('latd' in paramshash or 'latdeg' in paramshash or
          'latitude' in paramshash):
        self.loctype += \
            [['other-template-with-coord', [('template', temptype)]]]
    # Recursively process the text inside the template in case there are
    # coordinates in it.
    return self.process_source_text(text[2:-2])

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
    namespace = article_namespaces_lower.get(namespace, namespace)
    if namespace in ('image', 6): # 6 = file
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
    # For textual internal link, use all arguments, unless --raw-text
    if Opts.raw_text:
      yield tempargs[-1]
    else:
      for chunk in tempargs: yield chunk

# Process a template into separate chunks for each interesting
# argument.  Yield the chunks.  They will be recursively processed, and
# joined by spaces.
def yield_template_args(text):
  # For a template, do something smart depending on the template.
  if debug['lots']: errprint("yield_template_args called with: %s" % text)

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
  if debug['lots']: errprint("template args: %s" % tempargs)
  temptype = tempargs[0].strip().lower()

  if debug['some']:
    all_templates[temptype] += 1

  # Extract the parameter and non-parameter arguments.
  (paramhash, nonparam) = find_template_params(tempargs[1:], False)
  #errprint("params: %s" % paramhash)
  #errprint("nonparam: %s" % nonparam)

  # For certain known template types, use the values from the interesting
  # parameter args and ignore the others.  For other template types,
  # assume the parameter are uninteresting.
  if re.match(r'v?cite', temptype):
    # A citation, a very common type of template.
    for (key,value) in paramhash.items():
      # A fairly arbitrary list of "interesting" parameters.
      if re.match(r'(last|first|authorlink)[1-9]?$', key) or \
         re.match(r'(author|editor)[1-9]?-(last|first|link)$', key) or \
         key in ('coauthors', 'others', 'title', 'transtitle',
                 'quote', 'work', 'contribution', 'chapter', 'transchapter',
                 'series', 'volume'):
        yield value
  elif re.match(r'infobox', temptype):
    # Handle Infoboxes.
    for (key,value) in paramhash.items():
      # A fairly arbitrary list of "interesting" parameters.
      # Remember that _ and space are removed.
      if key in ('name', 'fullname', 'nickname', 'altname', 'former',
                 'alt', 'caption', 'description', 'title', 'titleorig',
                 'imagecaption', 'imagecaption', 'mapcaption',
                 # Associated with states, etc.
                 'motto', 'mottoenglish', 'slogan', 'demonym', 'capital',
                 # Add more here
                 ):
        yield value
  elif re.match(r'coord', temptype):
    return

  # For other template types, ignore all parameters and yield the
  # remaining arguments.
  # Yield any non-parameter arguments.
  for arg in nonparam:
    yield arg

# Process a table into separate chunks.  Unlike code for processing
# internal links, the chunks should have whitespace added where necessary.
def yield_table_chunks(text):
  if debug['lots']: errprint("Entering yield_table_chunks: [%s]" % text)

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
    if debug['lots']: errprint("Entering process_table_chunk: [%s], %s" % (text, atstart))
    for chunk in process_table_chunk_1(text, atstart):
      if debug['lots']: errprint("process_table_chunk yields: [%s]" % chunk)
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
    if debug['lots']: errprint("parse_balanced_text(balanced_table_re) yields: [%s]" % arg)
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
  (text, _) = re.subn(left_ref_re, r' ', text)
  if Opts.no_tokenize:
    # No tokenization requested.  Just split on whitespace.  But still try
    # to eliminate URL's.  Rather than just look for :, we look for :/, which
    # URL's are likely to contain.  Possibly we should look for a colon in
    # the middle of a word, which is effectively what the checks down below
    # do (or modify those checks to look for :/).
    for word in re.split('\s+', text):
      if ':/' not in word:
        yield word
  elif Opts.raw_text:
    # This regexp splits on whitespace, but also handles the following cases:
    # 1. Any of , ; . etc. at the end of a word
    # 2. Parens or quotes in words like (foo) or "bar"
    off = 0
    for word in re.split(r'([,;."):]*)\s+([("]*)', text):
      if (off % 3) != 0:
        for c in word:
          yield c
      else:
        # Sometimes URL's or other junk slips through.  Much of this junk has
        # a colon in it and little useful stuff does.
        if ':' not in word:
          # Handle things like "Two-port_network#ABCD-parameters".  Do this after
          # filtering for : so URL's don't get split up.
          for word2 in re.split('[#_]', word):
            if word2: yield word2
      off += 1
  else:
    # This regexp splits on whitespace, but also handles the following cases:
    # 1. Any of , ; . etc. at the end of a word
    # 2. Parens or quotes in words like (foo) or "bar"
    for word in re.split(r'[,;."):]*\s+[("]*', text):
      # Sometimes URL's or other junk slips through.  Much of this junk has
      # a colon in it and little useful stuff does.
      if ':' not in word:
        # Handle things like "Two-port_network#ABCD-parameters".  Do this after
        # filtering for : so URL's don't get split up.
        for word2 in re.split('[#_]', word):
          if word2: yield word2

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
      if debug['lots']: errprint("process_table yields: [%s]" % bar)
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
  
  def process_reference(self, text):
    return self.process_source_text(" " + text[5:-6] + " ")
    
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

  # Try getting rid of everything in a reference
  #(text, _) = re.subn(r'(?s)<ref.*?>.*?</ref>', '', text)
  #(text, _) = re.subn(r'(?s)<ref[^<>/]*?/>', '', text)

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
  #(text, _) = re.subn(r'&#91;', '[', text)
  #(text, _) = re.subn(r'&#93;', ']', text)

  return text

# Process the text in various ways in preparation for extracting
# the words from the text.
def format_text_second_pass(text):
  # Convert breaks into newlines
  (text, _) = re.subn(r'<br( +/)?>', r'\n', text)

  # Remove references, but convert to whitespace to avoid concatenating
  # words outside and inside a reference together
  #(text, _) = re.subn(r'(?s)<ref.*?>', ' ', text)

  # An alternative approach.
  # Convert references to simple tags.
  (text, _) = re.subn(r'(?s)<ref[^<>]*?/>', ' ', text)
  (text, _) = re.subn(r'(?s)<ref.*?>', '< ref>', text)
  (text, _) = re.subn(r'(?s)</ref.*?>', '< /ref>', text)

  # Similar for nowiki, which may have <'s, brackets and such inside.
  (text, _) = re.subn(r'(?s)<nowiki>.*?</nowiki>', ' ', text)

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
  (text, _) = re.subn(r'(?s)<[A-Za-z/].*?>', '', text)

  (text, _) = re.subn(r'< (/?ref)>', r'<\1>', text)

  # Remove multiple sequences of quotes (indicating boldface or italics)
  (text, _) = re.subn(r"''+", '', text)
 
  # Remove beginning-of-line markers indicating indentation, lists, headers,
  # etc.
  (text, _) = re.subn(r"(?m)^[*#:]+", '', text)

  # Remove end-of-line markers indicating headers (e.g. ===Introduction===)
  (text, _) = re.subn(r"(?m)^=+(.*?)=+$", r'\1', text)

  return text

#######################################################################
#                           Article handlers                          #
#######################################################################



### Default handler class for processing article text.  Subclass this to
### implement your own handlers.
class ArticleHandler(object):
  def __init__(self):
    self.title = None
    self.id = None

  redirect_commands = "|".join([
      # English, etc.
      'redirect', 'redirect to',
      # Italian (IT)
      'rinvia', 'rinvio',
      # Polish (PL)
      'patrz', 'przekieruj', 'tam',
      # Dutch (NL)
      'doorverwijzing',
      # French (FR)
      'redirection',
      # Spanish (ES)
      u'redirección',
      # Portuguese (PT)
      'redirecionamento',
      # German (DE)
      'weiterleitung',
      # Russian (RU)
      u'перенаправление',
    ])
 
  global redirect_re
  redirect_re = re.compile(ur'(?i)#(?:%s)\s*:?\s*\[\[(.*?)\]\]' %
      redirect_commands)

  # Process the text of article TITLE, with text TEXT.  The default
  # implementation does the following:
  #
  # 1. Remove comments, math, and other unuseful stuff.
  # 2. If article is a redirect, call self.process_redirect() to handle it.
  # 3. Else, call self.process_text_for_data() to extract data out.
  # 4. If that handler returned True, call self.process_text_for_text()
  #    to do processing of the text itself (e.g. for words).

  def process_article_text(self, text, title, id, redirect):
    self.title = title
    self.id = id
    global debug_cur_title
    debug_cur_title = title
  
    if debug['some']:
      errprint("Article title: %s" % title)
      errprint("Article ID: %s" % id)
      errprint("Article is redirect: %s" % redirect)
      errprint("Original article text:\n%s" % text)
  
    ### Preliminary processing of text, removing stuff unuseful even for
    ### extracting data.
 
    text = format_text_first_pass(text)
  
    ### Look to see if the article is a redirect
  
    if redirect:
      m = redirect_re.match(text.strip())
      if m:
        self.process_redirect(m.group(1))
        # NOTE: There may be additional templates specified along with a
        # redirection page, typically something like {{R from misspelling}}
        # that gives the reason for the redirection.  Currently, we ignore
        # such templates.
        return
      else:
        wikiwarning(
          "Article %s (ID %s) is a redirect but can't parse redirect spec %s"
          % (title, id, text))
  
    ### Extract the data out of templates; if it returns True, also process
    ### text for words
  
    if self.process_text_for_data(text):
      self.process_text_for_text(text)

  # Process the text itself, e.g. for words.  Default implementation does
  # nothing.
  def process_text_for_text(self, text):
    pass

  # Process an article that is a redirect.  Default implementation does
  # nothing.

  def process_redirect(self, redirtitle):
    pass

  # Process the text and extract data.  Return True if further processing of
  # the article should happen. (Extracting the real text in
  # process_text_for_text() currently takes up the vast majority of running
  # time, so skipping it is a big win.)
  #
  # Default implementation just returns True.

  def process_text_for_data(self, text):
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

  def process_text_for_text(self, text):  
    # Now process the text in various ways in preparation for extracting
    # the words from the text
    text = format_text_second_pass(text)
    # Now process the resulting text into chunks.  Join them back together
    # again (to handle cases like "the [[latent variable]]s are ..."), and
    # split to find words.
    self.process_text_for_words(
      split_text_into_words(
        ''.join(ExtractUsefulText().process_source_text(text))))

  # Process the real words of the text of an article.  Default implementation
  # does nothing.

  def process_text_for_words(self, word_generator):
    pass



# Print out the info passed in for article words; as for the implementation of
# process_text_for_data(), uses ExtractCoordinatesFromSource() to extract
# coordinates, and outputs all the coordinates seen.  Always returns True.

class OutputAllWords(ArticleHandlerForUsefulText):
  def process_text_for_words(self, word_generator):
    splitprint("Article title: %s" % self.title)
    splitprint("Article ID: %s" % self.id)
    for word in word_generator:
      if debug['some']: errprint("Saw word: %s" % word)
      else: splitprint("%s" % word)

  def process_text_for_data(self, text):
    #handler = ExtractCoordinatesFromSource()
    #for foo in handler.process_source_text(text): pass
    #for (temptype,lat,long) in handler.coords:
    #  splitprint("Article coordinates: %s,%s" % (lat, long))
    return True

  def finish_processing(self):
    ### Output all of the templates that were seen with coordinates in them,
    ### along with counts of how many times each template was seen.
    if debug['some']:
      print("Templates with coordinates:")
      output_reverse_sorted_table(templates_with_coords,
                                  outfile=cur_output_file)
      
      print("All templates:")
      output_reverse_sorted_table(all_templates, outfile=cur_output_file)
  
      print "Notice: ending processing"


class OutputCoordWords(OutputAllWords):
  def process_text_for_data(self, text):
    if extract_coordinates_from_article(text):
      return True
    return False


# Just find redirects.

class FindRedirects(ArticleHandler):
  def process_redirect(self, redirtitle):
    splitprint("Article title: %s" % self.title)
    splitprint("Article ID: %s" % self.id)
    splitprint("Redirect to: %s" % redirtitle)

def output_title(title, id):
  splitprint("Article title: %s" % title)
  splitprint("Article ID: %s" % id)

def output_title_and_coordinates(title, id, lat, long):
  output_title(title, id)
  splitprint("Article coordinates: %s,%s" % (lat, long))

# FIXME:
#
# (1) Figure out whether coordinates had a display=title in them.
#     If so, use the last one.
# (2) Else, use the last other Coord, but possibly limit to Coords that
#     appear on a line by themselves or at least are at top level (not
#     inside some other template, table, etc.).
# (3) Else, do what we prevously did.
#
# Also, we should test to see whether it's better in (2) to limit Coords
# to those that apear on a line by themselves.  To do that, we'd generate
# coordinates for Wikipedia, and in the process note
#
# (1) Whether it was step 1, 2 or 3 above that produced the coordinate;
# (2) If step 2, would the result have been different if we did step 2
#     differently?  Check the possibilities: No limit in step 2;
#     (maybe, if not too hard) limit to those things at top level;
#     limit to be on line by itself; don't ever use Coords in step 2.
#     If there is a difference among the results of any of these strategies
#     debug-output this fact along with the different values and the
#     strategies that produced them.
#
# Then
#
# (1) Output counts of how many resolved through steps 1, 2, 3, and how
#     many in step 2 triggered a debug-output.
# (2) Go through manually and check e.g. 50 of the ones with debug-output
#     and see which one is more correct.  

def extract_coordinates_from_article(text):
  handler = ExtractCoordinatesFromSource()
  for foo in handler.process_source_text(text): pass
  if handler.notearth:
    return None
  elif len(handler.coords) > 0:
    # Prefer a coordinate specified using {{Coord|...}} or similar to
    # a coordinate in an Infobox, because the latter tend to be less
    # accurate.
    for (temptype, lat, long) in handler.coords:
      if temptype.startswith('coor'):
        return (lat, long)
    (temptype, lat, long) = handler.coords[0]
    return (lat, long)
  else: return None

def extract_and_output_coordinates_from_article(title, id, text):
  retval = extract_coordinates_from_article(text)
  if retval == None: return False
  (lat, long) = retval
  output_title_and_coordinates(title, id, lat, long)
  return True

def extract_location_type(text):
  handler = ExtractLocationTypeFromSource()
  for foo in handler.process_source_text(text): pass
  for (ty, vals) in handler.loctype:
    splitprint("  %s: %s" % (ty, vals))
  for cat in handler.categories:
    splitprint("  category: %s" % cat)

# Handler to output count information on words.  Only processes articles
# with coordinates in them.  Computes the count of each word in the article
# text, after filtering text for "actual text" (as opposed to directives
# etc.), and outputs the counts.

class OutputCoordCounts(ArticleHandlerForUsefulText):
  def process_text_for_words(self, word_generator):
    wordhash = intdict()
    for word in word_generator:
      if word: wordhash[word] += 1
    output_reverse_sorted_table(wordhash, outfile=cur_output_file)

  def process_text_for_data(self, text):
    if extract_coordinates_from_article(text):
      output_title(self.title, self.id)
      return True
    return False

# Same as above but output counts for all articles, not just those with
# coordinates in them.

class OutputAllCounts(OutputCoordCounts):
  def process_text_for_data(self, text):
    output_title(self.title, self.id)
    return True

# Handler to output just coordinate information.
class OutputCoords(ArticleHandler):
  def process_text_for_data(self, text):
    return extract_and_output_coordinates_from_article(self.title, self.id,
                                                       text)

# Handler to try to determine the type of an article with coordinates.
class OutputLocationType(ArticleHandler):
  def process_text_for_data(self, text):
    iscoord = extract_and_output_coordinates_from_article(self.title, self.id,
                                                          text)
    if iscoord:
      extract_location_type(text)
    return iscoord


class ToponymEvalDataHandler(ExtractUsefulText):
  def join_arguments_as_generator(self, args_of_macro):
    first = True
    for chunk in args_of_macro:
      if not first: yield ' '
      first = False
      for chu in self.process_source_text(chunk):
        yield chu

  # OK, this is a bit tricky.  The definitions of process_template() and
  # process_internal_link() in ExtractUsefulText() use yield_template_args()
  # and yield_internal_link_args(), respectively, to yield arguments, and
  # then call process_source_text() to recursively process the arguments and
  # then join everything together into a string, with spaces between the
  # chunks corresponding to separate arguments.  The joining together
  # happens inside of process_and_join_arguments().  This runs into problems
  # if we have an internal link inside of another internal link, which often
  # happens with images, which are internal links that have an extra caption
  # argument, which frequently contains (nested) internal links.  The
  # reason is that we've overridden process_internal_link() to sometimes
  # return a tuple (which signals the outer handler that we found a link
  # of the appropriate sort), and the joining together chokes on non-string
  # arguments.  So instead, we "join" arguments by just yielding everything
  # in sequence, with spaces inserted as needed between arguments; this
  # happens in join_arguments_as_generator().  We specifically need to
  # override process_template() (and already override process_internal_link()),
  # because it's exactly those two that currently call
  # process_and_join_arguments().
  #
  # The idea is that we never join arguments together at any level of
  # recursion, but just yield chunks.  At the topmost level, we will join
  # as necessary and resplit for word boundaries.

  def process_template(self, text):
    for chunk in self.join_arguments_as_generator(yield_template_args(text)):
      yield chunk
  
  def process_internal_link(self, text):
    tempargs = get_macro_args(text)
    m = re.match(r'(?s)\s*([a-zA-Z0-9_]+)\s*:(.*)', tempargs[0])
    if m:
      # Something like [[Image:...]] or [[wikt:...]] or [[fr:...]]
      # For now, just skip them all; eventually, might want to do something
      # useful with some, e.g. categories
      pass
    else:
      article = capfirst(tempargs[0])
      # Skip links to articles without coordinates
      if coordinate_articles and article not in coordinate_articles:
        pass
      else:
        yield ('link', tempargs)
        return

    for chunk in self.join_arguments_as_generator(yield_internal_link_args(text)):
      yield chunk


class GenerateToponymEvalData(ArticleHandler):
  # Process the text itself, e.g. for words.  Input it text that has been
  # preprocessed as described above (remove comments, etc.).  Default
  # handler does two things:
  #
  # 1. Further process the text (see format_text_second_pass())
  # 2. Use process_source_text() to extract chunks of useful
  #    text.  Join together and then split into words.  Pass the generator
  #    of words to self.process_text_for_words().

  def process_text_for_text(self, text):
    # Now process the text in various ways in preparation for extracting
    # the words from the text
    text = format_text_second_pass(text)

    splitprint("Article title: %s" % self.title)
    chunkgen = ToponymEvalDataHandler().process_source_text(text)
    #for chunk in chunkgen:
    #  errprint("Saw chunk: %s" % (chunk,))
    # groupby() allows us to group all the non-link chunks (which are raw
    # strings) together efficiently
    for k, g in itertools.groupby(chunkgen,
                                  lambda chunk: type(chunk) is tuple):
      #args = [arg for arg in g]
      #errprint("Saw k=%s, g=%s" % (k,args))
      if k:
         for (linktext, linkargs) in g:
           splitprint("Link: %s" % '|'.join(linkargs))
      else:
        # Now process the resulting text into chunks.  Join them back together
        # again (to handle cases like "the [[latent variable]]s are ..."), and
        # split to find words.
        for word in split_text_into_words(''.join(g)):
          if word:
            splitprint("%s" % word)

# Generate article data of various sorts
class GenerateArticleData(ArticleHandler):
  def process_article(self, redirtitle):
    if rematch('(.*?):', self.title):
      namespace = m_[1]
      if namespace in article_namespace_aliases:
        namespace = article_namespace_aliases[namespace]
      elif namespace not in article_namespaces:
        namespace = 'Main'
    else:
      namespace = 'Main'
    yesno = {True:'yes', False:'no'}
    listof = self.title.startswith('List of ')
    disambig = self.id in disambig_pages_by_id
    nskey = article_namespace_aliases.get(namespace, namespace)
    list = listof or disambig or nskey in (14, 108) # ('Category', 'Book')
    outprint("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" %
             (self.id, self.title, cur_split_name, redirtitle, namespace,
              yesno[listof], yesno[disambig], yesno[list]))

  def process_redirect(self, redirtitle):
    self.process_article(capfirst(redirtitle))

  def process_text_for_data(self, text):
    self.process_article('')
    return False

# Handler to output link information as well as coordinate information.
# Note that a link consists of two parts: The anchor text and the article
# name.  For all links, we keep track of all the possible articles for a
# given anchor text and their counts.  We also count all of the incoming
# links to an article (can be used for computing prior probabilities of
# an article).

class ProcessSourceForCoordLinks(RecursiveSourceTextHandler):
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
      article = capfirst(tempargs[0])
      # Skip links to articles without coordinates
      if coordinate_articles and article not in coordinate_articles:
        pass
      else:
        anchor = ''.join(self.useful_text_handler.
                         process_source_text(tempargs[-1]))
        incoming_link_count[article] += 1
        if anchor not in anchor_text_map:
          nested_anchor_text_map = intdict()
          anchor_text_map[anchor] = nested_anchor_text_map
        else:
          nested_anchor_text_map = anchor_text_map[anchor]
        nested_anchor_text_map[article] += 1
 
    # Also recursively process all the arguments for links, etc.
    return self.process_source_text(text[2:-2])

class FindCoordLinks(ArticleHandler):
  def process_text_for_data(self, text):
    handler = ProcessSourceForCoordLinks()
    for foo in handler.process_source_text(text): pass
    return False

  def finish_processing(self):
    print "------------------ Count of incoming links: ---------------"
    output_reverse_sorted_table(incoming_link_count, outfile=cur_output_file)
  
    print "==========================================================="
    print "==========================================================="
    print "==========================================================="
    print ""
    for (anchor,map) in anchor_text_map.items():
      splitprint("-------- Anchor text->article for %s: " % anchor)
      output_reverse_sorted_table(map, outfile=cur_output_file)

#######################################################################
#                SAX handler for processing raw dump files            #
#######################################################################

class FinishParsing:
  pass

# We do a very simple-minded way of handling the XML.  We maintain the
# path of nested elements that we're within, and we track the text since the
# last time we saw the beginning of an element.  We reset the text we're
# tracking every time we see an element begin tag, and we don't record
# text at all after an end tag, until we see a begin tag again.  Basically,
# this means we don't handle cases where tags are nested inside of text.
# This isn't a problem since cases like this don't occur in the Wikipedia
# dump.

class WikipediaDumpSaxHandler(ContentHandler):
  '''SAX handler for processing Wikipedia dumps.  Note that SAX is a
simple interface for handling XML in a serial fashion (as opposed to a
DOM-type interface, which reads the entire XML file into memory and allows
it to be dynamically manipulated).  Given the size of the XML dump file
(around 25 GB uncompressed), we can't read it all into memory.'''
  def __init__(self, output_handler):
    errprint("Beginning processing of Wikipedia dump...")
    self.curpath = []
    self.curattrs = []
    self.curtext = None
    self.output_handler = output_handler
    self.status = StatusMessage('article')
    
  def startElement(self, name, attrs):
    '''Handler for beginning of XML element.'''
    if debug['sax']:
      errprint("startElement() saw %s/%s" % (name, attrs))
      for (key,val) in attrs.items(): errprint("  Attribute (%s,%s)" % (key,val))
    # We should never see an element inside of the Wikipedia text.
    if self.curpath:
      assert self.curpath[-1] != 'text'
    self.curpath.append(name)
    self.curattrs.append(attrs)
    self.curtext = []
    # We care about the title, ID, and redirect status.  Reset them for
    # every page; this is especially important for redirect status.
    if name == 'page':
      self.title = None
      self.id = None
      self.redirect = False

  def characters(self, text):
    '''Handler for chunks of text.  Accumulate all adjacent chunks.  When
the end element </text> is seen, process_article_text() will be called on the
combined chunks.'''
    if debug['sax']: errprint("characters() saw %s" % text)
    # None means the last directive we saw was an end tag; we don't track
    # text any more until the next begin tag.
    if self.curtext != None:
      self.curtext.append(text)
 
  def endElement(self, name):
    '''Handler for end of XML element.'''
    eltext = ''.join(self.curtext) if self.curtext else ''
    self.curtext = None # Stop tracking text
    self.curpath.pop()
    attrs = self.curattrs.pop()
    if name == 'title':
      self.title = eltext
    # ID's occur in three places: the page ID, revision ID and contributor ID.
    # We only want the page ID, so check to make sure we've got the right one.
    elif name == 'id' and self.curpath[-1] == 'page':
      self.id = eltext
    elif name == 'redirect':
      self.redirect = True
    elif name == 'namespace':
      key = attrs.getValue("key")
      if debug['sax']: errprint("Saw namespace, key=%s, eltext=%s" %
          (key, eltext))
      article_namespaces[eltext] = key
      article_namespaces_lower[eltext.lower()] = key
    elif name == 'text':
      # If we saw the end of the article text, join all the text chunks
      # together and call process_article_text() on it.
      set_next_split_file()
      if debug['lots']:
        max_text_len = 150
        endslice = min(max_text_len, len(eltext))
        truncated = len(eltext) > max_text_len
        errprint(
        """Calling process_article_text with title=%s, id=%s, redirect=%s;
  text=[%s%s]""" % (self.title, self.id, self.redirect, eltext[0:endslice],
                    "..." if truncated else ""))
      self.output_handler.process_article_text(text=eltext, title=self.title,
        id=self.id, redirect=self.redirect)
      if self.status.item_processed(maxtime=Opts.max_time_per_stage):
        raise FinishParsing()
 
#######################################################################
#                                Main code                            #
#######################################################################


def main_process_input(wiki_handler):
  ### Create the SAX parser and run it on stdin.
  sax_parser = make_parser()
  sax_handler = WikipediaDumpSaxHandler(wiki_handler)
  sax_parser.setContentHandler(sax_handler)
  try:
    sax_parser.parse(sys.stdin)
  except FinishParsing:
    pass
  wiki_handler.finish_processing()
  
def main():

  op = OptionParser(usage="%prog [options] < file")
  op.add_option("--output-all-words",
                help="Output words of text, for all articles.",
                action="store_true")
  op.add_option("--output-coord-words",
                help="Output text, but only for articles with coordinates.",
                action="store_true")
  op.add_option("--raw-text", help="""When outputting words, make output
resemble some concept of "raw text".  Currently, this just includes
punctuation instead of omitting it, and shows only the anchor text of a
link rather than both the anchor text and actual article name linked to,
when different.""", action="store_true")
  op.add_option("--no-tokenize", help="""When outputting words, don't tokenize.
This causes words to only be split on whitespace, rather than also on
punctuation.""", action="store_true")
  op.add_option("--find-coord-links",
                help="""Find all links and print info about them, for
articles with coordinates or redirects to such articles.  Includes count of
incoming links, and, for each anchor-text form, counts of all articles it
maps to.""",
                action="store_true")
  op.add_option("--output-all-counts",
                help="Print info about counts of words, for all articles.",
                action="store_true")
  op.add_option("--output-coord-counts",
                help="Print info about counts of words, but only for articles with coodinates.",
                action="store_true")
  op.add_option("--output-coords",
                help="Print info about coordinates of articles with coordinates.",
                action="store_true")
  op.add_option("--output-location-type",
                help="Print info about type of articles with coordinates.",
                action="store_true")
  op.add_option("--find-redirects",
                help="Output all redirects.",
                action="store_true")
  op.add_option("--generate-toponym-eval",
                help="Generate data files for use in toponym evaluation.",
                action="store_true")
  op.add_option("--generate-article-data",
                help="""Generate file listing all articles and info about them.
If using this option, the --disambig-id-file and --split-training-dev-test
options should also be used.

The format is

ID TITLE SPLIT REDIR NAMESPACE LIST-OF DISAMBIG LIST

where each field is separated by a tab character.

The fields are

ID = Numeric ID of article, given by wikiprep
TITLE = Title of article
SPLIT = Split to assign the article to; one of 'training', 'dev', or 'test'.
REDIR = If the article is a redirect, lists the article it redirects to;
        else, blank.
NAMESPACE = Namespace of the article, one of 'Main', 'User', 'Wikipedia',
            'File', 'MediaWiki', 'Template', 'Help', 'Category', 'Thread',
            'Summary', 'Portal', 'Book'.  These are the basic namespaces
            defined in [[Wikipedia:Namespace]].  Articles of the appropriate
            namespace begin with the namespace prefix, e.g. 'File:*', except
            for articles in the main namespace, which includes everything
            else.  Note that some of these namespaces don't actually appear
            in the article dump; likewise, talk pages don't appear in the
            dump.  In addition, we automatically include the common namespace
            abbreviations in the appropriate space, i.e.

            P               Portal
            H               Help
            T               Template
            CAT, Cat, C     Category
            MOS, MoS, Mos   Wikipedia (used for "Manual of Style" pages)
LIST-OF = 'yes' if article title is of the form 'List of *', typically
          containing a list; else 'no'.
DISAMBIG = 'yes' if article is a disambiguation page (used to disambiguate
           multiple concepts with the same name); else 'no'.
LIST = 'yes' if article is a list of some sort, else no.  This includes
       'List of' articles, disambiguation pages, and articles in the 'Category'
       and 'Book' namespaces.""",
                action="store_true")
  op.add_option("--split-training-dev-test",
                help="""Split output into training, dev and test files.
Use the specified value as the file prefix, suffixed with '.train', '.dev'
and '.test' respectively.""",
                metavar="FILE")
  op.add_option("--training-fraction", type='float', default=80,
                help="""Fraction of total articles to use for training.
The absolute amount doesn't matter, only the value relative to the test
and dev fractions, as the values are normalized.  Default %default.""",
                metavar="FRACTION")
  op.add_option("--dev-fraction", type='float', default=10,
                help="""Fraction of total articles to use for dev set.
The absolute amount doesn't matter, only the value relative to the training
and test fractions, as the values are normalized.  Default %default.""",
                metavar="FRACTION")
  op.add_option("--test-fraction", type='float', default=10,
                help="""Fraction of total articles to use for test set.
The absolute amount doesn't matter, only the value relative to the training
and dev fractions, as the values are normalized.  Default %default.""",
                metavar="FRACTION")
  op.add_option("--coords-file",
                help="""File containing output from a prior run of
--coords-counts, listing all the articles with associated coordinates.
This is used to limit the operation of --find-coord-links to only consider
links to articles with coordinates.  Currently, if this is not done, then
using --coords-file requires at least 10GB, perhaps more, of memory in order
to store the entire table of anchor->article mappings in memory. (If this
entire table is needed, it may be necessary to implement a MapReduce-style
process where smaller chunks are processed separately and then the results
combined.)""",
                metavar="FILE")
  op.add_option("--article-data-file",
                help="""File containing article data.  Used by
--find-coord-links to find the redirects pointing to articles with
coordinates.""",
                metavar="FILE")
  op.add_option("--disambig-id-file",
                help="""File containing list of article ID's that are
disambiguation pages.""",
                metavar="FILE")
  op.add_option("--max-time-per-stage", "--mts", type='int', default=0,
                help="""Maximum time per stage in seconds.  If 0, no limit.
Used for testing purposes.  Default %default.""")
  op.add_option("--debug", metavar="FLAGS",
                help="Output debug info of the given types (separated by spaces or commas)")

  errprint("Arguments: %s" % ' '.join(sys.argv))
  opts, args = op.parse_args()
  output_option_parameters(opts)

  global Opts
  Opts = opts

  global debug
  if opts.debug:
    flags = re.split(r'[,\s]+', opts.debug)
    for f in flags:
      debug[f] = True
  if debug['err'] or debug['some'] or debug['lots'] or debug['sax']:
    cur_output_file = sys.stderr
    debug_to_stderr = True

  if opts.split_training_dev_test:
    init_output_files(opts.split_training_dev_test,
                      [opts.training_fraction, opts.dev_fraction,
                       opts.test_fraction],
                      ['training', 'dev', 'test'])

  if opts.coords_file:
    read_coordinates_file(opts.coords_file)    
  if opts.article_data_file:
    read_redirects_from_article_data(opts.article_data_file)
  if opts.disambig_id_file:
    read_disambig_id_file(opts.disambig_id_file)
  if opts.output_all_words:
    main_process_input(OutputAllWords())
  elif opts.output_coord_words:
    main_process_input(OutputCoordWords())
  elif opts.find_coord_links:
    main_process_input(FindCoordLinks())
  elif opts.find_redirects:
    main_process_input(FindRedirects())
  elif opts.output_coords:
    main_process_input(OutputCoords())
  elif opts.output_all_counts:
    main_process_input(OutputAllCounts())
  elif opts.output_coord_counts:
    main_process_input(OutputCoordCounts())
  elif opts.output_location_type:
    main_process_input(OutputLocationType())
  elif opts.generate_toponym_eval:
    main_process_input(GenerateToponymEvalData())
  elif opts.generate_article_data:
    outprint('id\ttitle\tsplit\tredir\tnamespace\tis_list_of\tis_disambig\tis_list')
    main_process_input(GenerateArticleData())

#import cProfile
#cProfile.run('main()', 'process-wiki.prof')
main()
