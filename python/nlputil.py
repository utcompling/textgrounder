from __future__ import with_statement # For chompopen(), uchompopen()
from optparse import OptionParser
from itertools import *
import itertools
import re # For regexp wrappers
import sys, codecs # For uchompopen()
import math # For float_with_commas()
import bisect # For sorted lists
import time # For status messages, resource usage
from heapq import * # For priority queue
import UserDict # For SortedList, LRUCache
import resource # For resource usage
from collections import deque # For breadth-first search
from subprocess import * # For backquote
from errno import * # For backquote
import os # For get_program_memory_usage_ps()
import os.path # For exists of /proc, etc.
import fileinput # For uchompopen() etc.

#############################################################################
#                        Regular expression functions                       #
#############################################################################


#### Some simple wrappers around basic text-processing Python functions to
#### make them easier to use.
####
#### 1. rematch() and research():
####
#### The functions 'rematch' and 'research' are wrappers around re.match()
#### and re.search(), respectively, but instead of returning a match object
#### None, they return True or False, and in the case a match object would
#### have been returned, a corresponding WreMatch object is stored in the
#### global variable m_.  Groups can be accessed from this variable using
#### m_.group() or m_.groups(), but they can also be accessed through direct
#### subscripting, i.e. m_[###] = m_.group(###).

class WreMatch(object):
  def setmatch(self, match):
    self.match = match

  def groups(self, *foo):
    return self.match.groups(*foo)

  def group(self, *foo):
    return self.match.group(*foo)

  def __getitem__(self, key):
    return self.match.group(key)

m_ = WreMatch()

def rematch(pattern, string, flags=0):
  m = re.match(pattern, string, flags)
  if m:
    m_.setmatch(m)
    return True
  return False

def research(pattern, string, flags=0):
  global m_
  m = re.search(pattern, string, flags)
  if m:
    m_.setmatch(m)
    return True
  return False

#############################################################################
#                            File reading functions                         #
#############################################################################

### NOTE NOTE NOTE: Only works on Python 2.5 and above, due to using the
### "with" statement.

#### 1. chompopen():
####
#### A generator that yields lines from a file, with any terminating newline
#### removed (but no other whitespace removed).  Ensures that the file
#### will be automatically closed under all circumstances.
####
#### 2. uchompopen():
####
#### Same as chompopen() but specifically open the file as 'utf-8' and
#### return Unicode strings.

#"""
#Test gopen
#
#import nlputil
#for line in nlputil.gopen("foo.txt"):
#  print line
#for line in nlputil.gopen("foo.txt", chomp=True):
#  print line
#for line in nlputil.gopen("foo.txt", encoding='utf-8'):
#  print line
#for line in nlputil.gopen("foo.txt", encoding='utf-8', chomp=True):
#  print line
#for line in nlputil.gopen("foo.txt", encoding='iso-8859-1'):
#  print line
#for line in nlputil.gopen(["foo.txt"], encoding='iso-8859-1'):
#  print line
#for line in nlputil.gopen(["foo.txt"], encoding='utf-8'):
#  print line
#for line in nlputil.gopen(["foo.txt"], encoding='iso-8859-1', chomp=True):
#  print line
#for line in nlputil.gopen(["foo.txt", "foo2.txt"], encoding='iso-8859-1', chomp=True):
#  print line
#"""

# General function for opening a file, with automatic closure after iterating
# through the lines.  The encoding can be specified (e.g. 'utf-8'), and if so,
# the error-handling can be given.  Whether to remove the final newline
# (chomp=True) can be specified.  The filename can be either a regular
# filename (opened with open) or codecs.open(), or a list of filenames or
# None, in which case the argument is passed to fileinput.input()
# (if a non-empty list is given, opens the list of filenames one after the
# other; if an empty list is given, opens stdin; if None is given, takes
# list from the command-line arguments and proceeds as above).  When using
# fileinput.input(), the arguments "inplace", "backup" and "bufsize" can be
# given, appropriate to that function (e.g. to do in-place filtering of a
# file).  In all cases, 
def gopen(filename, mode='r', encoding=None, errors='strict', chomp=False,
    inplace=0, backup="", bufsize=0):
  if isinstance(filename, basestring):
    def yieldlines():
      if encoding is None:
        mgr = open(filename)
      else:
        mgr = codecs.open(filename, mode, encoding=encoding, errors=errors)
      with mgr as f:
        for line in f:
          yield line
    iterator = yieldlines()
  else:
    if encoding is None:
      openhook = None
    else:
      def openhook(filename, mode):
        return codecs.open(filename, mode, encoding=encoding, errors=errors)
    iterator = fileinput.input(filename, inplace=inplace, backup=backup,
        bufsize=bufsize, mode=mode, openhook=openhook)
  if chomp:
    for line in iterator:
      if line and line[-1] == '\n': line = line[:-1]
      yield line
  else:
    for line in iterator:
      yield line

# Open a filename with UTF-8-encoded input and yield lines converted to
# Unicode strings, but with any terminating newline removed (similar to
# "chomp" in Perl).  Basically same as gopen() but with defaults set
# differently.
def uchompopen(filename=None, mode='r', encoding='utf-8', errors='strict',
    chomp=True, inplace=0, backup="", bufsize=0):
  return gopen(filename, mode=mode, encoding=encoding, errors=errors,
      chomp=chomp, inplace=inplace, backup=backup, bufsize=bufsize)

# Open a filename and yield lines, but with any terminating newline
# removed (similar to "chomp" in Perl).  Basically same as gopen() but
# with defaults set differently.
def chompopen(filename, mode='r', encoding=None, errors='strict',
    chomp=True, inplace=0, backup="", bufsize=0):
  return gopen(filename, mode=mode, encoding=encoding, errors=errors,
      chomp=chomp, inplace=inplace, backup=backup, bufsize=bufsize)

# Open a filename with UTF-8-encoded input.  Basically same as gopen()
# but with defaults set differently.
def uopen(filename, mode='r', encoding='utf-8', errors='strict',
    chomp=False, inplace=0, backup="", bufsize=0):
  return gopen(filename, mode=mode, encoding=encoding, errors=errors,
      chomp=chomp, inplace=inplace, backup=backup, bufsize=bufsize)

#############################################################################
#                         Other basic utility functions                     #
#############################################################################

def internasc(text):
  '''Intern a string (for more efficient memory use, potentially faster lookup.
If string is Unicode, automatically convert to UTF-8.'''
  if type(text) is unicode: text = text.encode("utf-8")
  return intern(text)

def uniprint(text, outfile=sys.stdout, nonl=False, flush=False):
  '''Print text string using 'print', converting Unicode as necessary.
If string is Unicode, automatically convert to UTF-8, so it can be output
without errors.  Send output to the file given in OUTFILE (default is
stdout).  Uses the 'print' command, and normally outputs a newline; but
this can be suppressed using NONL.  Output is not normally flushed (unless
the stream does this automatically); but this can be forced using FLUSH.'''
  
  if type(text) is unicode:
    text = text.encode("utf-8")
  if nonl:
    print >>outfile, text,
  else:
    print >>outfile, text
  if flush:
    outfile.flush()

def uniout(text, outfile=sys.stdout, flush=False):
  '''Output text string, converting Unicode as necessary.
If string is Unicode, automatically convert to UTF-8, so it can be output
without errors.  Send output to the file given in OUTFILE (default is
stdout).  Uses the write() function, which outputs the text directly,
without adding spaces or newlines.  Output is not normally flushed (unless
the stream does this automatically); but this can be forced using FLUSH.'''
  
  if type(text) is unicode:
    text = text.encode("utf-8")
  outfile.write(text)
  if flush:
    outfile.flush()

def errprint(text, nonl=False):
  '''Print text to stderr using 'print', converting Unicode as necessary.
If string is Unicode, automatically convert to UTF-8, so it can be output
without errors.  Uses the 'print' command, and normally outputs a newline; but
this can be suppressed using NONL.'''
  uniprint(text, outfile=sys.stderr, nonl=nonl)

def errout(text):
  '''Output text to stderr, converting Unicode as necessary.
If string is Unicode, automatically convert to UTF-8, so it can be output
without errors.  Uses the write() function, which outputs the text directly,
without adding spaces or newlines.'''
  uniout(text, outfile=sys.stderr)

def warning(text):
  '''Output a warning, formatting into UTF-8 as necessary'''
  errprint("Warning: %s" % text)

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

def pluralize(word):
  '''Pluralize an English word, using a basic but effective algorithm.'''
  if word[-1] >= 'A' and word[-1] <= 'Z': upper = True
  else: upper = False
  lowerword = word.lower()
  if re.match(r'.*[b-df-hj-np-tv-z]y$', lowerword):
    if upper: return word[:-1] + 'IES'
    else: return word[:-1] + 'ies'
  elif re.match(r'.*([cs]h|[sx])$', lowerword):
    if upper: return word + 'ES'
    else: return word + 'es'
  else:
    if upper: return word + 'S'
    else: return word + 's'

def capfirst(st):
  '''Capitalize the first letter of string, leaving the remainder alone.'''
  if not st: return st
  return st[0].capitalize() + st[1:]

# From: http://stackoverflow.com/questions/1823058/how-to-print-number-with-commas-as-thousands-separators-in-python-2-x
def int_with_commas(x):
  if type(x) not in [type(0), type(0L)]:
    raise TypeError("Parameter must be an integer.")
  if x < 0:
    return '-' + int_with_commas(-x)
  result = ''
  while x >= 1000:
    x, r = divmod(x, 1000)
    result = ",%03d%s" % (r, result)
  return "%d%s" % (x, result)

# My own version
def float_with_commas(x):
  intpart = int(math.floor(x))
  fracpart = x - intpart
  return int_with_commas(intpart) + ("%.2f" % fracpart)[1:]

def median(list):
  "Return the median value of a sorted list."
  l = len(list)
  if l % 2 == 1:
    return list[l // 2]
  else:
    l = l // 2
    return 0.5*(list[l-1] + list[l])

def mean(list):
  "Return the mean of a list."
  return sum(list) / float(len(list))

def split_text_into_words(text, ignore_punc=False, include_nl=False):
  # This regexp splits on whitespace, but also handles the following cases:
  # 1. Any of , ; . etc. at the end of a word
  # 2. Parens or quotes in words like (foo) or "bar"
  # These punctuation characters are returned as separate words, unless
  # 'ignore_punc' is given.  Also, if 'include_nl' is given, newlines are
  # returned as their own words; otherwise, they are treated like all other
  # whitespace (i.e. ignored).
  if include_nl:
    split_punc_re = r'[ \t]+'
  else:
    split_punc_re = r'\s+'
  # The use of izip and cycle will pair True with return values that come
  # from the grouping in the split re, and False with regular words.
  for (ispunc, word) in izip(cycle([False, True]),
                  re.split('([,;."):]*\s+[("]*)', text)):
    if not word: continue
    if ispunc:
      # Divide the punctuation up 
      for punc in word:
        if punc == '\n':
          if include_nl: yield punc
        elif punc in ' \t\r\f\v': continue
        elif not ignore_punc: yield punc
    else:
      yield word

def fromto(fro, to, step=1):
  if fro <= to:
    step = abs(step)
    to += 1
  else:
    step = -abs(step)
    to -= 1
  return xrange(fro, to, step)

#############################################################################
#                             Default dictionaries                          #
#############################################################################

# Our own version similar to collections.defaultdict().  The difference is
# that we can specify whether or not simply referencing an unseen key
# automatically causes the key to permanently spring into existence with
# the "missing" value.  collections.defaultdict() always behaves as if
# 'add_upon_ref'=True, same as our default.  Basically:
#
# foo = defdict(list, add_upon_ref=False)
# foo['bar']          -> []
# 'bar' in foo        -> False
#
# foo = defdict(list, add_upon_ref=True)
# foo['bar']          -> []
# 'bar' in foo        -> True
#
# The former may be useful where you may make many queries involving
# non-existent keys, and you don't want all these keys added to the dict.
# The latter is useful with mutable objects like lists.  If I create
#
#   foo = defdict(list, add_upon_ref=False)
#
# and then call
#
#   foo['glorplebargle'].append('shazbat')
#
# where 'glorplebargle' is a previously non-existent key, the call to
# 'append' will "appear" to work but in fact nothing will happen, because
# the reference foo['glorplebargle'] will create a new list and return
# it, but not store it in the dict, and 'append' will add to this
# temporary list, which will soon disappear.  Note that using += will
# actually work, but this is fragile behavior, not something to depend on.
#
class defdict(dict):
  def __init__(self, factory, add_upon_ref=True):
    super(defdict, self).__init__()
    self.factory = factory
    self.add_upon_ref = add_upon_ref

  def __missing__(self, key):
    val = self.factory()
    if self.add_upon_ref:
      self[key] = val
    return val


# A dictionary where asking for the value of a missing key causes 0 (or 0.0,
# etc.) to be returned.  Useful for dictionaries that track counts of items.

def intdict():
  return defdict(int, add_upon_ref=False)

def floatdict():
  return defdict(float, add_upon_ref=False)

def booldict():
  return defdict(float, add_upon_ref=False)

# Similar but the default value is an empty collection.  We set
# 'add_upon_ref' to True whenever the collection is mutable; see comments
# above.

def listdict():
  return defdict(list, add_upon_ref=True)

def strdict():
  return defdict(str, add_upon_ref=False)

def dictdict():
  return defdict(dict, add_upon_ref=True)

def tupledict():
  return defdict(tuple, add_upon_ref=False)

def setdict():
  return defdict(set, add_upon_ref=True)

#############################################################################
#                                 Sorted lists                              #
#############################################################################

# Return a tuple (keys, values) of lists of items corresponding to a hash
# table.  Stored in sorted order according to the keys.  Use
# lookup_sorted_list(key) to find the corresponding value.  The purpose of
# doing this, rather than just directly using a hash table, is to save
# memory.

def make_sorted_list(table):
  items = sorted(table.items(), key=lambda x:x[0])
  keys = ['']*len(items)
  values = ['']*len(items)
  for i in xrange(len(items)):
    item = items[i]
    keys[i] = item[0]
    values[i] = item[1]
  return (keys, values)

# Given a sorted list in the tuple form (KEYS, VALUES), look up the item KEY.
# If found, return the corresponding value; else return None.

def lookup_sorted_list(sorted_list, key, default=None):
  (keys, values) = sorted_list
  i = bisect.bisect_left(keys, key)
  if i != len(keys) and keys[i] == key:
    return values[i]
  return default

# A class that provides a dictionary-compatible interface to a sorted list

class SortedList(object, UserDict.DictMixin):
  def __init__(self, table):
    self.sorted_list = make_sorted_list(table)

  def __len__(self):
    return len(self.sorted_list[0])

  def __getitem__(self, key):
    retval = lookup_sorted_list(self.sorted_list, key)
    if retval is None:
      raise KeyError(key)
    return retval

  def __contains__(self, key):
    return lookup_sorted_list(self.sorted_list, key) is not None

  def __iter__(self):
    (keys, values) = self.sorted_list
    for x in keys:
      yield x

  def keys(self):
    return self.sorted_list[0]

  def itervalues(self):
    (keys, values) = self.sorted_list
    for x in values:
      yield x

  def iteritems(self):
    (keys, values) = self.sorted_list
    for (key, value) in izip(keys, values):
      yield (key, value)

#############################################################################
#                                Table Output                               #
#############################################################################

def key_sorted_items(d):
  return sorted(d.iteritems(), key=lambda x:x[0])

def value_sorted_items(d):
  return sorted(d.iteritems(), key=lambda x:x[1])

def reverse_key_sorted_items(d):
  return sorted(d.iteritems(), key=lambda x:x[0], reverse=True)

def reverse_value_sorted_items(d):
  return sorted(d.iteritems(), key=lambda x:x[1], reverse=True)

# Given a list of tuples, where the second element of the tuple is a number and
# the first a key, output the list, sorted on the numbers from bigger to
# smaller.  Within a given number, sort the items alphabetically, unless
# keep_secondary_order is True, in which case the original order of items is
# left.  If 'outfile' is specified, send output to this stream instead of
# stdout.  If 'indent' is specified, indent all rows by this string (usually
# some number of spaces).  If 'maxrows' is specified, output at most this many
# rows.
def output_reverse_sorted_list(items, outfile=sys.stdout, indent="",
    keep_secondary_order=False, maxrows=None):
  if not keep_secondary_order:
    items = sorted(items, key=lambda x:x[0])
  items = sorted(items, key=lambda x:x[1], reverse=True)
  if maxrows:
    items = items[0:maxrows]
  for key, value in items:
    uniprint("%s%s = %s" % (indent, key, value), outfile=outfile)

# Given a table with values that are numbers, output the table, sorted
# on the numbers from bigger to smaller.  Within a given number, sort the
# items alphabetically, unless keep_secondary_order is True, in which case
# the original order of items is left.  If 'outfile' is specified, send
# output to this stream instead of stdout.  If 'indent' is specified, indent
# all rows by this string (usually some number of spaces).  If 'maxrows'
# is specified, output at most this many rows.
def output_reverse_sorted_table(table, outfile=sys.stdout, indent="",
    keep_secondary_order=False, maxrows=None):
  output_reverse_sorted_list(table.iteritems())

#############################################################################
#                             Status Messages                               #
#############################################################################

# Output status messages periodically, at some multiple of
# 'secs_between_output', measured in real time. 'item_name' is the name
# of the items being processed.  Every time an item is processed, call
# item_processed()
class StatusMessage(object):
  def __init__(self, item_name, secs_between_output=15):
    self.item_name = item_name
    self.plural_item_name = pluralize(item_name)
    self.secs_between_output = secs_between_output
    self.items_processed = 0
    self.first_time = time.time()
    self.last_time = self.first_time

  def num_processed(self):
    return self.items_processed

  def elapsed_time(self):
    return time.time() - self.first_time

  def item_unit(self):
    if self.items_processed == 1:
      return self.item_name
    else:
      return self.plural_item_name

  def item_processed(self, maxtime=0):
    curtime = time.time()
    self.items_processed += 1
    total_elapsed_secs = int(curtime - self.first_time)
    last_elapsed_secs = int(curtime - self.last_time)
    if last_elapsed_secs >= self.secs_between_output:
      # Rather than directly recording the time, round it down to the nearest
      # multiple of secs_between_output; else we will eventually see something
      # like 0, 15, 45, 60, 76, 91, 107, 122, ...
      # rather than
      # like 0, 15, 45, 60, 76, 90, 106, 120, ...
      rounded_elapsed = (int(total_elapsed_secs / self.secs_between_output) *
                         self.secs_between_output)
      self.last_time = self.first_time + rounded_elapsed
      errprint("Elapsed time: %s minutes %s seconds, %s %s processed"
               % (int(total_elapsed_secs / 60), total_elapsed_secs % 60,
                  self.items_processed, self.item_unit()))
    if maxtime and total_elapsed_secs >= maxtime:
      errprint("Maximum time reached, interrupting processing after %s %s"
               % (self.items_processed, self.item_unit()))
      return True
    return False
 
#############################################################################
#                               File Splitting                              #
#############################################################################

# Return the next file to output to, when the instances being output to the
# files are meant to be split according to SPLIT_FRACTIONS.  The absolute
# quantities in SPLIT_FRACTIONS don't matter, only the values relative to
# the other values, i.e. [20, 60, 10] is the same as [4, 12, 2].  This
# function implements an algorithm that is deterministic (same results
# each time it is run), and spreads out the instances as much as possible.
# For example, if all values are equal, it will cycle successively through
# the different split files; if the values are [1, 1.5, 1], the output
# will be [1, 2, 3, 2, 1, 2, 3, ...]; etc.

def next_split_set(split_fractions):

  num_splits = len(split_fractions)
  cumulative_articles = [0]*num_splits

  # Normalize so that the smallest value is 1.

  minval = min(split_fractions)
  split_fractions = [float(val)/minval for val in split_fractions]

  # The algorithm used is as follows.  We cycle through the output sets in
  # order; each time we return a set, we increment the corresponding
  # cumulative count, but before returning a set, we check to see if the
  # count has reached the corresponding fraction and skip this set if so.
  # If we have run through an entire cycle without returning any sets,
  # then for each set we subtract the fraction value from the cumulative
  # value.  This way, if the fraction value is not a whole number, then
  # any fractional quantity (e.g. 0.6 for a value of 7.6) is left over,
  # any will ensure that the total ratios still work out appropriately.

  while True:
    this_output = False
    for j in xrange(num_splits):
      #print "j=%s, this_output=%s" % (j, this_output)
      if cumulative_articles[j] < split_fractions[j]:
        yield j
        cumulative_articles[j] += 1
        this_output = True
    if not this_output:
      for j in xrange(num_splits):
        while cumulative_articles[j] >= split_fractions[j]:
          cumulative_articles[j] -= split_fractions[j]

#############################################################################
#                               NLP Programs                                #
#############################################################################

def output_option_parameters(opts, params=None):
  errprint("Parameter values:")
  for opt in dir(opts):
    if not opt.startswith('_') and opt not in \
       ['ensure_value', 'read_file', 'read_module']:
      errprint("%30s: %s" % (opt, getattr(opts, opt)))
  if params:
    for opt in dir(params):
      if not opt.startswith('_'):
        errprint("%30s: %s" % (opt, getattr(params, opt)))
  errprint("")

class NLPProgram(object):
  def __init__(self):
    if self.run_main_on_init():
      self.main()

  def implement_main(self, opts, params, args):
    pass

  def populate_options(self, op):
    pass

  def handle_arguments(self, opts, op, args):
    pass

  def argument_usage(self):
    return ""

  def get_usage(self):
    argusage = self.argument_usage()
    if argusage:
      argusage = ' ' + argusage
    return "%%prog [options]%s" % argusage

  def run_main_on_init(self):
    return True

  def populate_shared_options(self, op):
    op.add_option("--max-time-per-stage", "--mts", type='int', default=0,
                  help="""Maximum time per stage in seconds.  If 0, no limit.
Used for testing purposes.  Default %default.""")
    op.add_option("-d", "--debug", metavar="FLAGS",
                  help="Output debug info of the given types (separated by spaces or commas)")

  def need(self, arg, arg_english=None):
    if not arg_english:
      arg_english=arg.replace('_', ' ')
    if not getattr(self.opts, arg):
      self.op.error("Must specify %s using --%s" %
                    (arg_english, arg.replace('_', '-')))

  def main(self):
    errprint("Beginning operation at %s" % (time.ctime()))

    self.op = OptionParser(usage="%prog [options]")
    self.populate_shared_options(self.op)
    self.canon_options = self.populate_options(self.op)

    errprint("Arguments: %s" % ' '.join(sys.argv))

    ### Process the command-line options and set other values from them ###
    
    self.opts, self.args = self.op.parse_args()
    # If a mapper for canonicalizing options is given, apply the mappings
    if self.canon_options:
      for (opt, mapper) in self.canon_options.iteritems():
        val = getattr(self.opts, opt)
        if isinstance(val, list):
          # If a list, then map all members, in case of multi-options
          val = [mapper.get(x, x) for x in val]
          setattr(self.opts, opt, val)
        elif val in mapper:
          setattr(self.opts, opt, mapper[val])

    params = self.handle_arguments(self.opts, self.op, self.args)

    output_option_parameters(self.opts, params)

    retval = self.implement_main(self.opts, params, self.args)
    errprint("Ending operation at %s" % (time.ctime()))
    return retval

#############################################################################
#                               Priority Queues                             #
#############################################################################

# Priority queue implementation, based on Python heapq documentation.
# Note that in Python 2.6 and on, there is a priority queue implementation
# in the Queue module.
class PriorityQueue(object):
  INVALID = 0                     # mark an entry as deleted

  def __init__(self):
    self.pq = []                         # the priority queue list
    self.counter = itertools.count(1)    # unique sequence count
    self.task_finder = {}                # mapping of tasks to entries

  def add_task(self, priority, task, count=None):
    if count is None:
      count = self.counter.next()
    entry = [priority, count, task]
    self.task_finder[task] = entry
    heappush(self.pq, entry)

  #Return the top-priority task. If 'return_priority' is false, just
  #return the task itself; otherwise, return a tuple (task, priority).
  def get_top_priority(self, return_priority=False):
    while True:
      priority, count, task = heappop(self.pq)
      if count is not PriorityQueue.INVALID:
        del self.task_finder[task]
        if return_priority:
          return (task, priority)
        else:
          return task

  def delete_task(self, task):
    entry = self.task_finder[task]
    entry[1] = PriorityQueue.INVALID

  def reprioritize(self, priority, task):
    entry = self.task_finder[task]
    self.add_task(priority, task, entry[1])
    entry[1] = PriorityQueue.INVALID

#############################################################################
#                      Least-recently-used (LRU) Caches                     #
#############################################################################

class LRUCache(object, UserDict.DictMixin):
  def __init__(self, maxsize=1000):
    self.cache = {}
    self.pq = PriorityQueue()
    self.maxsize = maxsize
    self.time = 0

  def __len__(self):
    return len(self.cache)

  def __getitem__(self, key):
    if key in self.cache:
      time = self.time
      self.time += 1
      self.pq.reprioritize(time, key)
    return self.cache[key]

  def __delitem__(self, key):
    del self.cache[key]
    self.pq.delete_task(key)

  def __setitem__(self, key, value):
    time = self.time
    self.time += 1
    if key in self.cache:
      self.pq.reprioritize(time, key)
    else:
      while len(self.cache) >= self.maxsize:
        delkey = self.pq.get_top_priority()
        del self.cache[delkey]
      self.pq.add_task(time, key)
    self.cache[key] = value

  def keys(self):
    return self.cache.keys()

  def __contains__(self, key):
    return key in self.cache

  def __iter__(self):
    return self.cache.iterkeys()

  def iteritems(self):
    return self.cache.iteritems()

#############################################################################
#                               Resource Usage                              #
#############################################################################

beginning_prog_time = time.time()

def get_program_time_usage():
  return time.time() - beginning_prog_time

def get_program_memory_usage():
  if os.path.exists("/proc/self/status"):
    return get_program_memory_usage_proc()
  else:
    try:
      return get_program_memory_usage_ps()
    except:
      return get_program_memory_rusage()


def get_program_memory_usage_rusage():
  res = resource.getrusage(resource.RUSAGE_SELF)
  # FIXME!  This is "maximum resident set size".  There are other more useful
  # values, but on the Mac at least they show up as 0 in this structure.
  # On Linux, alas, all values show up as 0 or garbage (e.g. negative).
  return res.ru_maxrss

# Get memory usage by running 'ps'; getrusage() doesn't seem to work very
# well.  The following seems to work on both Mac OS X and Linux, at least.
def get_program_memory_usage_ps():
  pid = os.getpid()
  input = backquote("ps -p %s -o rss" % pid)
  lines = re.split(r'\n', input)
  for line in lines:
    if line.strip() == 'RSS': continue
    return 1024*int(line.strip())

# Get memory usage by running 'proc'; this works on Linux and doesn't require
# spawning a subprocess, which can crash when your program is very large.
def get_program_memory_usage_proc():
  with open("/proc/self/status") as f:
    for line in f:
      line = line.strip()
      if line.startswith('VmRSS:'):
        rss = int(line.split()[1])
        return 1024*rss
  return 0

def format_minutes_seconds(secs):
  mins = int(secs / 60)
  secs = secs % 60
  hours = int(mins / 60)
  mins = mins % 60
  if hours > 0:
    hourstr = "%s hour%s " % (hours, "" if hours == 1 else "s")
  else:
    hourstr = ""
  secstr = "%s" % secs if type(secs) is int else "%0.1f" % secs
  return hourstr + "%s minute%s %s second%s" % (
      mins, "" if mins == 1 else "s",
      secstr, "" if secs == 1 else "s")

def output_resource_usage():
  errprint("Total elapsed time since program start: %s" %
           format_minutes_seconds(get_program_time_usage()))
  errprint("Memory usage: %s bytes" %
      int_with_commas(get_program_memory_usage()))

#############################################################################
#                             Hash tables by range                          #
#############################################################################

# A table that groups all keys in a specific range together.  Instead of
# directly storing the values for a group of keys, we store an object (termed a
# "collector") that the user can use to keep track of the keys and values.
# This way, the user can choose to use a list of values, a set of values, a
# table of keys and values, etc.

class TableByRange(object):
  # Create a new object. 'ranges' is a sorted list of numbers, indicating the
  # boundaries of the ranges.  One range includes all keys that are
  # numerically below the first number, one range includes all keys that are
  # at or above the last number, and there is a range going from each number
  # up to, but not including, the next number.  'collector' is used to create
  # the collectors used to keep track of keys and values within each range;
  # it is either a type or a no-argument factory function.  We only create
  # ranges and collectors as needed. 'lowest_bound' is the value of the
  # lower bound of the lowest range; default is 0.  This is used only
  # it iter_ranges() when returning the lower bound of the lowest range,
  # and can be an item of any type, e.g. the number 0, the string "-infinity",
  # etc.
  def __init__(self, ranges, collector, lowest_bound=0):
    self.ranges = ranges
    self.collector = collector
    self.lowest_bound = lowest_bound
    self.items_by_range = {}

  def get_collector(self, key):
    lower_range = self.lowest_bound
    # upper_range = 'infinity'
    for i in self.ranges:
      if i <= key:
        lower_range = i
      else:
        # upper_range = i
        break
    if lower_range not in self.items_by_range:
      self.items_by_range[lower_range] = self.collector()
    return self.items_by_range[lower_range]

  def iter_ranges(self, unseen_between=True, unseen_all=False):
    """Return an iterator over ranges in the table.  Each returned value is
a tuple (LOWER, UPPER, COLLECTOR), giving the lower and upper bounds
(inclusive and exclusive, respectively), and the collector item for this
range.  The lower bound of the lowest range comes from the value of
'lowest_bound' specified during creation, and the upper bound of the range
that is higher than any numbers specified during creation in the 'ranges'
list will be the string "infinity" is such a range is returned.

The optional arguments 'unseen_between' and 'unseen_all' control the
behavior of this iterator with respect to ranges that have never been seen
(i.e. no keys in this range have been passed to 'get_collector').  If
'unseen_all' is true, all such ranges will be returned; else if
'unseen_between' is true, only ranges between the lowest and highest
actually-seen ranges will be returned."""
    highest_seen = None
    for (lower, upper) in (
        izip(chain([self.lowest_bound], self.ranges),
             chain(self.ranges, ['infinity']))):
      if lower in self.items_by_range:
        highest_seen = upper

    seen_any = False
    for (lower, upper) in (
        izip(chain([self.lowest_bound], self.ranges),
             chain(self.ranges, ['infinity']))):
      collector = self.items_by_range.get(lower, None)
      if collector is None:
        if not unseen_all:
          if not unseen_between: continue
          if not seen_any: continue
          if upper == 'infinity' or upper > highest_seen: continue
        collector = self.collector()
      else:
        seen_any = True
      yield (lower, upper, collector)


#############################################################################
#                          Depth-, breadth-first search                     #
#############################################################################

# General depth-first search.  'node' is the node to search, the top of a
# tree.  'matches' indicates whether a given node matches.  'children'
# returns a list of child nodes.
def depth_first_search(node, matches, children):
  nodelist = [node]
  while len(nodelist) > 0:
    node = nodelist.pop()
    if matches(node):
      yield node
    nodelist.extend(reversed(children(node)))

# General breadth-first search.  'node' is the node to search, the top of a
# tree.  'matches' indicates whether a given node matches.  'children'
# returns a list of child nodes.
def breadth_first_search(node, matches, children):
  nodelist = deque([node])
  while len(nodelist) > 0:
    node = nodelist.popLeft()
    if matches(node):
      yield node
    nodelist.extend(children(node))

#############################################################################
#                               Merge sequences                             #
#############################################################################

# Return an iterator over all elements in all the given sequences, omitting
# elements seen more than once and keeping the order.
def merge_sequences_uniquely(*seqs):
  table = {}
  for seq in seqs:
    for s in seq:
      if s not in table:
        table[s] = True
        yield s


#############################################################################
#                                Subprocesses                               #
#############################################################################

# Run the specified command; return its combined output and stderr as a string.
# 'command' can either be a string or a list of individual arguments.  Optional
# argument 'shell' indicates whether to pass the command to the shell to run.
# If unspecified, it defaults to True if 'command' is a string, False if a
# list.  If optional arg 'input' is given, pass this string as the stdin to the
# command.  If 'include_stderr' is True, stderr will be included along with
# the output.  If return code is non-zero, throw CommandError if 'throw' is
# specified; else, return tuple of (output, return-code).
def backquote(command, input=None, shell=None, include_stderr=True, throw=True):
  #logdebug("backquote called: %s" % command)
  if shell is None:
    if isinstance(command, basestring):
      shell = True
    else:
      shell = False
  stderrval = STDOUT if include_stderr else PIPE
  if input is not None:
    popen = Popen(command, stdin=PIPE, stdout=PIPE, stderr=stderrval,
                  shell=shell, close_fds=True)
    output = popen.communicate(input)
  else:
    popen = Popen(command, stdout=PIPE, stderr=stderrval,
                  shell=shell, close_fds=True)
    output = popen.communicate()
  if popen.returncode != 0:
    if throw:
      if output[0]:
        outputstr = "Command's output:\n%s" % output[0]
        if outputstr[-1] != '\n':
          outputstr += '\n'
      errstr = output[1]
      if errstr and errstr[-1] != '\n':
        errstr += '\n'
      errmess = ("Error running command: %s\n\n%s\n%s" %
          (command, output[0], output[1]))
      #log.error(errmess)
      oserror(errmess, EINVAL)
    else:
      return (output[0], popen.returncode)
  return output[0]

def oserror(mess, err):
  e = OSError(mess)
  e.errno = err
  raise e

#############################################################################
#                              Generating XML                               #
#############################################################################

# This is old code I wrote originally for ccg.ply (the ccg2xml converter),
# for generating XML.  It doesn't use the functions in xml.dom.minidom,
# which in any case are significantly more cumbersome than the list/tuple-based
# structure used below.

# --------- XML ----------
#
# Thankfully, the structure of XML is extremely simple.  We represent
# a single XML statement of the form
#
# <biteme foo="1" blorp="baz">
#   <bitemetoo ...>
#     ...
#   gurgle
# </biteme>
#
# as a list
#
# ['biteme', [('foo', '1'), ('blorp', 'baz')],
#    ['bitemetoo', ...],
#    'gurgle'
# ]
#
# i.e. an XML statement corresponds to a list where the first element
# is the statement name, the second element lists any properties, and
# the remaining elements list items inside the statement.
#
# ----------- Property lists -------------
#
# The second element of an XML statement in list form is a "property list",
# a list of two-element tuples (property and value).  Some functions below
# (e.g. `getprop', `putprop') manipulate property lists.
#
# FIXME: Just use a hash table.

def check_arg_type(errtype, arg, ty):
  if type(arg) is not ty:
    raise TypeError("%s: Type is not %s: %s" % (errtype, ty, arg))

def xml_sub(text):
  if not isinstance(text, basestring):
    text = text.__str__()
  if type(text) is unicode:
    text = text.encode("utf-8")
  text = text.replace('&', '&amp;')
  text = text.replace('<', '&lt;')
  text = text.replace('>', '&gt;')
  return text

def print_xml_1(file, xml, indent=0):
  #if xml_debug > 1:
  #  errout("%sPrinting: %s\n" % (' ' * indent, str(xml)))
  if type(xml) is not list:
    file.write('%s%s\n' % (' ' * indent, xml_sub(xml)))
  else:
    check_arg_type("XML statement", xml[0], str)
    file.write(' ' * indent)
    file.write('<%s' % xml_sub(xml[0]))
    for x in xml[1]:
      check_arg_type("XML statement", x, tuple)
      if len(x) != 2:
        raise TypeError("Bad tuple pair: " + str(x))
      file.write(' %s="%s"' % (xml_sub(x[0]), xml_sub(x[1])))
    subargs = xml[2:]
    if len(subargs) == 1 and type(subargs[0]) is not list:
      file.write('>%s</%s>\n' % (xml_sub(subargs[0]), xml_sub(xml[0])))
    elif not subargs:
      file.write('/>\n')
    else:
      file.write('>\n')
      for x in subargs:
        print_xml_1(file, x, indent + 2)
      file.write(' ' * indent)
      file.write('</%s>\n' % xml_sub(xml[0]))

# Pretty-print a section of XML, in the format above, to FILE.
# Start at indent INDENT.

def print_xml(file, xml):
  print_xml_1(file, xml)

# Function to output a particular XML file
def output_xml_file(filename, xml):
  fil = open(filename, 'w')
  fil.write('<?xml version="1.0" encoding="UTF-8"?>\n')
  print_xml(fil, xml)
  fil.close()

# Return True if PROP is seen as a property in PROPLIST, a list of tuples
# of (prop, value)
def property_specified(prop, proplist):
  return not not ['foo' for (x,y) in proplist if x == prop]

# Return value of property PROP in PROPLIST; signal an error if not found.
def getprop(prop, proplist):
  for (x,y) in proplist:
    if x == prop:
      return y
  raise ValueError("Property %s not found in %s" % (prop, proplist))

# Return value of property PROP in PROPLIST, or DEFAULT.
def getoptprop(prop, proplist, default=None):
  for (x,y) in proplist:
    if x == prop:
      return y
  return default

# Replace value of property PROP with VALUE in PROPLIST.
def putprop(prop, value, proplist):
  for i in xrange(len(proplist)):
    if proplist[i][0] == prop:
      proplist[i] = (prop, value)
      return
  else:
    proplist += [(prop, value)]
    
# Replace property named PROP with NEW in PROPLIST.  Often this is called with
# with PROP equal to None; the None occurs when a PROP=VALUE clause is expected
# but a bare value is supplied.  The context will supply a particular default
# property (e.g. 'name') to be used when the property name is omitted, but the
# generic code to handle property-value clauses doesn't know what this is.
# The surrounding code calls property_name_replace() to fill in the proper name.

def property_name_replace(prop, new, proplist):
  for i in xrange(len(proplist)):
    if proplist[i][0] == prop:
      proplist[i] = (new, proplist[i][1])


#############################################################################
#                 Extra functions for working with sequences                #
#                   Part of the Python docs for itertools                   #
#############################################################################

def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(islice(iterable, n))

def tabulate(function, start=0):
    "Return function(0), function(1), ..."
    return imap(function, count(start))

def consume(iterator, n):
    "Advance the iterator n-steps ahead. If n is none, consume entirely."
    # Use functions that consume iterators at C speed.
    if n is None:
        # feed the entire iterator into a zero-length deque
        collections.deque(iterator, maxlen=0)
    else:
        # advance to the empty slice starting at position n
        next(islice(iterator, n, n), None)

def nth(iterable, n, default=None):
    "Returns the nth item or a default value"
    return next(islice(iterable, n, None), default)

def quantify(iterable, pred=bool):
    "Count how many times the predicate is true"
    return sum(imap(pred, iterable))

def padnone(iterable):
    """Returns the sequence elements and then returns None indefinitely.

    Useful for emulating the behavior of the built-in map() function.
    """
    return chain(iterable, repeat(None))

def ncycles(iterable, n):
    "Returns the sequence elements n times"
    return chain.from_iterable(repeat(tuple(iterable), n))

def dotproduct(vec1, vec2):
    return sum(imap(operator.mul, vec1, vec2))

def flatten(listOfLists):
    "Flatten one level of nesting"
    return chain.from_iterable(listOfLists)

def repeatfunc(func, times=None, *args):
    """Repeat calls to func with specified arguments.

    Example:  repeatfunc(random.random)
    """
    if times is None:
        return starmap(func, repeat(args))
    return starmap(func, repeat(args, times))

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return izip(a, b)

def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)

def roundrobin(*iterables):
    "roundrobin('ABC', 'D', 'EF') --> A D E B F C"
    # Recipe credited to George Sakkis
    pending = len(iterables)
    nexts = cycle(iter(it).next for it in iterables)
    while pending:
        try:
            for next in nexts:
                yield next()
        except StopIteration:
            pending -= 1
            nexts = cycle(islice(nexts, pending))

def powerset(iterable):
    "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))

def unique_everseen(iterable, key=None):
    "List unique elements, preserving order. Remember all elements ever seen."
    # unique_everseen('AAAABBBCCDAABBB') --> A B C D
    # unique_everseen('ABBCcAD', str.lower) --> A B C D
    seen = set()
    seen_add = seen.add
    if key is None:
        for element in ifilterfalse(seen.__contains__, iterable):
            seen_add(element)
            yield element
    else:
        for element in iterable:
            k = key(element)
            if k not in seen:
                seen_add(k)
                yield element

def unique_justseen(iterable, key=None):
    "List unique elements, preserving order. Remember only the element just seen."
    # unique_justseen('AAAABBBCCDAABBB') --> A B C D A B
    # unique_justseen('ABBCcAD', str.lower) --> A B C A D
    return imap(next, imap(itemgetter(1), groupby(iterable, key)))

def iter_except(func, exception, first=None):
    """ Call a function repeatedly until an exception is raised.

    Converts a call-until-exception interface to an iterator interface.
    Like __builtin__.iter(func, sentinel) but uses an exception instead
    of a sentinel to end the loop.

    Examples:
        bsddbiter = iter_except(db.next, bsddb.error, db.first)
        heapiter = iter_except(functools.partial(heappop, h), IndexError)
        dictiter = iter_except(d.popitem, KeyError)
        dequeiter = iter_except(d.popleft, IndexError)
        queueiter = iter_except(q.get_nowait, Queue.Empty)
        setiter = iter_except(s.pop, KeyError)

    """
    try:
        if first is not None:
            yield first()
        while 1:
            yield func()
    except exception:
        pass

def random_product(*args, **kwds):
    "Random selection from itertools.product(*args, **kwds)"
    pools = map(tuple, args) * kwds.get('repeat', 1)
    return tuple(random.choice(pool) for pool in pools)

def random_permutation(iterable, r=None):
    "Random selection from itertools.permutations(iterable, r)"
    pool = tuple(iterable)
    r = len(pool) if r is None else r
    return tuple(random.sample(pool, r))

def random_combination(iterable, r):
    "Random selection from itertools.combinations(iterable, r)"
    pool = tuple(iterable)
    n = len(pool)
    indices = sorted(random.sample(xrange(n), r))
    return tuple(pool[i] for i in indices)

def random_combination_with_replacement(iterable, r):
    "Random selection from itertools.combinations_with_replacement(iterable, r)"
    pool = tuple(iterable)
    n = len(pool)
    indices = sorted(random.randrange(n) for i in xrange(r))
    return tuple(pool[i] for i in indices)

