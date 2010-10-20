from __future__ import with_statement # For chompopen(), uchompopen()
import re # For regexp wrappers
import sys, codecs # For uchompopen()
import bisect # For sorted lists

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

# Open a filename with UTF-8-encoded input and yield lines converted to
# Unicode strings, but with any terminating newline removed (similar to
# "chomp" in Perl).
def uchompopen(filename, errors='strict'):
  with codecs.open(filename, encoding='utf-8', errors=errors) as f:
    for line in f:
      if line and line[-1] == '\n': line = line[:-1]
      yield line

# Open a filename and yield lines, but with any terminating newline
# removed (similar to "chomp" in Perl).
def chompopen(filename):
  with open(filename) as f:
    for line in f:
      if line and line[-1] == '\n': line = line[:-1]
      yield line

#############################################################################
#                         Other Unicode utility functions                   #
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

#############################################################################
#                             Default dictionaries                          #
#############################################################################

# A dictionary where missing keys automatically spring into existence
# with a value of 0.  Useful for dictionaries that track counts of items.
class intdict(dict):
  def __missing__(self, key):
    return 0

# A dictionary where missing keys automatically spring into existence
# with a value of [].  Useful for dictionaries that track lists of items.
# NOTE NOTE NOTE: It DOES NOT work to add to a non-existent key using
# append(); use += instead.  It "appears" to work but the new value gets
# swallowed.  The reason is that although asking for the value of a
# non-existent key automatically returns [], the key itself doesn't
# spring into existence until you assign assign a value to it, as for
# normal dictionaries.  NOTE: This behavior is NOT the same as when you
# use collections.defaultdict(list), where asking for the value of a
# non-existent key DOES cause the key to get added to the dictionary with
# the default value.
#
# Hence:
#
# foo = listdict()
# foo['bar'] -> []
# 'bar' in foo -> False
#
# import collections
# foo2 = collections.defaultdict(list)
# foo2['bar'] -> []
# 'bar' in foo2 -> True
#
class listdict(dict):
  def __missing__(self, key):
    return list()

# A dictionary where missing keys automatically spring into existence
# with a value of (), i.e. the empty tuple.
class tupledict(dict):
  def __missing__(self, key):
    return ()

# A dictionary where missing keys automatically spring into existence
# with a value of set(), i.e. the empty set.  Useful for dictionaries that
# track sets of items.  See above: you need to add items to the set
# using +=, not using append().
class setdict(dict):
  def __missing__(self, key):
    return set()

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

def lookup_sorted_list(sorted_list, key):
  (keys, values) = sorted_list
  i = bisect.bisect_left(keys, key)
  if i != len(keys) and keys[i] == key:
    return values[i]
  return None

#############################################################################
#                                     Misc                                  #
#############################################################################

# Given a table with values that are numbers, output the table, sorted
# on the numbers from bigger to smaller.
def output_reverse_sorted_table(table):
  for x in sorted(table.items(), key=lambda x:x[1], reverse=True):
    uniprint("%s = %s" % (x[0], x[1]))

