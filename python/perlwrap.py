import re

#### Some simple wrappers around basic Python functions to make them
#### easier to use (and typically more like Perl).
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
####
#### 2. chompopen():
####
#### A generator that yields lines from a file, with any terminating newline
#### removed (but no other whitespace removed).  Ensures that the file
#### will be automatically closed under all circumstances.

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

# Open a filename and yield lines, but with any terminating newline
# removed (similar to "chomp" in Perl).
def chompopen(filename):
  with open(filename) as f:
    for line in f:
      if line and line[-1] == '\n': line = line[:-1]
      yield line
