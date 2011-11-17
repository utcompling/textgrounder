#!/usr/bin/python

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

def next_split_file(split_fractions, split_prefix, split_types):
  for x in next_split_set(split_fractions):
    yield "%s.%s" % (split_prefix, split_types[x])
  
gen = next_split_file([1, 1.5, 1], "foo-bar", ['training', 'dev', 'test'])
for i in xrange(1000):
  print gen.next()
