import math
from nlputil import *
from math import log
from itertools import *
from kl_divergence import *

debug = booldict()

use_sorted_list = False

############################################################################
#                             Word distributions                           #
############################################################################

# Fields defined:
#
#   counts: A "sorted list" (tuple of sorted keys and values) of
#           (word, count) items, specifying the counts of all words seen
#           at least once.
#   finished: Whether we have finished computing the distribution in
#             'counts'.
#   unseen_mass: Total probability mass to be assigned to all words not
#                seen in the article, estimated (motivated by Good-Turing
#                smoothing) as the unadjusted empirical probability of
#                having seen a word once.
#   overall_unseen_mass:
#     Probability mass assigned in 'overall_word_probs' to all words not seen
#     in the article.  This is 1 - (sum over W in A of overall_word_probs[W]).
#     The idea is that we compute the probability of seeing a word W in
#     article A as
#
#     -- if W has been seen before in A, use the following:
#          COUNTS[W]/TOTAL_TOKENS*(1 - UNSEEN_MASS)
#     -- else, if W seen in any articles (W in 'overall_word_probs'),
#        use UNSEEN_MASS * (overall_word_probs[W] / OVERALL_UNSEEN_MASS).
#        The idea is that overall_word_probs[W] / OVERALL_UNSEEN_MASS is
#        an estimate of p(W | W not in A).  We have to divide by
#        OVERALL_UNSEEN_MASS to make these probabilities be normalized
#        properly.  We scale p(W | W not in A) by the total probability mass
#        we have available for all words not seen in A.
#     -- else, use UNSEEN_MASS * globally_unseen_word_prob / NUM_UNSEEN_WORDS,
#        where NUM_UNSEEN_WORDS is an estimate of the total number of words
#        "exist" but haven't been seen in any articles.  One simple idea is
#        to use the number of words seen once in any article.  This certainly
#        underestimates this number if not too many articles have been seen
#        but might be OK if many articles seen.
#   total_tokens: Total number of word tokens seen

class WordDist(object):
  # Can't use __slots__, or you get this following for NBArticle:
  #TypeError: Error when calling the metaclass bases
  #    multiple bases have instance lay-out conflict
  __slots__ = ['finished', 'counts', 'unseen_mass', 'total_tokens',
               'overall_unseen_mass']

  # Total number of word types seen (size of vocabulary)
  num_word_types = 0

  # Total number of word tokens seen
  num_word_tokens = 0

  # Total number of types seen once
  num_types_seen_once = 0

  # Estimate of number of unseen word types for all articles
  num_unseen_word_types = 0

  # Overall probabilities over all articles of seeing a word in an article,
  # for all words seen at least once in any article, computed using the
  # empirical frequency of a word among all articles, adjusted by the mass
  # to be assigned to globally unseen words (words never seen at all), i.e. the
  # value in 'globally_unseen_word_prob'.
  overall_word_probs = intdict()

  # The total probability mass to be assigned to words not seen at all in
  # any article, estimated using Good-Turing smoothing as the unadjusted
  # empirical probability of having seen a word once.
  globally_unseen_word_prob = 0.0

  # For articles whose word counts are not known, use an empty list to
  # look up in.
  # unknown_article_counts = ([], [])

  # Yuck.
  @classmethod
  def set_debug(cls, val):
    global debug
    debug = val

  def __init__(self):
    self.finished = False
    self.counts = intdict()
    self.unseen_mass = 0.5
    self.total_tokens = 0
    self.overall_unseen_mass = 1.0

  def __str__(self):
    finished = ""
    if not self.finished: finished = ", unfinished"
    num_words_to_print = 15
    items = list('%s=%s' % (word, count) for (word, count) in
        islice(self.counts.iteritems(), num_words_to_print + 1))
    if len(items) > num_words_to_print:
      items[-1] = '...'
    words = ' '.join(items)
    return "WordDist(%d tokens, %.2f unseen mass%s, %s)" % (
        self.total_tokens, self.unseen_mass, finished, words)

  def set_word_distribution(self, total_tokens, wordhash, note_globally=True):
    '''Set the word distribution from the given table of words and counts.
'total_tokens' is the total number of word tokens.  If 'note_globally',
add the word counts to the global word count statistics.'''
    if self.counts:
      warning("Article %s already has counts for it!" % art)
    self.total_tokens = total_tokens
    if note_globally:
      for (ind, count) in wordhash.iteritems():
        if ind not in WordDist.overall_word_probs:
          WordDist.num_word_types += 1
        # Record in overall_word_probs; note more tokens seen.
        WordDist.overall_word_probs[ind] += count
        WordDist.num_word_tokens += count
    self.counts = wordhash

  def add_words(self, words, ignore_case=True, stopwords={}):
    '''Incorporate a list of words into the distribution. 'words' is an iterable.'''
    assert not self.finished
    counts = self.counts
    toks = 0
    for word in words:
      if ignore_case: word = word.lower()
      if word in stopwords:
        continue
      word = internasc(word)
      counts[word] += 1
      toks += 1
    self.total_tokens += toks

  def add_word_distribution(self, worddist):
    '''Incorporate counts from the given distribution into our distribution.'''
    assert not self.finished
    counts = self.counts
    for (word, count) in worddist.counts.iteritems():
      counts[word] += count
    self.total_tokens += worddist.total_tokens

  def finish(self, minimum_word_count=0):
    '''Finish computation of the word distribution.'''

    # If 'minimum_word_count' was given, then eliminate words whose count
    # is too small.
    if minimum_word_count > 1:
      for (word, count) in self.counts.iteritems():
        if count < minimum_word_count:
          self.total_tokens -= count
          del self.counts[word]

    # make sure counts not None (eg article in coords file but not counts file)
    if not self.counts: return
    if self.finished: return
    # Compute probabilities.  Use a very simple version of Good-Turing
    # smoothing where we assign to unseen words the probability mass of
    # words seen once, and adjust all other probs accordingly.
    num_types_seen_once = \
      sum(1 for word in self.counts if self.counts[word] == 1)
    if self.total_tokens > 0:
      # If no words seen only once, we will have a problem if we assign 0
      # to the unseen mass, as unseen words will end up with 0 probability.
      # However, if we assign a value of 1.0 to unseen_mass (which could
      # happen in case all words seen exactly once), then we will end
      # up assigning 0 probability to seen words.  So we arbitrarily
      # limit it to 0.5, which is pretty damn much mass going to unseen
      # words.
      self.unseen_mass = \
        min(0.5, float(max(1, num_types_seen_once))/self.total_tokens)
    else:
      self.unseen_mass = 0.5
    overall_seen_mass = 0.0
    for ind in self.counts:
      overall_seen_mass += WordDist.overall_word_probs[ind]
    self.overall_unseen_mass = 1.0 - overall_seen_mass
    if use_sorted_list:
      self.counts = SortedList(self.counts)
    self.finished = True

  @classmethod
  def finish_global_distribution(cls):
    # Now, adjust overall_word_probs accordingly.
    cls.num_types_seen_once = 0
    ### FIXME: A simple calculation reveals that in the scheme where we use
    ### globally_unseen_word_prob, num_types_seen_once cancels out and
    ### we never actually have to compute it.
    for count in cls.overall_word_probs.itervalues():
      if count == 1:
        cls.num_types_seen_once += 1
    cls.globally_unseen_word_prob = (
      float(cls.num_types_seen_once)/cls.num_word_tokens)
    for (wordind,count) in cls.overall_word_probs.iteritems():
      cls.overall_word_probs[wordind] = (
        float(count)/cls.num_word_tokens*
          (1 - cls.globally_unseen_word_prob))
    # A very rough estimate, perhaps totally wrong
    cls.num_unseen_word_types = \
        max(cls.num_types_seen_once, cls.num_word_types/20)
    #if debug['tons']:
    #  errprint("Num types = %s, num tokens = %s, num_seen_once = %s, globally unseen word prob = %s, total mass = %s" % (cls.num_word_types, cls.num_word_tokens, cls.num_types_seen_once, cls.globally_unseen_word_prob, cls.globally_unseen_word_prob + sum(cls.overall_word_probs.itervalues())))

  def test_kl_divergence(self, other, partial=False):
    '''Check fast and slow versions against each other.'''
    assert self.finished
    assert other.finished
    fast_kldiv = fast_kl_divergence(self, other, partial)
    slow_kldiv = self.slow_kl_divergence(other, partial)
    if abs(fast_kldiv - slow_kldiv) > 1e-8:
      errprint("Fast KL-div=%s but slow KL-div=%s" % (fast_kldiv, slow_kldiv))
      assert fast_kldiv == slow_kldiv
    return fast_kldiv


  # Compute the KL divergence between this distribution and another
  # distribution.  This is a bit tricky.  We have to take into account:
  # 1. Words in this distribution (may or may not be in the other).
  # 2. Words in the other distribution that are not in this one.
  # 3. Words in neither distribution but seen globally.
  # 4. Words never seen at all.
  # If 'return_contributing_words', return a tuple of (val, word_contribs)
  #   where word_contribs is a table of words and the amount each word
  #   contributes to the KL divergence.
  def slow_kl_divergence(self, other, partial=False,
      return_contributing_words=False):
    '''The basic implementation of KL-divergence.  Useful for checking against
other implementations.'''
    assert self.finished
    assert other.finished
    kldiv = 0.0
    if return_contributing_words:
      contribs = {}
    # 1.
    for word in self.counts:
      p = self.lookup_word(word)
      q = other.lookup_word(word)
      if p <= 0.0 or q <= 0.0:
        errprint("Warning: problematic values: p=%s, q=%s, word=%s" % (p, q, word))
      else:
        kldiv += p*(log(p) - log(q))
        if return_contributing_words:
          contribs[word] = p*(log(p) - log(q))

    if partial:
      if return_contributing_words:
        return (kldiv, contribs)
      return kldiv

    # 2.
    overall_probs_diff_words = 0.0
    for word in other.counts:
      if word not in self.counts:
        p = self.lookup_word(word)
        q = other.lookup_word(word)
        kldiv += p*(log(p) - log(q))
        if return_contributing_words:
          contribs[word] = p*(log(p) - log(q))
        overall_probs_diff_words += WordDist.overall_word_probs[word]

    retval = kldiv + self.kl_divergence_34(other, overall_probs_diff_words)
    if return_contributing_words:
      return (retval, contribs)
    return retval


  def kl_divergence_34(self, other, overall_probs_diff_words):
    '''Steps 3 and 4 of KL-divergence computation.'''
    kldiv = 0.0

    # 3. For words seen in neither dist but seen globally:
    # You can show that this is
    #
    # factor1 = (log(self.unseen_mass) - log(self.overall_unseen_mass)) -
    #           (log(other.unseen_mass) - log(other.overall_unseen_mass))
    # factor2 = self.unseen_mass / self.overall_unseen_mass * factor1
    # kldiv = factor2 * (sum(words seen globally but not in either dist)
    #                    of overall_word_probs[word]) 
    #
    # The final sum
    #   = 1 - sum(words in self) overall_word_probs[word]
    #       - sum(words in other, not self) overall_word_probs[word]
    #   = self.overall_unseen_mass
    #       - sum(words in other, not self) overall_word_probs[word]
    #
    # So we just need the sum over the words in other, not self.

    factor1 = ((log(self.unseen_mass) - log(self.overall_unseen_mass)) -
               (log(other.unseen_mass) - log(other.overall_unseen_mass)))
    factor2 = self.unseen_mass / self.overall_unseen_mass * factor1
    the_sum = self.overall_unseen_mass - overall_probs_diff_words
    kldiv += factor2 * the_sum

    # 4. For words never seen at all:
    p = (self.unseen_mass*WordDist.globally_unseen_word_prob /
          WordDist.num_unseen_word_types)
    q = (other.unseen_mass*WordDist.globally_unseen_word_prob /
          WordDist.num_unseen_word_types)
    kldiv += WordDist.num_unseen_word_types*(p*(log(p) - log(q)))

    return kldiv

  def symmetric_kldiv(self, other):
    return (0.5*self.kl_divergence(other) + 
            0.5*other.kl_divergence(self))

  def lookup_word(self, word):
    assert self.finished
    #if debug['some']:
    #  errprint("Found counts for article %s, num word types = %s"
    #           % (art, len(wordcounts[0])))
    #  errprint("Unknown prob = %s, overall_unseen_mass = %s" %
    #           (unseen_mass, overall_unseen_mass))
    wordprob = self.counts.get(word, None)
    if wordprob is None:
      owprob = WordDist.overall_word_probs.get(word, None)
      if owprob is None:
        wordprob = (self.unseen_mass*WordDist.globally_unseen_word_prob
                    / WordDist.num_unseen_word_types)
        if debug['lots']:
          errprint("Word %s, never seen at all, wordprob = %s" %
                   (word, wordprob))
      else:
        wordprob = (self.unseen_mass *
                    (WordDist.overall_word_probs[word] /
                     self.overall_unseen_mass))
        #if wordprob <= 0:
        #  warning("Bad values; unseen_mass = %s, overall_word_probs[word] = %s, overall_unseen_mass = %s" % (unseen_mass, WordDist.overall_word_probs[word], WordDist.overall_unseen_mass))
        if debug['lots']:
          errprint("Word %s, seen but not in article, wordprob = %s" %
                   (word, wordprob))
    else:
      #if wordprob <= 0 or total_tokens <= 0 or unseen_mass >= 1.0:
      #  warning("Bad values; wordprob = %s, unseen_mass = %s" %
      #          (wordprob, unseen_mass))
      #  for (word, count) in self.counts.iteritems():
      #    errprint("%s: %s" % (word, count))
      wordprob = float(wordprob)/self.total_tokens*(1 - self.unseen_mass)
      if debug['lots']:
        errprint("Word %s, seen in article, wordprob = %s" %
                 (word, wordprob))
    return wordprob


