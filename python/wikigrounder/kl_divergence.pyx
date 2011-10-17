# This is a Cython file -- basically a pure Python file but with annotations
# to make it possible to compile it into a C module that runs a lot faster
# than normal Python.
#
# You need Cython installed in order for this to work, of course.
# See www.cython.org.
#
# For Mac OS X, you can install Cython using something like
#
# sudo port install py26-cython
#
#
# To convert this file to pure Python:
#
# (1) Remove all 'cdef' statements, as well as the 'cdef extern' block
#     at the beginning of the file.
#
# (2) Add the following statement in place of the 'cdef extern' block:
#
# from math import log, sqrt

cdef extern from "math.h":
  double log(double)
  double sqrt(double)

def fast_kl_divergence(self, other, partial=False):
  '''A fast implementation of KL-divergence that uses Cython declarations and
inlines lookups as much as possible.'''
  cdef double kldiv, p, q
  cdef double pfact, qfact, pfact_unseen, qfact_unseen
  kldiv = 0.0
  pfact = (1 - self.unseen_mass)/self.total_tokens
  qfact = (1 - other.unseen_mass)/other.total_tokens
  pfact_unseen = self.unseen_mass / self.overall_unseen_mass
  qfact_unseen = other.unseen_mass / other.overall_unseen_mass
  qfact_globally_unseen_prob = (other.unseen_mass*
      other.globally_unseen_word_prob / other.num_unseen_word_types)
  owprobs = self.overall_word_probs
  # 1.
  pcounts = self.counts
  qcounts = other.counts
  cdef int pcount
  # FIXME!! p * log(p) is the same for all calls of fast_kl_divergence
  # on this item, so we could cache it.  Not clear it would save much
  # time, though.
  for (word, pcount) in pcounts.iteritems():
    p = pcount * pfact
    qcount = qcounts.get(word, None)
    if qcount is None:
      q = (owprobs[word] * qfact_unseen) or qfact_globally_unseen_prob
    else:
      q = qcount * qfact
    #if q == 0.0:
    #  print "Strange: word=%s qfact_globally_unseen_prob=%s qcount=%s qfact=%s" % (word, qfact_globally_unseen_prob, qcount, qfact)
    #if p == 0.0 or q == 0.0:
    #  print "Warning: zero value: p=%s q=%s word=%s pcount=%s qcount=%s qfact=%s qfact_unseen=%s owprobs=%s" % (
    #      p, q, word, pcount, qcount, qfact, qfact_unseen, owprobs[word])
    kldiv += p * (log(p) - log(q))

  if partial:
    return kldiv

  # 2.
  cdef double overall_probs_diff_words
  cdef double word_overall_prob
  overall_probs_diff_words = 0.0
  for word in qcounts:
    if word not in pcounts:
      word_overall_prob = owprobs[word]
      p = word_overall_prob * pfact_unseen
      q = qcounts[word] * qfact
      kldiv += p * (log(p) - log(q))
      overall_probs_diff_words += word_overall_prob

  kldiv += self.kl_divergence_34(other, overall_probs_diff_words)

  return kldiv

# The older implementation that uses smoothed probabilities.

def fast_smoothed_cosine_similarity(self, other, partial=False):
  '''A fast implementation of cosine similarity that uses Cython declarations
and inlines lookups as much as possible.  It's always "partial" in that it
ignores words neither in P nor Q, despite the fact that they have non-zero
probability due to smoothing.  But with parameter "partial" to True we
proceed as with KL-divergence and ignore words not in P.'''
  cdef double p, q
  cdef double pfact, qfact, pfact_unseen, qfact_unseen
  cdef double pqsum, p2sum, q2sum
  pqsum = 0.0
  p2sum = 0.0
  q2sum = 0.0
  pfact = (1 - self.unseen_mass)/self.total_tokens
  qfact = (1 - other.unseen_mass)/other.total_tokens
  pfact_unseen = self.unseen_mass / self.overall_unseen_mass
  qfact_unseen = other.unseen_mass / other.overall_unseen_mass
  qfact_globally_unseen_prob = (other.unseen_mass*
      other.globally_unseen_word_prob / other.num_unseen_word_types)
  owprobs = self.overall_word_probs
  # 1.
  pcounts = self.counts
  qcounts = other.counts
  cdef int pcount
  # FIXME!! Length of p is the same for all calls of fast_cosine_similarity
  # on this item, so we could cache it.  Not clear it would save much
  # time, though.
  for (word, pcount) in pcounts.iteritems():
    p = pcount * pfact
    qcount = qcounts.get(word, None)
    if qcount is None:
      q = (owprobs[word] * qfact_unseen) or qfact_globally_unseen_prob
    else:
      q = qcount * qfact
    #if q == 0.0:
    #  print "Strange: word=%s qfact_globally_unseen_prob=%s qcount=%s qfact=%s" % (word, qfact_globally_unseen_prob, qcount, qfact)
    #if p == 0.0 or q == 0.0:
    #  print "Warning: zero value: p=%s q=%s word=%s pcount=%s qcount=%s qfact=%s qfact_unseen=%s owprobs=%s" % (
    #      p, q, word, pcount, qcount, qfact, qfact_unseen, owprobs[word])
    pqsum += p * q
    p2sum += p * p
    q2sum += q * q

  if partial:
    return pqsum / (sqrt(p2sum) * sqrt(q2sum))

  # 2.
  cdef double overall_probs_diff_words
  cdef double word_overall_prob
  overall_probs_diff_words = 0.0
  for word in qcounts:
    if word not in pcounts:
      word_overall_prob = owprobs[word]
      p = word_overall_prob * pfact_unseen
      q = qcounts[word] * qfact
      pqsum += p * q
      p2sum += p * p
      q2sum += q * q
      #overall_probs_diff_words += word_overall_prob

  # FIXME: This would be the remainder of the computation for words
  # neither in P nor Q.  We did a certain amount of math in the case of the
  # KL-divergence to make it possible to do these steps efficiently.
  # Probably similar math could make the steps here efficient as well, but
  # unclear.

  #kldiv += self.kl_divergence_34(other, overall_probs_diff_words)
  #return kldiv

  return pqsum / (sqrt(p2sum) * sqrt(q2sum))

# The newer implementation that uses unsmoothed probabilities.

def fast_cosine_similarity(self, other, partial=False):
  '''A fast implementation of cosine similarity that uses Cython declarations
and inlines lookups as much as possible.  It's always "partial" in that it
ignores words neither in P nor Q, despite the fact that they have non-zero
probability due to smoothing.  But with parameter "partial" to True we
proceed as with KL-divergence and ignore words not in P.'''
  cdef double p, q
  cdef double pfact, qfact
  cdef double pqsum, p2sum, q2sum
  pqsum = 0.0
  p2sum = 0.0
  q2sum = 0.0
  pfact = 1.0/self.total_tokens
  qfact = 1.0/other.total_tokens
  # 1.
  pcounts = self.counts
  qcounts = other.counts
  cdef int pcount
  # FIXME!! Length of p is the same for all calls of fast_cosine_similarity
  # on this item, so we could cache it.  Not clear it would save much
  # time, though.
  for (word, pcount) in pcounts.iteritems():
    p = pcount * pfact
    qcount = qcounts.get(word, None)
    if qcount is None:
      q = 0.0
    else:
      q = qcount * qfact
    #if q == 0.0:
    #  print "Strange: word=%s qfact_globally_unseen_prob=%s qcount=%s qfact=%s" % (word, qfact_globally_unseen_prob, qcount, qfact)
    #if p == 0.0 or q == 0.0:
    #  print "Warning: zero value: p=%s q=%s word=%s pcount=%s qcount=%s qfact=%s qfact_unseen=%s owprobs=%s" % (
    #      p, q, word, pcount, qcount, qfact, qfact_unseen, owprobs[word])
    pqsum += p * q
    p2sum += p * p
    q2sum += q * q

  # 2.
  if not partial:
    for word in qcounts:
      if word not in pcounts:
        q = qcounts[word] * qfact
        q2sum += q * q

  if pqsum == 0.0: return 0.0
  return pqsum / (sqrt(p2sum) * sqrt(q2sum))
