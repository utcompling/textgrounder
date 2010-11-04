from word_distribution import WordDist


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


# For pure Python code, use this instead:
#
# from math import log

cdef extern from "math.h":
  double log(double)

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
  owprobs = WordDist.overall_word_probs
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
      q = owprobs[word] * qfact_unseen
    else:
      q = qcount * qfact
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
