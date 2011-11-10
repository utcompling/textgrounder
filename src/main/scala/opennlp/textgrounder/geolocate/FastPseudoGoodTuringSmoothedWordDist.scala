///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2011 Ben Wing, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////

package opennlp.textgrounder.geolocate

import scala.collection.mutable
import math.{log, sqrt}

import tgutil.DynamicArray
import WordDist.memoizer.Word

/**
  Fast implementation of KL-divergence and cosine-similarity algorithms
  for use with pseudo-Good-Turing smoothing.

  This code was originally broken out of WordDist (before the
  pseudo-Good-Turing code was separated out) so that it could be
  rewritten in .pyc (Python with extra 'cdef' annotations that can be
  converted to C and compiled down to machine language).  With the
  pseudo-Good-Turing code extracted, this should properly be merged
  into PseudoGoodTuringSmoothedWordDist.scala, but keep separated for
  the moment in case we need to convert it to Java, C++, etc.
 */

object FastPseudoGoodTuringSmoothedWordDist {
  /*
   In normal operation of fast_kl_divergence(), we are passed the same
   'self' distribution repeatedly with different 'other' distributions,
   as we compare a given document/article against all the different
   non-empty cells of the Earth.  In each call, we have to iterate
   over all elements in the hash table, so we cache the elements and
   only retrieve again next time we're passed a different 'self'
   distribution.
   
   NOTE: This assumes no change to 'self' in the midst of these calls!
   RESULTS WILL BE WRONG OTHERWISE!  You can use test_kl_divergence()
   to test correct operation, which compares the result of this function
   to a different, slower but safer KL-divergence implementation.
   */
  protected val initial_static_array_size = 1000
  protected val static_key_array =
    new DynamicArray[Word](initial_alloc = initial_static_array_size)
  /* These arrays are specialized, so when we retrieve the underlying
     array we get a raw array. */
  protected val static_value_array =
    new DynamicArray[Int](initial_alloc = initial_static_array_size)
  protected def size_static_arrays(size: Int) {
    static_key_array.ensure_at_least(size)
    static_value_array.ensure_at_least(size)
  }

  type DistType = PseudoGoodTuringSmoothedWordDist
  val DistType = PseudoGoodTuringSmoothedWordDist
  protected var cached_worddist: DistType = null
  protected var cached_size: Int = 0

  def setup_static_arrays(self: DistType) {
    if (self eq cached_worddist) {
      assert(self.counts.size == cached_size)
      return
    }
    // Retrieve keys and values of P (self) into static arrays.
    val pcounts = self.counts
    cached_worddist = self
    cached_size = pcounts.size
    size_static_arrays(cached_size)
    val keys = static_key_array.array
    val values = static_value_array.array
    pcounts.keys.copyToArray(keys)
    pcounts.values.copyToArray(values)
  }

  /**
   A fast implementation of KL-divergence that uses inline lookups as much
   as possible.
   */
  def fast_kl_divergence(self: DistType, other: DistType,
    partial: Boolean=false): Double = {
    val pfact = (1.0 - self.unseen_mass)/self.num_word_tokens
    val qfact = (1.0 - other.unseen_mass)/other.num_word_tokens
    val qfact_unseen = other.unseen_mass / other.overall_unseen_mass
    val qfact_globally_unseen_prob = (other.unseen_mass*
        self.factory.globally_unseen_word_prob /
        self.factory.total_num_unseen_word_types)
    val owprobs = self.factory.overall_word_probs
    val pcounts = self.counts
    val qcounts = other.counts

    // 1.

    /* See comments above -- normal operation of fast_kl_divergence()
       means that we can usually reuse the same arrays we retrieved
       previously.  Since DynamicArray[T] is specialized on T, the
       arrays in 'pkeys' and 'pvalues' will be direct Java arrays of
       ints, and the array accesses below compile down to direct
       array-access bytecodes. */

    setup_static_arrays(self)
    val pkeys = static_key_array.array
    val pvalues = static_value_array.array
    val psize = self.counts.size

    // FIXME!! p * log(p) is the same for all calls of fast_kl_divergence
    // on this item, so we could cache it.  Not clear it would save much
    // time, though.
    var kldiv = 0.0
    /* THIS IS THE INSIDE LOOP.  THIS IS THE CODE BOTTLENECK.  THIS IS IT.
       
       This code needs to scream.  Hence we do extra setup above involving
       static arrays, to avoid having a function call through a function
       pointer (through the "obvious" use of forEach()). FIXME: But see
       comment above.
      
       Note that HotSpot is good about inlining function calls.
       Hence we can assume that the calls to apply() below (e.g.
       qcounts(word)) will be inlined.  However, it's *very important*
       to avoid doing anything that creates objects each iteration,
       and best to avoid creating objects per call to fast_kl_divergence().
       This object creation will kill us, as it will trigger tons
       and tons of garbage collection.

       Recent HotSpot implementations (6.0 rev 14 and above) have "escape
       analysis" that *might* make the object creation magically vanish,
       but don't count on it.
     */
    var i = 0
    while (i < psize) {
      val word = pkeys(i)
      val pcount = pvalues(i)
      val p = pcount * pfact
      val q = {
        val qcount = qcounts(word)
        if (qcount != 0) qcount * qfact
        else {
          val owprob = owprobs(word)
          if (owprob != 0.0) owprob * qfact_unseen
          else qfact_globally_unseen_prob
        }
      }
      //if (q == 0.0)
      //  errprint("Strange: word=%s qfact_globally_unseen_prob=%s qcount=%s qfact=%s",
      //           word, qfact_globally_unseen_prob, qcount, qfact)
      //if (p == 0.0 || q == 0.0)
      //  errprint("Warning: zero value: p=%s q=%s word=%s pcount=%s qcount=%s qfact=%s qfact_unseen=%s owprobs=%s",
      //      p, q, word, pcount, qcount, qfact, qfact_unseen,
      //      owprobs(word))
      kldiv += p * (log(p) - log(q))
      i += 1
    }
  
    if (partial)
      return kldiv

    // 2.
    val pfact_unseen = self.unseen_mass / self.overall_unseen_mass
    var overall_probs_diff_words = 0.0
    for ((word, qcount) <- qcounts if !(pcounts contains word)) {
      val word_overall_prob = owprobs(word)
      val p = word_overall_prob * pfact_unseen
      val q = qcount * qfact
      kldiv += p * (log(p) - log(q))
      overall_probs_diff_words += word_overall_prob
    }    

    return kldiv + self.inner_kl_divergence_34(other, overall_probs_diff_words)
  }
  
  // The older implementation that uses smoothed probabilities.
  
  /**
   A fast implementation of cosine similarity that uses Cython declarations
  and inlines lookups as much as possible.  It's always "partial" in that it
  ignores words neither in P nor Q, despite the fact that they have non-zero
  probability due to smoothing.  But with parameter "partial" to true we
  proceed as with KL-divergence and ignore words not in P.
   */
  def fast_smoothed_cosine_similarity(self: DistType, other: DistType,
    partial: Boolean=false): Double = {
    val pfact = (1.0 - self.unseen_mass)/self.num_word_tokens
    val qfact = (1.0 - other.unseen_mass)/other.num_word_tokens
    val qfact_unseen = other.unseen_mass / other.overall_unseen_mass
    val qfact_globally_unseen_prob = (other.unseen_mass*
        self.factory.globally_unseen_word_prob /
        self.factory.total_num_unseen_word_types)
    val owprobs = self.factory.overall_word_probs
    // 1.
    val pcounts = self.counts
    val qcounts = other.counts

    // FIXME!! Length of p is the same for all calls of fast_cosine_similarity
    // on this item, so we could cache it.  Not clear it would save much
    // time, though.
    var pqsum = 0.0
    var p2sum = 0.0
    var q2sum = 0.0
    for ((word, pcount) <- pcounts) {
      val p = pcount * pfact
      val q = {
        val qcount = qcounts(word)
        if (qcount != 0) qcount * qfact
        else {
          val owprob = owprobs(word)
          if (owprob != 0.0) owprob * qfact_unseen
          else qfact_globally_unseen_prob
        }
      }
      //if (q == 0.0)
      //  errprint("Strange: word=%s qfact_globally_unseen_prob=%s qcount=%s qfact=%s",
      //           word, qfact_globally_unseen_prob, qcount, qfact)
      //if (p == 0.0 || q == 0.0)
      //  errprint("Warning: zero value: p=%s q=%s word=%s pcount=%s qcount=%s qfact=%s qfact_unseen=%s owprobs=%s",
      //      p, q, word, pcount, qcount, qfact, qfact_unseen,
      //      owprobs(word))
      pqsum += p * q
      p2sum += p * p
      q2sum += q * q
    }
  
    if (partial)
      return pqsum / (sqrt(p2sum) * sqrt(q2sum))
  
    // 2.
    val pfact_unseen = self.unseen_mass / self.overall_unseen_mass
    var overall_probs_diff_words = 0.0
    for ((word, qcount) <- qcounts if !(pcounts contains word)) {
      val word_overall_prob = owprobs(word)
      val p = word_overall_prob * pfact_unseen
      val q = qcount * qfact
      pqsum += p * q
      p2sum += p * p
      q2sum += q * q
      //overall_probs_diff_words += word_overall_prob
    }
  
    // FIXME: This would be the remainder of the computation for words
    // neither in P nor Q.  We did a certain amount of math in the case of the
    // KL-divergence to make it possible to do these steps efficiently.
    // Probably similar math could make the steps here efficient as well, but
    // unclear.
  
    //kldiv += self.kl_divergence_34(other, overall_probs_diff_words)
    //return kldiv
  
    return pqsum / (sqrt(p2sum) * sqrt(q2sum))
  }
  
  // The newer implementation that uses unsmoothed probabilities.
  
  /**
   A fast implementation of cosine similarity that uses Cython declarations
  and inlines lookups as much as possible.  It's always "partial" in that it
  ignores words neither in P nor Q, despite the fact that they have non-zero
  probability due to smoothing.  But with parameter "partial" to true we
  proceed as with KL-divergence and ignore words not in P.
   */
  def fast_cosine_similarity(self: DistType, other: DistType,
    partial: Boolean=false) = {
    val pfact = 1.0/self.num_word_tokens
    val qfact = 1.0/other.num_word_tokens
    // 1.
    val pcounts = self.counts
    val qcounts = other.counts

    // FIXME!! Length of p is the same for all calls of fast_cosine_similarity
    // on this item, so we could cache it.  Not clear it would save much
    // time, though.
    var pqsum = 0.0
    var p2sum = 0.0
    var q2sum = 0.0
    for ((word, pcount) <- pcounts) {
      val p = pcount * pfact
      val q = qcounts(word) * qfact
      //if (q == 0.0)
      //  errprint("Strange: word=%s qfact_globally_unseen_prob=%s qcount=%s qfact=%s",
      //           word, qfact_globally_unseen_prob, qcount, qfact)
      //if (p == 0.0 || q == 0.0)
      //  errprint("Warning: zero value: p=%s q=%s word=%s pcount=%s qcount=%s qfact=%s qfact_unseen=%s owprobs=%s",
      //      p, q, word, pcount, qcount, qfact, qfact_unseen,
      //      owprobs(word))
      pqsum += p * q
      p2sum += p * p
      q2sum += q * q
    }
  
    // 2.
    if (!partial)
    for ((word, qcount) <- qcounts if !(pcounts contains word)) {
      val q = qcount * qfact
      q2sum += q * q
    }
  
    if (pqsum == 0.0) 0.0 else pqsum / (sqrt(p2sum) * sqrt(q2sum))
  }
}
