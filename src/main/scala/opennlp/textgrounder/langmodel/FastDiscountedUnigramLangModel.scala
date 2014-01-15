///////////////////////////////////////////////////////////////////////////////
//  FastDiscountedUnigramLangModel.scala
//
//  Copyright (C) 2010-2013 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder
package langmodel

import scala.collection.mutable
import math.{log, sqrt}

import util.collection.DynamicArray

/**
  Fast implementation of KL-divergence and cosine-similarity algorithms
  for use with discounted smoothing.  This code was originally written
  for Pseudo-Good-Turing, but it isn't specific to this algorithm --
  it works for any algorithm that involves discounting of a lang model
  in order to interpolate the global lang model.

  This code was originally broken out of LangModel (before the
  pseudo-Good-Turing code was separated out) so that it could be
  rewritten in .pyc (Python with extra 'cdef' annotations that can be
  converted to C and compiled down to machine language).  With the
  pseudo-Good-Turing code extracted, this should properly be merged
  into PseudoGoodTuringSmoothedLangModel.scala, but keep separated for
  the moment in case we need to convert it to Java, C++, etc.
 */

class DiscountedUnigramKLDivergenceCache(
    val langmodel: DiscountedUnigramLangModel
  ) extends KLDivergenceCache {
  val self_size = langmodel.num_types
  val self_keys = langmodel.iter_keys.toArray
  val self_values = langmodel.iter_grams.map { case (k,v) => v}.toArray
}

object FastDiscountedUnigramLangModel {
  type TDist = DiscountedUnigramLangModel

  def get_kl_divergence_cache(self: TDist) =
    new DiscountedUnigramKLDivergenceCache(self)

  /*
   A fast implementation of KL-divergence that uses inline lookups as much
   as possible.  Uses cached values if possible to avoid garbage from
   copying arrays.

   In normal operation of grid location, we repeatedly do KL divergence
   with the same `self` lang model and different `other` lang models,
   and we have to iterate over all key/value pairs in `self`, so caching
   the `self` keys and values into arrays is useful.  `cache` can be
   null (no cache available) or a cache created using
   `get_kl_divergence_cache`, which must have been called on `self`,
   and NO CHANGES to `self` made between cache creation time and use time.
   */
  def fast_kl_divergence(self: TDist,
      cache: DiscountedUnigramKLDivergenceCache,
      other: TDist, interpolate: Boolean, partial: Boolean = true): Double = {

    val the_cache =
      if (cache == null)
        get_kl_divergence_cache(self)
      else
        cache
    assert(the_cache.langmodel == self)
    assert(the_cache.self_size == self.num_types)
    val pkeys = the_cache.self_keys
    val pvalues = the_cache.self_values
    val pfact = (1.0 - self.unseen_mass)/self.num_tokens
    val qfact = (1.0 - other.unseen_mass)/other.num_tokens
    val pfact_unseen = self.unseen_mass / self.overall_unseen_mass
    val qfact_unseen = other.unseen_mass / other.overall_unseen_mass
    val factory = self.factory
    /* Not needed in the new way
    val qfact_globally_unseen_prob = (other.unseen_mass*
        factory.globally_unseen_word_prob /
        factory.total_num_unseen_word_types)
    */
    val owprobs = factory.overall_word_probs
    val pmodel = self.model
    val qmodel = other.model

    // 1.

    val psize = self.num_types

    // FIXME!! p * log(p) is the same for all calls of fast_kl_divergence
    // on this gram, so we could cache it.  Not clear it would save much
    // time, though.
    var kldiv = 0.0
    /* THIS IS THE INSIDE LOOP.  THIS IS THE CODE BOTTLENECK.  THIS IS IT.
       
       This code needs to scream.  Hence we do extra setup above involving
       arrays, to avoid having a function call through a function
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
    if (interpolate) {
      while (i < psize) {
        val word = pkeys(i)
        val pcount = pvalues(i)
        val qcount = qmodel.get_gram(word)
        val owprob = owprobs(word)
        val p: Double = pcount * pfact + owprob * pfact_unseen
        val q: Double = qcount * qfact + owprob * qfact_unseen
        /* In the "new way" we have to notice when a word was never seen
           at all, and ignore it. */
        if (q > 0.0) {
          //if (p == 0.0)
          //  errprint("Warning: zero value: p=%s q=%s word=%s pcount=%s qcount=%s qfact=%s qfact_unseen=%s owprobs=%s",
          //      p, q, word, pcount, qcount, qfact, qfact_unseen,
          //      owprobs(word))
          // Use log(p/q) not log(p)-log(q) -- division faster than extra log
          kldiv += p * log(p/q)
        }
        i += 1
      }
    } else {
      while (i < psize) {
        val word = pkeys(i)
        val pcount = pvalues(i)
        val p = pcount * pfact
        val q = {
          val qcount = qmodel.get_gram(word)
          if (qcount != 0) qcount * qfact
          else {
            val owprob = owprobs(word)
            /* The old way:
            if (owprob != 0.0) owprob * qfact_unseen
            else qfact_globally_unseen_prob
            */
            /* The new way: No need for a globally unseen probability. */
            owprob * qfact_unseen
          }
        }
        /* However, in the "new way" we have to notice when a word was never
           seen at all, and ignore it. */
        if (q > 0.0) {
          //if (q == 0.0)
          //  errprint("Strange: word=%s qfact_globally_unseen_prob=%s qcount=%s qfact=%s",
          //           word, qfact_globally_unseen_prob, qcount, qfact)
          //if (p == 0.0 || q == 0.0)
          //  errprint("Warning: zero value: p=%s q=%s word=%s pcount=%s qcount=%s qfact=%s qfact_unseen=%s owprobs=%s",
          //      p, q, word, pcount, qcount, qfact, qfact_unseen,
          //      owprobs(word))
          kldiv += p * (log(p) - log(q))
        }
        i += 1
      }
    }
  
    if (partial)
      return kldiv

    // 2.
    var overall_probs_diff_words = 0.0
    for ((word, qcount) <- qmodel.iter_grams if !(pmodel contains word)) {
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
   * A fast implementation of cosine similarity that inlines lookups as
   * much as possible.  It's always "partial" in that it ignores words
   * neither in P nor Q, despite the fact that they have non-zero
   * probability due to smoothing.  But with parameter "partial" to true we
   * proceed as with KL-divergence and ignore words not in P.
   */
  def fast_smoothed_cosine_similarity(self: TDist, other: TDist,
    partial: Boolean = true): Double = {
    val pfact = (1.0 - self.unseen_mass)/self.num_tokens
    val qfact = (1.0 - other.unseen_mass)/other.num_tokens
    val qfact_unseen = other.unseen_mass / other.overall_unseen_mass
    val factory = self.factory
    /* Not needed in the new way
    val qfact_globally_unseen_prob = (other.unseen_mass*
        factory.globally_unseen_word_prob /
        factory.total_num_unseen_word_types)
    */
    val owprobs = factory.overall_word_probs
    val pmodel = self.model
    val qmodel = other.model

    // 1.

    // FIXME!! Length of p is the same for all calls of fast_cosine_similarity
    // on this gram, so we could cache it.  Not clear it would save much
    // time, though.
    var pqsum = 0.0
    var p2sum = 0.0
    var q2sum = 0.0
    for ((word, pcount) <- pmodel.iter_grams) {
      val p = pcount * pfact
      val q = {
        val qcount = qmodel.get_gram(word)
        val owprob = owprobs(word)
        qcount * qfact + owprob * qfact_unseen
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
    for ((word, qcount) <- qmodel.iter_grams if !(pmodel contains word)) {
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
   * A fast implementation of cosine similarity that inlines lookups as
   * much as possible. With parameter "partial" to true we
   * proceed as with KL-divergence and ignore words not in P.
   */
  def fast_cosine_similarity(self: TDist, other: TDist,
    partial: Boolean = true) = {
    val pfact = 1.0/self.num_tokens
    val qfact = 1.0/other.num_tokens
    // 1.
    val pmodel = self.model
    val qmodel = other.model

    // FIXME!! Length of p is the same for all calls of fast_cosine_similarity
    // on this gram, so we could cache it.  Not clear it would save much
    // time, though.
    var pqsum = 0.0
    var p2sum = 0.0
    var q2sum = 0.0
    for ((word, pcount) <- pmodel.iter_grams) {
      val p = pcount * pfact
      val q = qmodel.get_gram(word) * qfact
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
    for ((word, qcount) <- qmodel.iter_grams if !(pmodel contains word)) {
      val q = qcount * qfact
      q2sum += q * q
    }
  
    if (pqsum == 0.0) 0.0 else pqsum / (sqrt(p2sum) * sqrt(q2sum))
  }
}
