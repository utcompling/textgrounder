package opennlp.textgrounder.geolocate

import scala.collection.mutable
import math.{log, sqrt}

import NlpUtil.DynamicArray
import WordDist.memoizer.Word

/**
  This code was originally broken out of WordDist so that it could be
  rewritten in .pyc (Python with extra 'cdef' annotations that can be
  converted to C and compiled down to machine language).  Kept separated
  in case we need to convert it to Java, C++, etc.
 */

object KLDiv {
  type Hashtab = mutable.Map[Word, Int]
  /* The only code that knows about how hash tables are implemented. */
  def get_keys(hash:Hashtab, array:Array[Word]) = {
    hash.keys.copyToArray(array)
    array
  }
  def get_values(hash:Hashtab, array:Array[Int]) = {
    hash.values.copyToArray(array)
    array
  }

  /*
   For very fast access, the Trove code allows you to retrieve all keys or
   values into a given array.  This should be faster than executing a
   function call on each (key, value) pair.  The retrieval code in Trove
   accepts an array and will automatically create a larger one if it's not
   big enough, but we want to do this ourselves so we have control over the
   array recreation, to make sure this doesn't happen too often.
   */
  protected val initial_static_array_size = 1000
  protected val static_key_array =
    new DynamicArray[Word](initial_alloc = initial_static_array_size)
  protected val static_value_array =
    new DynamicArray[Int](initial_alloc = initial_static_array_size)
  protected def size_static_arrays(size: Int) {
    static_key_array.ensure_at_least(size)
    static_value_array.ensure_at_least(size)
  }
  protected var cached_worddist: WordDist = null
  protected var cached_size: Int = 0

  def setup_static_arrays(self: WordDist) {
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
    // Make sure we didn't mess up and pass in too-small arrays.
    val new_keys = get_keys(pcounts, keys)
    assert(keys eq new_keys)
    val new_values = get_values(pcounts, values)
    assert(values eq new_values)
  }

  /**
   A fast implementation of KL-divergence that uses inline lookups as much
   as possible.
   */
  def fast_kl_divergence(self: WordDist, other: WordDist,
    partial: Boolean=false): Double = {
    val pfact = (1.0 - self.unseen_mass)/self.total_tokens
    val qfact = (1.0 - other.unseen_mass)/other.total_tokens
    val qfact_unseen = other.unseen_mass / other.overall_unseen_mass
    val qfact_globally_unseen_prob = (other.unseen_mass*
        WordDist.globally_unseen_word_prob / WordDist.num_unseen_word_types)
    val owprobs = WordDist.overall_word_probs
    val pcounts = self.counts
    val qcounts = other.counts
    var i = 0

    /** FIXME: How much this use of static arrays actually helps is
        debatable.  Hotspot is pretty good at inlining function calls,
        and it might be smart enough to inline even through function
        pointers -- if so, the use of arrays doesn't buy anything. */
    // 1.

    setup_static_arrays(self)
    val pkeys = static_key_array.array
    val pvalues = static_value_array.array
    val psize = self.counts.size

    // FIXME!! p * log(p) is the same for all calls of fast_kl_divergence
    // on this item, so we could cache it.  Not clear it would save much
    // time, though.
    var kldiv = 0.0
    i = 0
    /* THIS IS THE INSIDE LOOP.  THIS IS THE CODE BOTTLENECK.  THIS IS IT.
       
       This code needs to scream.  Hence we do extra setup above involving
       static arrays, to avoid having a function call through a function
       pointer (through the "obvious" use of forEach()). FIXME: But see
       comment above.
      
       Note that HotSpot is good about inlining function calls.  Hence we
       can assume that the calls to getOrElse() below will be inlined.
       However, it's *very important* to avoid doing anything that creates
       objects each iteration, and best to avoid creating objects per call
       to fast_kl_divergence().  This object creation will kill us, as it
       will trigger tons and tons of garbage collection.
       
       Recent HotSpot implementations (6.0 rev 14 and above) have "escape
       analysis" that *might* make the object creation magically vanish,
       but don't count on it.
     */
    while (i < psize) {
      val word = pkeys(i)
      val pcount = pvalues(i)
      val p = pcount * pfact
      val q = {
        val qcount = qcounts.getOrElse(word, 0)
        if (qcount != 0) qcount * qfact
        else {
          val owprob = owprobs.getOrElse(word, 0.0)
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
      //      owprobs.getOrElse(word, 0.0))
      kldiv += p * (log(p) - log(q))
      i += 1
    }
  
    if (partial)
      return kldiv

    // 2.
    val pfact_unseen = self.unseen_mass / self.overall_unseen_mass
    var overall_probs_diff_words = 0.0
    for ((word, qcount) <- qcounts if !(pcounts contains word)) {
      val word_overall_prob = owprobs.getOrElse(word, 0.0)
      val p = word_overall_prob * pfact_unseen
      val q = qcount * qfact
      kldiv += p * (log(p) - log(q))
      overall_probs_diff_words += word_overall_prob
    }    

    return kldiv + self.kl_divergence_34(other, overall_probs_diff_words)
  }
  
  // The older implementation that uses smoothed probabilities.
  
  /**
   A fast implementation of cosine similarity that uses Cython declarations
  and inlines lookups as much as possible.  It's always "partial" in that it
  ignores words neither in P nor Q, despite the fact that they have non-zero
  probability due to smoothing.  But with parameter "partial" to true we
  proceed as with KL-divergence and ignore words not in P.
   */
  def fast_smoothed_cosine_similarity(self: WordDist, other: WordDist,
    partial: Boolean=false): Double = {
    val pfact = (1.0 - self.unseen_mass)/self.total_tokens
    val qfact = (1.0 - other.unseen_mass)/other.total_tokens
    val qfact_unseen = other.unseen_mass / other.overall_unseen_mass
    val qfact_globally_unseen_prob = (other.unseen_mass*
        WordDist.globally_unseen_word_prob / WordDist.num_unseen_word_types)
    val owprobs = WordDist.overall_word_probs
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
        val qcount = qcounts.getOrElse(word, 0)
        if (qcount != 0) qcount * qfact
        else {
          val owprob = owprobs.getOrElse(word, 0.0)
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
      //      owprobs.getOrElse(word, 0.0))
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
      val word_overall_prob = owprobs.getOrElse(word, 0.0)
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
  def fast_cosine_similarity(self: WordDist, other: WordDist,
    partial: Boolean=false) = {
    val pfact = 1.0/self.total_tokens
    val qfact = 1.0/other.total_tokens
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
      val q = qcounts.getOrElse(word, 0) * qfact
      //if (q == 0.0)
      //  errprint("Strange: word=%s qfact_globally_unseen_prob=%s qcount=%s qfact=%s",
      //           word, qfact_globally_unseen_prob, qcount, qfact)
      //if (p == 0.0 || q == 0.0)
      //  errprint("Warning: zero value: p=%s q=%s word=%s pcount=%s qcount=%s qfact=%s qfact_unseen=%s owprobs=%s",
      //      p, q, word, pcount, qcount, qfact, qfact_unseen,
      //      owprobs.getOrElse(word, 0.0))
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
