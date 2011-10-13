package opennlp.textgrounder.geolocate

import math.{log, sqrt}

object KLDiv {
  /**
   A fast implementation of KL-divergence that uses inline lookups as much
   as possible.
   */
  def fast_kl_divergence(self: WordDist, other: WordDist,
    partial: Boolean=false): Double = {
    var kldiv = 0.0
    val pfact = (1.0 - self.unseen_mass)/self.total_tokens
    val qfact = (1.0 - other.unseen_mass)/other.total_tokens
    val pfact_unseen = self.unseen_mass / self.overall_unseen_mass
    val qfact_unseen = other.unseen_mass / other.overall_unseen_mass
    val qfact_globally_unseen_prob = (other.unseen_mass*
        WordDist.globally_unseen_word_prob / WordDist.num_unseen_word_types)
    val owprobs = WordDist.overall_word_probs
    // 1.
    val pcounts = self.counts
    val qcounts = other.counts
    // FIXME!! p * log(p) is the same for all calls of fast_kl_divergence
    // on this item, so we could cache it.  Not clear it would save much
    // time, though.
    for ((word, pcount) <- pcounts) {
      val p = pcount * pfact
      val q = qcounts.get(word) match {
        case Some(x) => x * qfact
        case None => {
          owprobs.get(word) match {
            case Some(owprob) => owprob * qfact_unseen
            case None => qfact_globally_unseen_prob
          }
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
    }
  
    if (partial)
      return kldiv

    // 2.
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
    var pqsum = 0.0
    var p2sum = 0.0
    var q2sum = 0.0
    val pfact = (1.0 - self.unseen_mass)/self.total_tokens
    val qfact = (1.0 - other.unseen_mass)/other.total_tokens
    val pfact_unseen = self.unseen_mass / self.overall_unseen_mass
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
    for ((word, pcount) <- pcounts) {
      val p = pcount * pfact
      val q = qcounts.get(word) match {
        case Some(x) => x * qfact
        case None => {
          owprobs.get(word) match {
            case Some(owprob) => owprob * qfact_unseen
            case None => qfact_globally_unseen_prob
          }
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
    var pqsum = 0.0
    var p2sum = 0.0
    var q2sum = 0.0
    val pfact = 1.0/self.total_tokens
    val qfact = 1.0/other.total_tokens
    // 1.
    val pcounts = self.counts
    val qcounts = other.counts
    // FIXME!! Length of p is the same for all calls of fast_cosine_similarity
    // on this item, so we could cache it.  Not clear it would save much
    // time, though.
    for ((word, pcount) <- pcounts) {
      val p = pcount * pfact
      val q = qcounts.get(word) match {
        case Some(x) => x * qfact
        case None => 0.0
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
  
    // 2.
    if (!partial)
    for ((word, qcount) <- qcounts if !(pcounts contains word)) {
      val q = qcount * qfact
      q2sum += q * q
    }
  
    if (pqsum == 0.0) 0.0 else pqsum / (sqrt(p2sum) * sqrt(q2sum))
  }
}
