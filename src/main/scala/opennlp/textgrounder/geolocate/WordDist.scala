package opennlp.textgrounder.geolocate

import math._
import NlpUtil._
import KLDiv._
import collection.mutable

// val use_sorted_list = false

//////////////////////////////////////////////////////////////////////////////
//                             Word distributions                           //
//////////////////////////////////////////////////////////////////////////////

// Fields defined:
//
//   counts: A "sorted list" (tuple of sorted keys and values) of
//           (word, count) items, specifying the counts of all words seen
//           at least once.
//   finished: Whether we have finished computing the distribution in
//             'counts'.
//   unseen_mass: Total probability mass to be assigned to all words not
//                seen in the article, estimated (motivated by Good-Turing
//                smoothing) as the unadjusted empirical probability of
//                having seen a word once.
//   overall_unseen_mass:
//     Probability mass assigned in 'overall_word_probs' to all words not seen
//     in the article.  This is 1 - (sum over W in A of overall_word_probs[W]).
//     The idea is that we compute the probability of seeing a word W in
//     article A as
//
//     -- if W has been seen before in A, use the following:
//          COUNTS[W]/TOTAL_TOKENS*(1 - UNSEEN_MASS)
//     -- else, if W seen in any articles (W in 'overall_word_probs'),
//        use UNSEEN_MASS * (overall_word_probs[W] / OVERALL_UNSEEN_MASS).
//        The idea is that overall_word_probs[W] / OVERALL_UNSEEN_MASS is
//        an estimate of p(W | W not in A).  We have to divide by
//        OVERALL_UNSEEN_MASS to make these probabilities be normalized
//        properly.  We scale p(W | W not in A) by the total probability mass
//        we have available for all words not seen in A.
//     -- else, use UNSEEN_MASS * globally_unseen_word_prob / NUM_UNSEEN_WORDS,
//        where NUM_UNSEEN_WORDS is an estimate of the total number of words
//        "exist" but haven't been seen in any articles.  One simple idea is
//        to use the number of words seen once in any article.  This certainly
//        underestimates this number if not too many articles have been seen
//        but might be OK if many articles seen.
//   total_tokens: Total number of word tokens seen

object WordDist {
  // Total number of word types seen (size of vocabulary)
  var num_word_types = 0

  // Total number of word tokens seen
  var num_word_tokens = 0

  // Total number of types seen once
  var num_types_seen_once = 0

  // Estimate of number of unseen word types for all articles
  var num_unseen_word_types = 0

  // Overall probabilities over all articles of seeing a word in an article,
  // for all words seen at least once in any article, computed using the
  // empirical frequency of a word among all articles, adjusted by the mass
  // to be assigned to globally unseen words (words never seen at all), i.e. the
  // value in 'globally_unseen_word_prob'.
  val overall_word_counts = intmap()

  var overall_word_probs:mutable.Map[String,Double] = null

  // The total probability mass to be assigned to words not seen at all in
  // any article, estimated using Good-Turing smoothing as the unadjusted
  // empirical probability of having seen a word once.
  var globally_unseen_word_prob = 0.0

  // For articles whose word counts are not known, use an empty list to
  // look up in.
  // unknown_article_counts = ([], [])

  def finish_global_distribution() = {
    // Now, adjust overall_word_probs accordingly.
    //// FIXME: A simple calculation reveals that in the scheme where we use
    //// globally_unseen_word_prob, num_types_seen_once cancels out and
    //// we never actually have to compute it.
    num_types_seen_once = overall_word_counts.values count (_ == 1)
    globally_unseen_word_prob = (
      num_types_seen_once.toDouble/num_word_tokens)
    overall_word_probs =
      for ((wordind,count) <- overall_word_counts)
        yield (wordind, count.toDouble/num_word_tokens*
                        (1 - globally_unseen_word_prob))
    // A very rough estimate, perhaps totally wrong
    num_unseen_word_types = num_types_seen_once max (num_word_types/20)
    if (debug("tons"))
      errprint("Num types = %s, num tokens = %s, num_seen_once = %s, globally unseen word prob = %s, total mass = %s", num_word_types, num_word_tokens, num_types_seen_once, globally_unseen_word_prob, globally_unseen_word_prob + (overall_word_probs.values sum))
  }
}

class WordDist {
  var finished = false
  var counts:mutable.Map[String,Int] = intmap()
  var unseen_mass = 0.5
  var total_tokens = 0
  var overall_unseen_mass = 1.0

    /**
 Set the word distribution from the given table of words and counts.
'total_tokens' is the total number of word tokens.  If 'note_globally',
add the word counts to the global word count statistics.
     */
  def this(total_toks:Int, wordhash:mutable.Map[String,Int],
      note_globally:Boolean=true) {
    this()
    total_tokens = total_toks
    if (note_globally) {
      for ((ind, count) <- wordhash) {
        if (!(WordDist.overall_word_counts contains ind))
          WordDist.num_word_types += 1
        // Record in overall_word_counts; note more tokens seen.
        WordDist.overall_word_counts(ind) += count
        WordDist.num_word_tokens += count
      }
    }
    counts = wordhash
  }

  override def toString = {
    val finished_str =
      if (!finished) ", unfinished" else ""
    val num_words_to_print = 15
    val need_dots = counts.size > num_words_to_print
    val items =
      for ((word, count) <- counts.view(0, num_words_to_print))
      yield "%s=%s" format (word, count) 
    val words = (items mkString " ") + (if (need_dots) " ..." else "")
    "WordDist(%d tokens, %.2f unseen mass%s, %s)" format (
        total_tokens, unseen_mass, finished_str, words)
  }

  /**
   * Incorporate a list of words into the distribution.
   */
  def add_words(words:Traversable[String], ignore_case:Boolean=true,
      stopwords:Set[String]=Set[String]()) {
    assert(!finished)
    for {word <- words
         val wlower = if (ignore_case) word.toLowerCase() else word
         if !stopwords(wlower) }
      counts(word) += 1
      total_tokens += 1
  }

  /**
   * Incorporate counts from the given distribution into our distribution.
   */
  def add_word_distribution(worddist: WordDist) {
    assert (!finished)
    for ((word, count) <- worddist.counts)
      counts(word) += count
    total_tokens += worddist.total_tokens
  }

  /**
  Finish computation of the word distribution.
  */
  def finish(minimum_word_count:Int=0) {

    // If 'minimum_word_count' was given, then eliminate words whose count
    // is too small.
    if (minimum_word_count > 1)
      for ((word, count) <- counts if count < minimum_word_count) {
        total_tokens -= count
        counts -= word
      }

    // make sure counts not None (eg article in coords file but not counts file)
    if (counts == null || finished) return
    // Compute probabilities.  Use a very simple version of Good-Turing
    // smoothing where we assign to unseen words the probability mass of
    // words seen once, and adjust all other probs accordingly.
    WordDist.num_types_seen_once = counts.values count (_ == 1)
    unseen_mass =
    if (total_tokens > 0)
      // If no words seen only once, we will have a problem if we assign 0
      // to the unseen mass, as unseen words will end up with 0 probability.
      // However, if we assign a value of 1.0 to unseen_mass (which could
      // happen in case all words seen exactly once), then we will end
      // up assigning 0 probability to seen words.  So we arbitrarily
      // limit it to 0.5, which is pretty damn much mass going to unseen
      // words.
        0.5 min (
          (1.0 max WordDist.num_types_seen_once)/total_tokens).toDouble
    else 0.5
    overall_unseen_mass = 1.0 - (
      (for (ind <- counts.keys)
        yield WordDist.overall_word_probs.getOrElse(ind, 0.0)) sum)
    //if use_sorted_list:
    //  self.counts = SortedList(self.counts)
    finished = true
  }

    /**
     Check fast and slow versions against each other.
     */
  def test_kl_divergence(other:WordDist, partial:Boolean=false) {
    assert(finished)
    assert(other.finished)
    val fast_kldiv = fast_kl_divergence(this, other, partial)
    val slow_kldiv = slow_kl_divergence(other, partial)
    if (abs(fast_kldiv - slow_kldiv) > 1e-8) {
      errprint("Fast KL-div=%s but slow KL-div=%s", fast_kldiv, slow_kldiv)
      assert(fast_kldiv == slow_kldiv)
    }
    fast_kldiv
  }

  // Compute the KL divergence between this distribution and another
  // distribution.  This is a bit tricky.  We have to take into account:
  // 1. Words in this distribution (may or may not be in the other).
  // 2. Words in the other distribution that are not in this one.
  // 3. Words in neither distribution but seen globally.
  // 4. Words never seen at all.
  // If 'return_contributing_words', return a tuple of (val, word_contribs)
  //   where word_contribs is a table of words and the amount each word
  //   contributes to the KL divergence.
    /**
     The basic implementation of KL-divergence.  Useful for checking against
other implementations.
     */
  def slow_kl_divergence_debug(other:WordDist, partial:Boolean=false,
      return_contributing_words:Boolean=false) = {
    assert(finished)
    assert(other.finished)
    var kldiv = 0.0
    val contribs =
      if (return_contributing_words) mutable.Map[String, Double]() else null
    // 1.
    for (word <- counts.keys) {
      val p = lookup_word(word)
      val q = other.lookup_word(word)
      if (p <= 0.0 || q <= 0.0)
        errprint("Warning: problematic values: p=%s, q=%s, word=%s", p, q, word)
      else {
        kldiv += p*(log(p) - log(q))
        if (return_contributing_words)
          contribs(word) = p*(log(p) - log(q))
      }
    }

    if (partial)
      (kldiv, contribs)
    else {
    // 2.
    var overall_probs_diff_words = 0.0
    for (word <- other.counts.keys if !(counts contains word)) {
      val p = lookup_word(word)
      val q = other.lookup_word(word)
      kldiv += p*(log(p) - log(q))
      if (return_contributing_words)
        contribs(word) = p*(log(p) - log(q))
      overall_probs_diff_words +=
        WordDist.overall_word_probs.getOrElse(word, 0.0)
    }

    val retval = kldiv + kl_divergence_34(other, overall_probs_diff_words)
    (retval, contribs)
    }
  }

  def slow_kl_divergence(other:WordDist, partial:Boolean=false) = {
    val (kldiv, contribs) = slow_kl_divergence_debug(other, partial, false)
    kldiv
  }

  /**
   Steps 3 and 4 of KL-divergence computation.
   */
  def kl_divergence_34(other:WordDist, overall_probs_diff_words:Double) = {
    var kldiv = 0.0

    // 3. For words seen in neither dist but seen globally:
    // You can show that this is
    //
    // factor1 = (log(self.unseen_mass) - log(self.overall_unseen_mass)) -
    //           (log(other.unseen_mass) - log(other.overall_unseen_mass))
    // factor2 = self.unseen_mass / self.overall_unseen_mass * factor1
    // kldiv = factor2 * (sum(words seen globally but not in either dist)
    //                    of overall_word_probs[word]) 
    //
    // The final sum
    //   = 1 - sum(words in self) overall_word_probs[word]
    //       - sum(words in other, not self) overall_word_probs[word]
    //   = self.overall_unseen_mass
    //       - sum(words in other, not self) overall_word_probs[word]
    //
    // So we just need the sum over the words in other, not self.

    val factor1 = ((log(unseen_mass) - log(overall_unseen_mass)) -
               (log(other.unseen_mass) - log(other.overall_unseen_mass)))
    val factor2 = unseen_mass / overall_unseen_mass * factor1
    val the_sum = overall_unseen_mass - overall_probs_diff_words
    kldiv += factor2 * the_sum

    // 4. For words never seen at all:
    val p = (unseen_mass*WordDist.globally_unseen_word_prob /
          WordDist.num_unseen_word_types)
    val q = (other.unseen_mass*WordDist.globally_unseen_word_prob /
          WordDist.num_unseen_word_types)
    kldiv += WordDist.num_unseen_word_types*(p*(log(p) - log(q)))
    kldiv
  }

  def symmetric_kldiv(other:WordDist) = {
    0.5*fast_kl_divergence(this, other) + 0.5*fast_kl_divergence(other, this)
  }

  def lookup_word(word:String) = {
    assert(finished)
    // if (debug("some")) {
    //   errprint("Found counts for article %s, num word types = %s",
    //            art, wordcounts(0).length)
    //   errprint("Unknown prob = %s, overall_unseen_mass = %s",
    //            unseen_mass, overall_unseen_mass)
    // }
    val retval = counts.get(word) match {
      case None => {
        WordDist.overall_word_probs.get(word) match {
          case None => {
            val wordprob = (unseen_mass*WordDist.globally_unseen_word_prob
                      / WordDist.num_unseen_word_types)
            if (debug("lots"))
              errprint("Word %s, never seen at all, wordprob = %s", word, wordprob)
            wordprob
          }
          case Some(owprob) => {
            val wordprob = unseen_mass * owprob / overall_unseen_mass
            //if wordprob <= 0:
            //  warning("Bad values; unseen_mass = %s, overall_word_probs[word] = %s, overall_unseen_mass = %s" % (unseen_mass, WordDist.overall_word_probs[word], WordDist.overall_unseen_mass))
            if (debug("lots"))
              errprint("Word %s, seen but not in article, wordprob = %s",
                      word, wordprob)
            wordprob
          }
        }
      }
      case Some(wordcount) => {
        //if wordcount <= 0 or total_tokens <= 0 or unseen_mass >= 1.0:
        //  warning("Bad values; wordcount = %s, unseen_mass = %s" %
        //          (wordcount, unseen_mass))
        //  for (word, count) in self.counts.iteritems():
        //    errprint("%s: %s" % (word, count))
        val wordprob = wordcount.toDouble/total_tokens*(1.0 - unseen_mass)
        if (debug("lots"))
          errprint("Word %s, seen in article, wordprob = %s",
                  word, wordprob)
        wordprob
      }
    }
    retval
  }
  
  def find_most_common_word(pred:String => Boolean) = {
    // Look for the most common word matching a given predicate.
    // But there may not be any.  max() will raise an error if given an
    // empty sequence, so insert a bogus value into the sequence with a
    // negative count.
    val (maxword, maxcount) =
      ((Seq((null, -1)) ++
        (for ((word, count) <- counts if pred(word))
           yield (word, count)))
       maxBy (_._2))
    maxword
  }
}

