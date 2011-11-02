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

import tgutil._
import GeolocateDriver.Debug._
import WordDist.memoizer._
import WordDist.SmoothedWordDist

import math._
import collection.mutable

/**
 * Extract out the behavior related to the pseudo Good-Turing smoother.
 */
object PseudoGoodTuringSmoothedWordDist {
  // Total number of types seen once
  var total_num_types_seen_once = 0

  // Estimate of number of unseen word types for all articles
  var total_num_unseen_word_types = 0

  /**
   * Overall probabilities over all articles of seeing a word in an article,
   * for all words seen at least once in any article, computed using the
   * empirical frequency of a word among all articles, adjusted by the mass
   * to be assigned to globally unseen words (words never seen at all), i.e.
   * the value in 'globally_unseen_word_prob'.  We start out by storing raw
   * counts, then adjusting them.
   */
  var overall_word_probs = create_word_double_map()
  var owp_adjusted = false

  // The total probability mass to be assigned to words not seen at all in
  // any article, estimated using Good-Turing smoothing as the unadjusted
  // empirical probability of having seen a word once.
  var globally_unseen_word_prob = 0.0

  // For articles whose word counts are not known, use an empty list to
  // look up in.
  // unknown_article_counts = ([], [])

  def finish_global_distribution() = {
    /* We do in-place conversion of counts to probabilities.  Make sure
       this isn't done twice!! */
    assert (!owp_adjusted)
    owp_adjusted = true
    // Now, adjust overall_word_probs accordingly.
    //// FIXME: A simple calculation reveals that in the scheme where we use
    //// globally_unseen_word_prob, total_num_types_seen_once cancels out and
    //// we never actually have to compute it.
    total_num_types_seen_once = overall_word_probs.values count (_ == 1.0)
    globally_unseen_word_prob =
      total_num_types_seen_once.toDouble/WordDist.total_num_word_tokens
    for ((word, count) <- overall_word_probs)
      overall_word_probs(word) = (
        count.toDouble/WordDist.total_num_word_tokens*
        (1.0 - globally_unseen_word_prob))
    // A very rough estimate, perhaps totally wrong
    total_num_unseen_word_types =
      total_num_types_seen_once max (WordDist.total_num_word_types/20)
    if (debug("tons"))
      errprint("Total num types = %s, total num tokens = %s, total num_seen_once = %s, globally unseen word prob = %s, total mass = %s",
               WordDist.total_num_word_types, WordDist.total_num_word_tokens,
               total_num_types_seen_once,
               globally_unseen_word_prob,
               globally_unseen_word_prob + (overall_word_probs.values sum))
  }
}

/**
 * Create a pseudo-Good-Turing smoothed word distribution given a table
 * listing counts for each word, initialized from the given key/value pairs.
 *
 * @param key Array holding keys, possibly over-sized, so that the internal
 *   arrays from DynamicArray objects can be used
 * @param values Array holding values corresponding to each key, possibly
 *   oversize
 * @param num_words Number of actual key/value pairs to be stored 
 * @param note_globally If true, add the word counts to the global word count
 *   statistics.
 */

class PseudoGoodTuringSmoothedWordDist(
  keys: Array[Word],
  values: Array[Int],
  num_words: Int,
  note_globally: Boolean=true
) extends UnigramWordDist(keys, values, num_words) {
  val FastAlgorithms = FastPseudoGoodTuringSmoothedWordDist
  val compobj = PseudoGoodTuringSmoothedWordDist

  /** Total probability mass to be assigned to all words not
      seen in the article, estimated (motivated by Good-Turing
      smoothing) as the unadjusted empirical probability of
      having seen a word once.
   */
  var unseen_mass = 0.5
  /**
     Probability mass assigned in 'overall_word_probs' to all words not seen
     in the article.  This is 1 - (sum over W in A of overall_word_probs[W]).
     The idea is that we compute the probability of seeing a word W in
     article A as

     -- if W has been seen before in A, use the following:
          COUNTS[W]/TOTAL_TOKENS*(1 - UNSEEN_MASS)
     -- else, if W seen in any articles (W in 'overall_word_probs'),
        use UNSEEN_MASS * (overall_word_probs[W] / OVERALL_UNSEEN_MASS).
        The idea is that overall_word_probs[W] / OVERALL_UNSEEN_MASS is
        an estimate of p(W | W not in A).  We have to divide by
        OVERALL_UNSEEN_MASS to make these probabilities be normalized
        properly.  We scale p(W | W not in A) by the total probability mass
        we have available for all words not seen in A.
     -- else, use UNSEEN_MASS * globally_unseen_word_prob / NUM_UNSEEN_WORDS,
        where NUM_UNSEEN_WORDS is an estimate of the total number of words
        "exist" but haven't been seen in any articles.  One simple idea is
        to use the number of words seen once in any article.  This certainly
        underestimates this number if not too many articles have been seen
        but might be OK if many articles seen.
    */
  var overall_unseen_mass = 1.0

  if (note_globally) {
    assert(!compobj.owp_adjusted)
    for ((word, count) <- counts) {
      if (!(compobj.overall_word_probs contains word))
        WordDist.total_num_word_types += 1
      // Record in overall_word_probs; note more tokens seen.
      compobj.overall_word_probs(word) += count
      WordDist.total_num_word_tokens += count
    }
  }

  def innerToString = ", %.2f unseen mass" format unseen_mass

   /**
    * Here we compute the value of `overall_unseen_mass`, which depends
    * on the global `overall_word_probs` computed from all of the
    * distributions.
    */

  def finish_after_global() {
    // Make sure that overall_word_probs has been computed properly.
    assert(compobj.owp_adjusted)

    // Compute probabilities.  Use a very simple version of Good-Turing
    // smoothing where we assign to unseen words the probability mass of
    // words seen once, and adjust all other probs accordingly.
    val num_types_seen_once = counts.values count (_ == 1)
    unseen_mass =
      if (num_word_tokens > 0)
        // If no words seen only once, we will have a problem if we assign 0
        // to the unseen mass, as unseen words will end up with 0 probability.
        // However, if we assign a value of 1.0 to unseen_mass (which could
        // happen in case all words seen exactly once), then we will end
        // up assigning 0 probability to seen words.  So we arbitrarily
        // limit it to 0.5, which is pretty damn much mass going to unseen
        // words.
        0.5 min ((1.0 max num_types_seen_once)/num_word_tokens)
      else 0.5
    overall_unseen_mass = 1.0 - (
      (for (ind <- counts.keys)
        yield compobj.overall_word_probs(ind)) sum)
    //if (use_sorted_list)
    //  counts = new SortedList(counts)
    finished = true
  }

  def fast_kl_divergence(other: WordDist, partial: Boolean=false) =
    FastAlgorithms.fast_kl_divergence(this.asInstanceOf[SmoothedWordDist],
      other.asInstanceOf[SmoothedWordDist], partial = partial)

  def fast_cosine_similarity(other: WordDist, partial: Boolean=false) =
    FastAlgorithms.fast_cosine_similarity(this.asInstanceOf[SmoothedWordDist],
      other.asInstanceOf[SmoothedWordDist], partial = partial)

  def fast_smoothed_cosine_similarity(other: WordDist, partial: Boolean=false) =
    FastAlgorithms.fast_smoothed_cosine_similarity(
      this.asInstanceOf[SmoothedWordDist],
      other.asInstanceOf[SmoothedWordDist], partial = partial)

  def kl_divergence_34(other: UnigramWordDist) = {      
    var overall_probs_diff_words = 0.0
    for (word <- other.counts.keys if !(counts contains word)) {
      overall_probs_diff_words +=
        compobj.overall_word_probs(word)
    }

    inner_kl_divergence_34(other.asInstanceOf[SmoothedWordDist],
      overall_probs_diff_words)
  }
      
  /**
   * Actual implementation of steps 3 and 4 of KL-divergence computation, given
   * a value that we may want to compute as part of step 2.
   */
  def inner_kl_divergence_34(other: SmoothedWordDist,
      overall_probs_diff_words: Double) = {
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
    val p = (unseen_mass*compobj.globally_unseen_word_prob /
          compobj.total_num_unseen_word_types)
    val q = (other.unseen_mass*compobj.globally_unseen_word_prob /
          compobj.total_num_unseen_word_types)
    kldiv += compobj.total_num_unseen_word_types*(p*(log(p) - log(q)))
    kldiv
  }

  def lookup_word(word: Word) = {
    assert(finished)
    // if (debug("some")) {
    //   errprint("Found counts for article %s, num word types = %s",
    //            art, wordcounts(0).length)
    //   errprint("Unknown prob = %s, overall_unseen_mass = %s",
    //            unseen_mass, overall_unseen_mass)
    // }
    val retval = counts.get(word) match {
      case None => {
        compobj.overall_word_probs.get(word) match {
          case None => {
            val wordprob = (unseen_mass*compobj.globally_unseen_word_prob
                      / compobj.total_num_unseen_word_types)
            if (debug("lots"))
              errprint("Word %s, never seen at all, wordprob = %s",
                       unmemoize_word(word), wordprob)
            wordprob
          }
          case Some(owprob) => {
            val wordprob = unseen_mass * owprob / overall_unseen_mass
            //if (wordprob <= 0)
            //  warning("Bad values; unseen_mass = %s, overall_word_probs[word] = %s, overall_unseen_mass = %s",
            //    unseen_mass, compobj.overall_word_probs[word],
            //    compobj.overall_unseen_mass)
            if (debug("lots"))
              errprint("Word %s, seen but not in article, wordprob = %s",
                       unmemoize_word(word), wordprob)
            wordprob
          }
        }
      }
      case Some(wordcount) => {
        //if (wordcount <= 0 or num_word_tokens <= 0 or unseen_mass >= 1.0)
        //  warning("Bad values; wordcount = %s, unseen_mass = %s",
        //          wordcount, unseen_mass)
        //  for ((word, count) <- self.counts)
        //    errprint("%s: %s", word, count)
        val wordprob = wordcount.toDouble/num_word_tokens*(1.0 - unseen_mass)
        if (debug("lots"))
          errprint("Word %s, seen in article, wordprob = %s",
                   unmemoize_word(word), wordprob)
        wordprob
      }
    }
    retval
  }
}

