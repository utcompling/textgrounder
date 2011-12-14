///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010, 2011 Ben Wing, The University of Texas at Austin
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

////////
//////// PseudoGoodTuringSmoothedWordDist.scala
////////
//////// Copyright (c) 2010, 2011 Ben Wing.
////////

package opennlp.textgrounder.geolocate

import math._
import collection.mutable
import util.control.Breaks._

import java.io._

import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.printutil.errprint

import GridLocateDriver.Debug._
import WordDist.memoizer._
import GenericTypes._

/**
 * This class implements a simple version of Good-Turing smoothing where we
 * assign probability mass to unseen words equal to the probability mass of
 * all words seen once, and rescale the remaining probabilities accordingly.
 * ("Proper" Good-Turing is more general and adjusts the probabilities of
 * words seen N times according to the number of words seen N-1 times.
 * FIXME: I haven't thought carefully enough to make sure that this
 * simplified version actually makes theoretical sense, although I assume it
 * does because it can be seen as a better version of add-one or
 * add-some-value smoothing, where instead of just adding some arbitrary
 * value to unseen words, we determine the amount to add in a way that makes
 * theoretical sense (and in particular ensures that we don't assign 99%
 * of the mass or whatever to unseen words, as can happen to add-one smoothing
 * especially for bigrams or trigrams).
 */ 
class PseudoGoodTuringSmoothedWordDistFactory extends
    UnigramWordDistFactory {
  // Total number of types seen once
  var total_num_types_seen_once = 0

  // Estimate of number of unseen word types for all documents
  var total_num_unseen_word_types = 0

  /**
   * Overall probabilities over all documents of seeing a word in a document,
   * for all words seen at least once in any document, computed using the
   * empirical frequency of a word among all documents, adjusted by the mass
   * to be assigned to globally unseen words (words never seen at all), i.e.
   * the value in 'globally_unseen_word_prob'.  We start out by storing raw
   * counts, then adjusting them.
   */
  var overall_word_probs = create_word_double_map()
  var owp_adjusted = false

  // The total probability mass to be assigned to words not seen at all in
  // any document, estimated using Good-Turing smoothing as the unadjusted
  // empirical probability of having seen a word once.
  var globally_unseen_word_prob = 0.0

  // For documents whose word counts are not known, use an empty list to
  // look up in.
  // unknown_document_counts = ([], [])

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
      total_num_types_seen_once.toDouble/total_num_word_tokens
    for ((word, count) <- overall_word_probs)
      overall_word_probs(word) = (
        count.toDouble/total_num_word_tokens*(1.0 - globally_unseen_word_prob))
    // A very rough estimate, perhaps totally wrong
    total_num_unseen_word_types =
      total_num_types_seen_once max (total_num_word_types/20)
    if (debug("tons"))
      errprint("Total num types = %s, total num tokens = %s, total num_seen_once = %s, globally unseen word prob = %s, total mass = %s",
               total_num_word_types, total_num_word_tokens,
               total_num_types_seen_once, globally_unseen_word_prob,
               globally_unseen_word_prob + (overall_word_probs.values sum))
  }

  def create_word_dist() =
    new PseudoGoodTuringSmoothedWordDist(this, Array[Word](), Array[Int](), 0,
      note_globally = false)

  def set_unigram_word_dist(doc: GenericDistDocument, keys: Array[Word],
      values: Array[Int], num_words: Int, is_training_set: Boolean) {
    doc.dist = new PseudoGoodTuringSmoothedWordDist(this, keys, values,
      num_words, note_globally = is_training_set)
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
  val factory: PseudoGoodTuringSmoothedWordDistFactory,
  keys: Array[Word],
  values: Array[Int],
  num_words: Int,
  note_globally: Boolean
) extends UnigramWordDist(keys, values, num_words) {
  type TThis = PseudoGoodTuringSmoothedWordDist

  /** Total probability mass to be assigned to all words not
      seen in the document, estimated (motivated by Good-Turing
      smoothing) as the unadjusted empirical probability of
      having seen a word once.
   */
  var unseen_mass = 0.5
  /**
     Probability mass assigned in 'overall_word_probs' to all words not seen
     in the document.  This is 1 - (sum over W in A of overall_word_probs[W]).
     The idea is that we compute the probability of seeing a word W in
     document A as

     -- if W has been seen before in A, use the following:
          COUNTS[W]/TOTAL_TOKENS*(1 - UNSEEN_MASS)
     -- else, if W seen in any documents (W in 'overall_word_probs'),
        use UNSEEN_MASS * (overall_word_probs[W] / OVERALL_UNSEEN_MASS).
        The idea is that overall_word_probs[W] / OVERALL_UNSEEN_MASS is
        an estimate of p(W | W not in A).  We have to divide by
        OVERALL_UNSEEN_MASS to make these probabilities be normalized
        properly.  We scale p(W | W not in A) by the total probability mass
        we have available for all words not seen in A.
     -- else, use UNSEEN_MASS * globally_unseen_word_prob / NUM_UNSEEN_WORDS,
        where NUM_UNSEEN_WORDS is an estimate of the total number of words
        "exist" but haven't been seen in any documents.  One simple idea is
        to use the number of words seen once in any document.  This certainly
        underestimates this number if not too many documents have been seen
        but might be OK if many documents seen.
    */
  var overall_unseen_mass = 1.0

  if (note_globally) {
    assert(!factory.owp_adjusted)
    for ((word, count) <- counts) {
      if (!(factory.overall_word_probs contains word))
        factory.total_num_word_types += 1
      // Record in overall_word_probs; note more tokens seen.
      factory.overall_word_probs(word) += count
      // Our training docs should never have partial (interpolated) counts.
      assert (count == count.toInt)
      factory.total_num_word_tokens += count.toInt
    }
  }

  def innerToString = ", %.2f unseen mass" format unseen_mass

   /**
    * Here we compute the value of `overall_unseen_mass`, which depends
    * on the global `overall_word_probs` computed from all of the
    * distributions.
    */

  protected def imp_finish_after_global() {
    // Make sure that overall_word_probs has been computed properly.
    assert(factory.owp_adjusted)

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
        yield factory.overall_word_probs(ind)) sum)
    //if (use_sorted_list)
    //  counts = new SortedList(counts)
  }

  override def finish(minimum_word_count: Int = 0) {
    super.finish(minimum_word_count)
    if (debug("lots")) {
      errprint("""For word dist, total tokens = %s, unseen_mass = %s, overall unseen mass = %s""",
        num_word_tokens, unseen_mass, overall_unseen_mass)
    }
  }

  def fast_kl_divergence(other: WordDist, partial: Boolean = false) = {
    FastPseudoGoodTuringSmoothedWordDist.fast_kl_divergence(
      this.asInstanceOf[TThis], other.asInstanceOf[TThis],
      partial = partial)
  }

  def cosine_similarity(other: WordDist, partial: Boolean = false,
      smoothed: Boolean = false) = {
    if (smoothed)
      FastPseudoGoodTuringSmoothedWordDist.fast_smoothed_cosine_similarity(
        this.asInstanceOf[TThis], other.asInstanceOf[TThis],
        partial = partial)
    else
      FastPseudoGoodTuringSmoothedWordDist.fast_cosine_similarity(
        this.asInstanceOf[TThis], other.asInstanceOf[TThis],
        partial = partial)
  }

  def kl_divergence_34(other: UnigramWordDist) = {      
    var overall_probs_diff_words = 0.0
    for (word <- other.counts.keys if !(counts contains word)) {
      overall_probs_diff_words += factory.overall_word_probs(word)
    }

    inner_kl_divergence_34(other.asInstanceOf[TThis],
      overall_probs_diff_words)
  }
      
  /**
   * Actual implementation of steps 3 and 4 of KL-divergence computation, given
   * a value that we may want to compute as part of step 2.
   */
  def inner_kl_divergence_34(other: TThis,
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
    val p = (unseen_mass*factory.globally_unseen_word_prob /
          factory.total_num_unseen_word_types)
    val q = (other.unseen_mass*factory.globally_unseen_word_prob /
          factory.total_num_unseen_word_types)
    kldiv += factory.total_num_unseen_word_types*(p*(log(p) - log(q)))
    kldiv
  }

  def lookup_word(word: Word) = {
    assert(finished)
    // if (debug("some")) {
    //   errprint("Found counts for document %s, num word types = %s",
    //            doc, wordcounts(0).length)
    //   errprint("Unknown prob = %s, overall_unseen_mass = %s",
    //            unseen_mass, overall_unseen_mass)
    // }
    val retval = counts.get(word) match {
      case None => {
        factory.overall_word_probs.get(word) match {
          case None => {
            val wordprob = (unseen_mass*factory.globally_unseen_word_prob
                      / factory.total_num_unseen_word_types)
            if (debug("lots"))
              errprint("Word %s, never seen at all, wordprob = %s",
                       unmemoize_string(word), wordprob)
            wordprob
          }
          case Some(owprob) => {
            val wordprob = unseen_mass * owprob / overall_unseen_mass
            //if (wordprob <= 0)
            //  warning("Bad values; unseen_mass = %s, overall_word_probs[word] = %s, overall_unseen_mass = %s",
            //    unseen_mass, factory.overall_word_probs[word],
            //    factory.overall_unseen_mass)
            if (debug("lots"))
              errprint("Word %s, seen but not in document, wordprob = %s",
                       unmemoize_string(word), wordprob)
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
          errprint("Word %s, seen in document, wordprob = %s",
                   unmemoize_string(word), wordprob)
        wordprob
      }
    }
    retval
  }
}

