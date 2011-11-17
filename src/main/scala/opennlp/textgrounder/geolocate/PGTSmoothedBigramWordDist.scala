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

import math._
import collection.mutable
import util.control.Breaks._

import java.io._

import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.ioutil.errprint

import GeolocateDriver.Params
import GeolocateDriver.Debug._
import WordDist.memoizer._

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

class PGTSmoothedBigramWordDist(
  val factory: PGTSmoothedBigramWordDistFactory,
  keys: Array[Word],
  values: Array[Int],
  num_words: Int,
  bigramKeys: Array[Word],
  bigramValues: Array[Int],
  num_bigrams: Int,
  val note_globally: Boolean=true
) extends BigramWordDist(keys, values, num_words, bigramKeys, bigramValues, num_bigrams) {
  //val FastAlgorithms = FastPseudoGoodTuringSmoothedWordDist
  type ThisType = PGTSmoothedBigramWordDist

  if (note_globally) {
    //assert(!factory.owp_adjusted)
    for ((word, count) <- counts) {
      if (!(factory.overall_word_probs contains word))
        WordDist.total_num_word_types += 1
      // Record in overall_word_probs; note more tokens seen.
      factory.overall_word_probs(word) += count
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
    finished = true
  }

  override def finish(minimum_word_count: Int = 0) {
    super.finish(minimum_word_count)
    if (debug("lots")) {
      errprint("""For word dist, total tokens = %s, unseen_mass = %s, overall unseen mass = %s""",
        num_word_tokens, unseen_mass, overall_unseen_mass)
    }
  }


  def fast_kl_divergence(xother: WordDist, partial: Boolean=false) = {
if(debug("bigram"))
  errprint("fast_kl_divergence")
    val other = xother.asInstanceOf[BigramWordDist]
    assert(finished)
    assert(other.finished)
    var kldiv = 0.0
    // 1.
    for (word <- counts.keys) {
      val p = lookup_word(word)
      val q = other.lookup_word(word)
      if (p <= 0.0 || q <= 0.0)
        errprint("Warning: problematic values: p=%s, q=%s, word=%s", p, q, word)
      else {
        kldiv += p*(log(p) - log(q))
if(debug("bigram"))
  errprint("kldiv1: " + kldiv + " :p: " + p + " :q: " + q)
      }
    }

    if(partial)
      kldiv
    else {
      // Step 2.
      for (word <- other.counts.keys if !(counts contains word)) {
        val p = lookup_word(word)
        val q = other.lookup_word(word)
        kldiv += p*(log(p) - log(q))
if(debug("bigram"))
  errprint("kldiv2: " + kldiv + " :p: " + p + " :q: " + q)
      }

      val retval = kldiv + kl_divergence_34(other)
      retval
   }
 }

  def fast_cosine_similarity(other: WordDist, partial: Boolean=false) = {
    var kldiv = 0.0
    kldiv
 }

  def fast_smoothed_cosine_similarity(other: WordDist, partial: Boolean=false) = {
    var kldiv = 0.0
    kldiv
 }

  def kl_divergence_34(other: BigramWordDist) = {
    var kldiv = 0.0
    kldiv
 }
   
  
  /**
   * Actual implementation of steps 3 and 4 of KL-divergence computation, given
   * a value that we may want to compute as part of step 2.
   */
  def inner_kl_divergence_34(other: ThisType,
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
if(debug("bigram"))
  errprint("unseen_mass: %s, globally_unseen_word_prob %s, total_num_unseen_word_types %s", unseen_mass, factory.globally_unseen_word_prob, factory.total_num_unseen_word_types)
            val wordprob = (unseen_mass*factory.globally_unseen_word_prob
                      / factory.total_num_unseen_word_types)
            if (debug("bigram"))
              errprint("Word %s, never seen at all, wordprob = %s",
                       unmemoize_word(word), wordprob)
            wordprob
          }
          case Some(owprob) => {
            val wordprob = unseen_mass * owprob / overall_unseen_mass
            //if (wordprob <= 0)
            //  warning("Bad values; unseen_mass = %s, overall_word_probs[word] = %s, overall_unseen_mass = %s",
            //    unseen_mass, factory.overall_word_probs[word],
            //    factory.overall_unseen_mass)
            if (debug("bigram"))
              errprint("Word %s, seen but not in document, wordprob = %s",
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
        if (debug("bigram"))
          errprint("Word %s, seen in document, wordprob = %s",
                   unmemoize_word(word), wordprob)
        wordprob
      }
    }
    retval
  }
}

