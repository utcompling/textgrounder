///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2011 Ben Wing, Thomas Darr, Andy Luong, Erik Skiles, The University of Texas at Austin
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

import math._
import collection.mutable
import com.codahale.trove.{mutable => trovescala}


/**
 * Bigram word distribution with a table listing counts for each word,
 * initialized from the given key/value pairs.
 *
 * @param key Array holding keys, possibly over-sized, so that the internal
 *   arrays from DynamicArray objects can be used
 * @param values Array holding values corresponding to each key, possibly
 *   oversize
 * @param num_words Number of actual key/value pairs to be stored 
 *   statistics.
 */

abstract class BigramWordDist(
  keys: Array[Word],
  values: Array[Int],
  num_words: Int,
  bigramKeys: Array[Word],
  bigramValues: Array[Int],
  num_bigrams: Int
) extends WordDist {

  val counts = create_word_int_map()
  for (i <- 0 until num_words)
    counts(keys(i)) = values(i)
  var num_word_tokens = counts.values.sum
  
  def num_word_types = counts.size

  def innerToString: String

  val bicounts = create_word_int_map()
  for (i <- 0 until num_bigrams)
    bicounts(bigramKeys(i)) = bigramValues(i)
  var num_bigram_tokens = bicounts.values.sum

  def num_bigram_types = bicounts.size

  def add_document(words: Traversable[String], ignore_case: Boolean=true,
      stopwords: Set[String]=Set[String]()) {
    assert(!finished)
    var previous = "<START>";
    counts(memoize_word(previous)) += 1
    for {word <- words
         val wlower = if (ignore_case) word.toLowerCase() else word
         if !stopwords(wlower) } {
      counts(memoize_word(wlower)) += 1
      num_word_tokens += 1
      bicounts(memoize_word(previous + "_" + wlower)) += 1
      previous = wlower
    }
  }

  def add_word_distribution(xworddist: WordDist) {
    assert(!finished)
    val worddist = xworddist.asInstanceOf[BigramWordDist]
    for ((word, count) <- worddist.counts)
      counts(word) += count
    for ((bigram, count) <- worddist.bicounts)
      bicounts(bigram) += count
    num_word_tokens += worddist.num_word_tokens
    num_bigram_tokens += worddist.num_bigram_tokens
  }

  def finish_before_global(minimum_word_count: Int = 0) {
    // make sure counts not null (eg article in coords file but not counts file)
    if (counts == null || bicounts == null || finished) return

    // If 'minimum_word_count' was given, then eliminate words whose count
    // is too small.
    if (minimum_word_count > 1) {
      for ((word, count) <- counts if count < minimum_word_count) {
        num_word_tokens -= count
        counts -= word
      }
      for ((bigram, count) <- bicounts if count < minimum_word_count) {
        num_bigram_tokens -= count
        bicounts -= bigram
      }
    }
  }
    /**
     * This is a basic unigram implementation of the computation of the
     * KL-divergence between this distribution and another distribution.
     * Useful for checking against other, faster implementations.
     * 
     * Computing the KL divergence is a bit tricky, especially in the
     * presence of smoothing, which assigns probabilities even to words not
     * seen in either distribution.  We have to take into account:
     * 
     * 1. Words in this distribution (may or may not be in the other).
     * 2. Words in the other distribution that are not in this one.
     * 3. Words in neither distribution but seen globally.
     * 4. Words never seen at all.
     * 
     * The computation of steps 3 and 4 depends heavily on the particular
     * smoothing algorithm; in the absence of smoothing, these steps
     * contribute nothing to the overall KL-divergence.
     * 
     * @param xother The other distribution to compute against.
     * @param partial If true, only do step 1 above.
     * @param return_contributing_words If true, return a map listing
     *   the words in both distributions and the amount of total
     *   KL-divergence they compute, useful for debugging.
     *   
     * @returns A tuple of (divergence, word_contribs) where the first
     *   value is the actual KL-divergence and the second is the map
     *   of word contributions as described above; will be null if
     *   not requested.
     */
  def slow_kl_divergence_debug(xother: WordDist, partial: Boolean=false,
      return_contributing_words: Boolean=false) = {
    val other = xother.asInstanceOf[UnigramWordDist]
    assert(finished)
    assert(other.finished)
    var kldiv = 0.0
    val contribs =
      if (return_contributing_words) mutable.Map[Word, Double]() else null
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
      // Step 2.
      for (word <- other.counts.keys if !(counts contains word)) {
        val p = lookup_word(word)
        val q = other.lookup_word(word)
        kldiv += p*(log(p) - log(q))
        if (return_contributing_words)
          contribs(word) = p*(log(p) - log(q))
      }

      val retval = kldiv + kl_divergence_34(other)
      (retval, contribs)
    }
  }

  /**
   * Steps 3 and 4 of KL-divergence computation.
   * @seealso #slow_kl_divergence_debug
   */
  def kl_divergence_34(other: UnigramWordDist): Double
  
  def get_nbayes_logprob(xworddist: WordDist) = {
    val worddist = xworddist.asInstanceOf[UnigramWordDist]
    var logprob = 0.0
    for ((word, count) <- worddist.counts) {
      val value = lookup_word(word)
      if (value <= 0) {
        // FIXME: Need to figure out why this happens (perhaps the word was
        // never seen anywhere in the training data? But I thought we have
        // a case to handle that) and what to do instead.
        errprint("Warning! For word %s, prob %s out of range", word, value)
      } else
        logprob += log(value)
    }
    // FIXME: Also use baseline (prior probability)
    logprob
  }

  def find_most_common_word(pred: String => Boolean) = {
    val filtered =
      (for ((word, count) <- counts if pred(unmemoize_word(word)))
        yield (word, count)).toSeq
    if (filtered.length == 0) None
    else {
      val (maxword, maxcount) = filtered maxBy (_._2)
      Some(maxword)
    }
  }
}
