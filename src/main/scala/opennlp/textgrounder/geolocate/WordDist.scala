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
import Debug._
import WordDist.memoizer._
import WordDist.SmoothedWordDist

import math._
import collection.mutable
import com.codahale.trove.{mutable => trovescala}

// val use_sorted_list = false

//////////////////////////////////////////////////////////////////////////////
//                             Word distributions                           //
//////////////////////////////////////////////////////////////////////////////

object IntStringMemoizer {
  type Word = Int
  val invalid_word: Word = 0

  protected var next_word_count: Word = 1

  // For replacing strings with ints.  This should save space on 64-bit
  // machines (string pointers are 8 bytes, ints are 4 bytes) and might
  // also speed lookup.
  protected val word_id_map = mutable.Map[String,Word]()

  // Map in the opposite direction.
  protected val id_word_map = mutable.Map[Word,String]()

  def memoize_word(word: String) = {
    val index = word_id_map.getOrElse(word, 0)
    if (index != 0) index
    else {
      val newind = next_word_count
      next_word_count += 1
      word_id_map(word) = newind
      id_word_map(index) = word
      newind
    }
  }

  def unmemoize_word(word: Word) = id_word_map(word)

  def create_word_int_map() = trovescala.IntIntMap()
  type WordIntMap = trovescala.IntIntMap
  def create_word_double_map() = trovescala.IntDoubleMap()
  type WordDoubleMap = trovescala.IntDoubleMap
}

object IdentityMemoizer {
  type Word = String
  val invalid_word: Word = null
  def memoize_word(word: String): Word = word
  def unmemoize_word(word: Word): String = word

  def create_word_int_map() = intmap[Word]()
  def create_word_double_map() = doublemap[Word]()
}

object TrivialIntMemoizer {
  type Word = Int
  val invalid_word: Word = 0
  def memoize_word(word: String): Word = 1
  def unmemoize_word(word: Word): String = "foo"

  def create_word_int_map() = IntStringMemoizer.create_word_int_map()
  def create_word_double_map() = IntStringMemoizer.create_word_double_map()
}

object WordDist {
  val memoizer = IntStringMemoizer
  type SmoothedWordDist = PseudoGoodTuringSmoothedWordDist
  val SmoothedWordDist = PseudoGoodTuringSmoothedWordDist

  // Total number of word types seen (size of vocabulary)
  var total_num_word_types = 0

  // Total number of word tokens seen
  var total_num_word_tokens = 0

  def apply(keys: Array[Word], values: Array[Int], num_words: Int,
            note_globally: Boolean) =
    new SmoothedWordDist(keys, values, num_words, note_globally)

  def apply():SmoothedWordDist =
    apply(Array[Word](), Array[Int](), 0, note_globally=false)
}

abstract class WordDist {
  /** Number of word tokens seen in the distribution. */
  var num_word_tokens: Int

  /**
   * Number of word types seen in the distribution
   * (i.e. number of different vocabulary items seen).
   */
  def num_word_types: Int
  
  /**
   * Whether we have finished computing the distribution, and therefore can
   * reliably do probability lookups.
   */
  var finished = false

  /**
   * Incorporate a document into the distribution.
   */
  def add_document(words: Traversable[String], ignore_case: Boolean=true,
      stopwords: Set[String]=Set[String]())

  /**
   * Incorporate the given distribution into our distribution.
   */
  def add_word_distribution(worddist: WordDist)

  /**
   * Finish computation of distribution.  This does any additional changes
   * needed when no more words or distributions will be added to this one,
   * but which do not rely on other distributions also being finished.
   * @seealso #finish_after_global()
   * 
   * @param minimum_word_count If greater than zero, eliminate words seen
   * less than this number of times.
   */
  def finish_before_global(minimum_word_count: Int = 0)

  /**
   * Completely finish computation of the word distribution.  This is called
   * after finish_global_distribution() on the factory method, and can be
   * used to compute values for the distribution that depend on global values
   * computed from all word distributions.
   */
  def finish_after_global()

  def finish(minimum_word_count: Int = 0) {
    finish_before_global(minimum_word_count)
    finish_after_global()
  }

  /**
   * Check fast and slow versions against each other.
   */
  def test_kl_divergence(other: WordDist, partial: Boolean=false) = {
    assert(finished)
    assert(other.finished)
    val fast_kldiv = fast_kl_divergence(other, partial)
    val slow_kldiv = slow_kl_divergence(other, partial)
    if (abs(fast_kldiv - slow_kldiv) > 1e-8) {
      errprint("Fast KL-div=%s but slow KL-div=%s", fast_kldiv, slow_kldiv)
      assert(fast_kldiv == slow_kldiv)
    }
    fast_kldiv
  }

  /**
   * Do a basic implementation of the computation of the KL-divergence
   * between this distribution and another distribution, including possible
   * debug information.  Useful for checking against other, faster
   * implementations, e.g. `fast_kl_divergence`.
   * 
   * @param other The other distribution to compute against.
   * @param partial If true, only compute the contribution involving words
   *   that exist in our distribution; otherwise we also have to take into
   *   account words in the other distribution even if we haven't seen them,
   *   and often also (esp. in the presence of smoothing) the contribution
   *   of all other words in the vocabulary.
   * @param return_contributing_words If true, return a map listing
   *   the words in both distributions (or, for a partial KL-divergence,
   *   the words in our distribution) and the amount of total KL-divergence
   *   they compute, useful for debugging.
   *   
   * @returns A tuple of (divergence, word_contribs) where the first
   *   value is the actual KL-divergence and the second is the map
   *   of word contributions as described above; will be null if
   *   not requested.
   */
  def slow_kl_divergence_debug(other: WordDist, partial: Boolean=false,
      return_contributing_words: Boolean=false):
    (Double, collection.Map[Word, Double])

  /**
   * Compute the KL-divergence using the "slow" algorithm of
   * `slow_kl_divergence_debug`, but without requesting or returning debug
   * info.
   */
  def slow_kl_divergence(other: WordDist, partial: Boolean=false) = {
    val (kldiv, contribs) = slow_kl_divergence_debug(other, partial, false)
    kldiv
  }

  /**
   * A fast, optimized implementation of KL-divergence.  See the discussion in
   * `slow_kl_divergence_debug`.
   */
  def fast_kl_divergence(other: WordDist, partial: Boolean=false): Double

  /**
   * Implementation of the cosine similarity between this and another
   * distribution, using unsmoothed probabilities.
   * 
   * @partial Same as in `slow_kl_divergence_debug`.
   */
  def fast_cosine_similarity(other: WordDist, partial: Boolean=false): Double

  /**
   * Implementation of the cosine similarity between this and another
   * distribution, using smoothed probabilities.
   * 
   * @partial Same as in `slow_kl_divergence_debug`.
   */
  def fast_smoothed_cosine_similarity(other: WordDist, partial: Boolean=false): Double

  /**
   * Compute the symmetric KL-divergence between two distributions by averaging
   * the respective one-way KL-divergences in each direction.
   * 
   * @partial Same as in `slow_kl_divergence_debug`.
   */
  def symmetric_kldiv(other: WordDist, partial: Boolean=false) = {
    0.5*this.fast_kl_divergence(other, partial) +
    0.5*this.fast_kl_divergence(other, partial)
  }

  /**
   * For a document described by its distribution 'worddist', return the
   * log probability log p(worddist|cell) using a Naive Bayes algorithm.
   *
   * @param worddist Distribution of document.
   */
  def get_nbayes_logprob(worddist: WordDist): Double

  /**
   * Return the probabilitiy of a given word in the distribution.
   * FIXME: Should be moved into either UnigramWordDist or a new
   * UnigramLikeWordDist, since for N-grams we really want the whole N-gram,
   * and for some language models this type of lookup makes no sense at all. 
   */
  def lookup_word(word: Word): Double
  
  /**
   * Look for the most common word matching a given predicate.
   * @param pred Predicate, passed the raw (unmemoized) form of a word.
   *   Should return true if a word matches.
   * @returns Most common word matching the predicate (wrapped with
   *   Some()), or None if no match.
   * 
   * FIXME: Probably should be moved similar to `lookup_word`.
   */
  def find_most_common_word(pred: String => Boolean): Option[Word] 
}

/**
 * Unigram word distribution with a table listing counts for each word,
 * initialized from the given key/value pairs.
 *
 * @param key Array holding keys, possibly over-sized, so that the internal
 *   arrays from DynamicArray objects can be used
 * @param values Array holding values corresponding to each key, possibly
 *   oversize
 * @param num_words Number of actual key/value pairs to be stored 
 *   statistics.
 */

abstract class UnigramWordDist(
  keys: Array[Word],
  values: Array[Int],
  num_words: Int
) extends WordDist {
  /** A map (or possibly a "sorted list" of tuples, to save memory?) of
      (word, count) items, specifying the counts of all words seen
      at least once.
   */
  val counts = create_word_int_map()
  for (i <- 0 until num_words)
    counts(keys(i)) = values(i)
  var num_word_tokens = counts.values.sum
  
  def num_word_types = counts.size

  def innerToString: String

  override def toString = {
    val finished_str =
      if (!finished) ", unfinished" else ""
    val num_words_to_print = 15
    val need_dots = counts.size > num_words_to_print
    val items =
      for ((word, count) <- counts.view(0, num_words_to_print))
      yield "%s=%s" format (unmemoize_word(word), count) 
    val words = (items mkString " ") + (if (need_dots) " ..." else "")
    "WordDist(%d tokens%s%s, %s)" format (
        num_word_tokens, innerToString, finished_str, words)
  }

  def add_document(words: Traversable[String], ignore_case: Boolean=true,
      stopwords: Set[String]=Set[String]()) {
    assert(!finished)
    for {word <- words
         val wlower = if (ignore_case) word.toLowerCase() else word
         if !stopwords(wlower) } {
      counts(memoize_word(wlower)) += 1
      num_word_tokens += 1
    }
  }

  def add_word_distribution(xworddist: WordDist) {
    assert(!finished)
    val worddist = xworddist.asInstanceOf[UnigramWordDist]
    for ((word, count) <- worddist.counts)
      counts(word) += count
    num_word_tokens += worddist.num_word_tokens
  }

  def finish_before_global(minimum_word_count: Int = 0) {
    // make sure counts not null (eg article in coords file but not counts file)
    if (counts == null || finished) return

    // If 'minimum_word_count' was given, then eliminate words whose count
    // is too small.
    if (minimum_word_count > 1) {
      for ((word, count) <- counts if count < minimum_word_count) {
        num_word_tokens -= count
        counts -= word
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

