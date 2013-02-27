///////////////////////////////////////////////////////////////////////////////
//  UnigramWordDist.scala
//
//  Copyright (C) 2010, 2011, 2012 Ben Wing, The University of Texas at Austin
//  Copyright (C) 2012 Mike Speriosu, The University of Texas at Austin
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
package worddist

import math._
import collection.mutable
import scala.util.control.Breaks._

import java.io._

import util.collection.DynamicArray
import util.textdb
import util.io.{FileHandler, FileFormatException}
import util.print.{errprint, warning}

import gridlocate.GeoDoc
import util.debug._

import WordDist._

/**
 * An interface for storing and retrieving vocabulary items (e.g. words,
 * n-grams, etc.).
 *
 * @tparam Item Type of the items stored.
 */
class UnigramStorage extends ItemStorage[Word] {

  /**
   * A map (or possibly a "sorted list" of tuples, to save memory?) of
   * (word, count) items, specifying the counts of all words seen
   * at least once.  These are given as double because in some cases
   * they may store "partial" counts (in particular, when the K-d tree
   * code does interpolation on cells).  FIXME: This seems ugly, perhaps
   * there is a better way?
   */
  val counts = create_word_double_map
  var tokens_accurate = true
  var num_tokens_val = 0.0

  def add_item(item: Word, count: Double) {
    counts(item) += count
    num_tokens_val += count
  }

  def set_item(item: Word, count: Double) {
    counts(item) = count
    tokens_accurate = false
  }

  def remove_item(item: Word) {
    counts -= item
    tokens_accurate = false
  }

  // Declare these inline final to try to ensure that the code gets inlined.
  // Note that the code does get inlined in normal circumstances, but won't
  // currently if trait `ItemStorage` is specialized on Int (which doesn't
  // help fast_kl_divergence() in any case, based on disassembly of the byte
  // code).
  @inline final def contains(item: Word) = counts contains item

  @inline final def get_item(item: Word) = counts(item)

  def iter_items = counts.toIterable

  def iter_keys = counts.keys

  def num_tokens = {
    if (!tokens_accurate) {
      num_tokens_val = counts.values.sum
      tokens_accurate = true
    }
    num_tokens_val
  }

  @inline final def num_types = counts.size
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
  factory: UnigramWordDistFactory
) extends WordDist(factory) with FastSlowKLDivergence {
  type Item = Word
  val pmodel = new UnigramStorage()
  val model = pmodel

  def innerToString: String

  def toString(num_words_to_print: Int) = {
    val finished_str =
      if (!finished) ", unfinished" else ""
    val num_actual_words_to_print =
      if (num_words_to_print < 0) model.num_types
      else num_words_to_print
    val need_dots = model.num_types > num_actual_words_to_print
    val items =
      for ((word, count) <-
        model.iter_items.toSeq.sortWith(_._2 > _._2).
          view(0, num_actual_words_to_print))
      yield "%s=%s" format (memoizer.unmemoize(word), count) 
    val words = (items mkString " ") + (if (need_dots) " ..." else "")
    "UnigramWordDist(%d types, %s tokens%s%s, %s)" format (
        model.num_types, model.num_tokens, innerToString,
        finished_str, words)
  }

  override def debug_string = toString(-1)

  override def toString = toString(15)

  /**
   * This is a basic unigram implementation of the computation of the
   * KL-divergence between this distribution and another distribution,
   * including possible debug information.
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
   */
  def slow_kl_divergence_debug(xother: WordDist, partial: Boolean = false,
      return_contributing_words: Boolean = false) = {
    val other = xother.asInstanceOf[UnigramWordDist]
    var kldiv = 0.0
    val contribs =
      if (return_contributing_words) mutable.Map[String, Double]() else null
    // 1.
    for (word <- model.iter_keys) {
      val p = lookup_word(word)
      val q = other.lookup_word(word)
      if (q == 0.0)
        { } // This is OK, we just skip these words
      else if (p <= 0.0 || q <= 0.0)
        errprint("Warning: problematic values: p=%s, q=%s, word=%s", p, q, word)
      else {
        kldiv += p*(log(p) - log(q))
        if (return_contributing_words)
          contribs(memoizer.unmemoize(word)) = p*(log(p) - log(q))
      }
    }

    if (partial)
      (kldiv, contribs)
    else {
      // Step 2.
      for (word <- other.model.iter_keys if !(model contains word)) {
        val p = lookup_word(word)
        val q = other.lookup_word(word)
        kldiv += p*(log(p) - log(q))
        if (return_contributing_words)
          contribs(memoizer.unmemoize(word)) = p*(log(p) - log(q))
      }

      val retval = kldiv + kl_divergence_34(other)
      (retval, contribs)
    }
  }

  /**
   * Steps 3 and 4 of KL-divergence computation.
   * @see #slow_kl_divergence_debug
   */
  def kl_divergence_34(other: UnigramWordDist): Double
  
  def get_nbayes_logprob(xworddist: WordDist) = {
    val worddist = xworddist.asInstanceOf[UnigramWordDist]
    var logprob = 0.0
    for ((word, count) <- worddist.model.iter_items) {
      val value = lookup_word(word)
      assert(value >= 0)
      // The probability returned will be 0 for words never seen in the
      // training data at all, i.e. we don't even have any global values to
      // back off to. General practice is to ignore such words.
      if (value > 0)
        logprob += log(value)
    }
    // FIXME: Also use baseline (prior probability)
    logprob
  }

  /**
   * Return the probabilitiy of a given word in the distribution.
   */
  protected def imp_lookup_word(word: Word): Double

  def lookup_word(word: Word): Double = {
    assert(finished)
    if (empty)
      throw new IllegalStateException("Attempt to lookup word %s in empty distribution %s"
        format (memoizer.unmemoize(word), this))
    val wordprob = imp_lookup_word(word)
    // Write this way because if negated as an attempt to catch bad values,
    // it won't catch NaN, which fails all comparisons.
    if (wordprob >= 0 && wordprob <= 1)
      ()
    else {
      errprint("Out-of-bounds prob %s for word %s",
        wordprob, memoizer.unmemoize(word))
      assert(false)
    }
    wordprob
  }
  
  /**
   * Look for the most common word matching a given predicate.
   * @param pred Predicate, passed the raw (unmemoized) form of a word.
   *   Should return true if a word matches.
   * @return Most common word matching the predicate (wrapped with
   *   Some()), or None if no match.
   */
  def find_most_common_word(pred: String => Boolean): Option[Word] = {
    val filtered =
      (for ((word, count) <- model.iter_items if pred(memoizer.unmemoize(word)))
        yield (word, count)).toSeq
    if (filtered.length == 0) None
    else {
      val (maxword, maxcount) = filtered maxBy (_._2)
      Some(maxword)
    }
  }
}

class DefaultUnigramWordDistConstructor(
  factory: WordDistFactory,
  ignore_case: Boolean,
  stopwords: Set[String],
  whitelist: Set[String],
  minimum_word_count: Int = 1
) extends WordDistConstructor(factory: WordDistFactory) {
  /**
   * Initial size of the internal DynamicArray objects; an optimization.
   */
  protected val initial_dynarr_size = 1000
  /**
   * Internal DynamicArray holding the keys (canonicalized words).
   */
  protected val keys_dynarr =
    new DynamicArray[String](initial_alloc = initial_dynarr_size)
  /**
   * Internal DynamicArray holding the values (word counts).
   */
  protected val values_dynarr =
    new DynamicArray[Int](initial_alloc = initial_dynarr_size)
  /**
   * Set of the raw, uncanonicalized words seen, to check that an
   * uncanonicalized word isn't seen twice. (Canonicalized words may very
   * well occur multiple times.)
   */
  protected val raw_keys_set = mutable.Set[String]()

  protected def parse_counts(countstr: String) {
    keys_dynarr.clear()
    values_dynarr.clear()
    raw_keys_set.clear()
    for ((word, count) <- textdb.decode_count_map(countstr)) {
      /* FIXME: Is this necessary? */
      if (raw_keys_set contains word)
        throw FileFormatException(
          "Word %s seen twice in same counts list: %s" format (word, countstr)
        )
      raw_keys_set += word
      keys_dynarr += word
      values_dynarr += count
    }
  }

  // Returns true if the word was counted, false if it was ignored due to stoplisting
  // and/or whitelisting
  protected def add_word_with_count(model: UnigramStorage, word: String,
      count: Int): Boolean = {
    val lword = maybe_lowercase(word)
    if (!stopwords.contains(lword) &&
        (whitelist.size == 0 || whitelist.contains(lword))) {
      model.add_item(memoizer.memoize(lword), count)
      true
    }
    else
      false
  }

  protected def imp_add_document(dist: WordDist, words: Iterable[String]) {
    val model = dist.asInstanceOf[UnigramWordDist].model
    for (word <- words)
      add_word_with_count(model, word, 1)
  }

  protected def imp_add_word_distribution(dist: WordDist, other: WordDist,
      partial: Double) {
    // FIXME: Implement partial!
    val model = dist.asInstanceOf[UnigramWordDist].model
    val othermodel = other.asInstanceOf[UnigramWordDist].model
    for ((word, count) <- othermodel.iter_items)
      model.add_item(word, count)
  }

  /**
   * Actual implementation of `add_keys_values` by subclasses.
   * External callers should use `add_keys_values`.
   */
  protected def imp_add_keys_values(dist: WordDist, keys: Array[String],
      values: Array[Int], num_words: Int) {
    val model = dist.asInstanceOf[UnigramWordDist].model
    var addedTypes = 0
    var addedTokens = 0
    var totalTokens = 0
    for (i <- 0 until num_words) {
      if(add_word_with_count(model, keys(i), values(i))) {
        addedTypes += 1
        addedTokens += values(i)
      }
      totalTokens += values(i)
    }
    // Way too much output to keep enabled
    //errprint("Fraction of word types kept:"+(addedTypes.toDouble/num_words))
    //errprint("Fraction of word tokens kept:"+(addedTokens.toDouble/totalTokens))
  } 

  /**
   * Incorporate a set of (key, value) pairs into the distribution.
   * The number of pairs to add should be taken from `num_words`, not from
   * the actual length of the arrays passed in.  The code should be able
   * to handle the possibility that the same word appears multiple times,
   * adding up the counts for each appearance of the word.
   */
  protected def add_keys_values(dist: WordDist,
      keys: Array[String], values: Array[Int], num_words: Int) {
    assert(!dist.finished)
    assert(!dist.finished_before_global)
    assert(keys.length >= num_words)
    assert(values.length >= num_words)
    imp_add_keys_values(dist, keys, values, num_words)
  }

  protected def imp_finish_before_global(gendist: WordDist) {
    val dist = gendist.asInstanceOf[UnigramWordDist]
    val model = dist.model
    val oov = memoizer.memoize("-OOV-")

    // If 'minimum_word_count' was given, then eliminate words whose count
    // is too small.
    if (minimum_word_count > 1) {
      for ((word, count) <- model.iter_items if count < minimum_word_count) {
        model.remove_item(word)
        model.add_item(oov, count)
      }
    }
  }

  def maybe_lowercase(word: String) =
    if (ignore_case) word.toLowerCase else word

  def create_distribution(countstr: String) = {
    parse_counts(countstr)
    // Now set the distribution on the document.
    val dist = factory.create_word_dist
    add_keys_values(dist, keys_dynarr.array, values_dynarr.array,
      keys_dynarr.length)
    dist
  }
}

/**
 * General factory for UnigramWordDist distributions.
 */ 
trait UnigramWordDistFactory extends WordDistFactory { }
