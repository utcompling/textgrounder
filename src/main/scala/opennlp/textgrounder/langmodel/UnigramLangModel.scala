///////////////////////////////////////////////////////////////////////////////
//  UnigramLangModel.scala
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
package langmodel

import math._
import collection.mutable
import scala.util.control.Breaks._

import java.io._

import util.collection.DynamicArray
import util.textdb
import util.io.{FileHandler, FileFormatException}
import util.print.{errprint, warning}

import util.debug._

import LangModel._

object Unigram {
  def check_unigram_lang_model(lang_model: LangModel) = {
    lang_model match {
      case x: UnigramLangModel => x
      case _ => throw new IllegalArgumentException("You must use a unigram language model for these parameters")
    }
  }
}

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

  def add_item(item: Word, count: WordCount) {
    counts(item) += count
    num_tokens_val += count
  }

  def set_item(item: Word, count: WordCount) {
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
 * Unigram language model with a table listing counts for each word,
 * initialized from the given key/value pairs.
 *
 * @param key Array holding keys, possibly over-sized, so that the internal
 *   arrays from DynamicArray objects can be used
 * @param values Array holding values corresponding to each key, possibly
 *   oversize
 * @param num_words Number of actual key/value pairs to be stored 
 *   statistics.
 */

abstract class UnigramLangModel(
  factory: UnigramLangModelFactory
) extends LangModel(factory) with FastSlowKLDivergence {
  type Item = Word
  val pmodel = new UnigramStorage()
  val model = pmodel

  def item_to_string(item: Item) = memoizer.unmemoize(item)

  /**
   * This is a basic unigram implementation of the computation of the
   * KL-divergence between this lang model and another lang model,
   * including possible debug information.
   * 
   * Computing the KL divergence is a bit tricky, especially in the
   * presence of smoothing, which assigns probabilities even to words not
   * seen in either lang model.  We have to take into account:
   * 
   * 1. Words in this lang model (may or may not be in the other).
   * 2. Words in the other lang model that are not in this one.
   * 3. Words in neither lang model but seen in the global back-off stats.
   * 4. Words never seen at all.
   * 
   * The computation of steps 3 and 4 depends heavily on the particular
   * smoothing algorithm; in the absence of smoothing, these steps
   * contribute nothing to the overall KL-divergence.
   *
   */
  def slow_kl_divergence_debug(xother: LangModel, partial: Boolean = false,
      return_contributing_words: Boolean = false) = {
    val other = xother.asInstanceOf[UnigramLangModel]
    var kldiv = 0.0
    val contribs =
      if (return_contributing_words) mutable.Map[String, WordCount]() else null
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
          contribs(item_to_string(word)) = p*(log(p) - log(q))
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
          contribs(item_to_string(word)) = p*(log(p) - log(q))
      }

      val retval = kldiv + kl_divergence_34(other)
      (retval, contribs)
    }
  }

  /**
   * Steps 3 and 4 of KL-divergence computation.
   * @see #slow_kl_divergence_debug
   */
  def kl_divergence_34(other: UnigramLangModel): Double
  
  def get_nbayes_factor(word: Item, count: WordCount) = {
    val value = lookup_word(word)
    assert(value >= 0)
    // The probability returned will be 0 for words never seen in the
    // training data at all, i.e. we don't even have any global values to
    // back off to. General practice is to ignore such words.
    if (value > 0)
      count * log(value)
    else 0.0
  }

  def get_nbayes_logprob(xlangmodel: LangModel) = {
    val langmodel = xlangmodel.asInstanceOf[UnigramLangModel]
    // FIXME: Also use baseline (prior probability)
    langmodel.model.iter_items.map {
      case (word, count) => get_nbayes_factor(word, count)
    }.sum
  }

  def get_most_contributing_grams(xlangmodel: LangModel,
      xrelative_to: Iterable[LangModel] = Iterable()) = {
    val langmodel = xlangmodel.asInstanceOf[UnigramLangModel]
    val relative_to = xrelative_to.map(_.asInstanceOf[UnigramLangModel])
    val itemcounts = langmodel.model.iter_items
    val weights =
      if (relative_to.isEmpty)
        itemcounts.map {
          case (word, count) => (word, get_nbayes_factor(word, count))
        }
      else if (relative_to.size == 1) {
        val othermodel = relative_to.head
        itemcounts.map {
          case (word, count) => (word, get_nbayes_factor(word, count) -
            othermodel.get_nbayes_factor(word, count))
        }
      } else {
        itemcounts.map {
          case (word, count) => {
            val factor = get_nbayes_factor(word, count)
            val relcontribs =
              relative_to.map { factor - _.get_nbayes_factor(word, count) }
            (word, relcontribs maxBy { _.abs})
          }
        }
      }
    weights.toSeq.sortWith { _._2.abs > _._2.abs }
  }

  /**
   * Return the probability of a given word in the lang model.
   */
  protected def imp_lookup_word(word: Word): Double

  def lookup_word(word: Word): Double = {
    assert(finished)
    if (empty)
      throw new IllegalStateException("Attempt to lookup word %s in empty lang model %s"
        format (item_to_string(word), this))
    val wordprob = imp_lookup_word(word)
    // Write this way because if negated as an attempt to catch bad values,
    // it won't catch NaN, which fails all comparisons.
    if (wordprob >= 0 && wordprob <= 1)
      ()
    else {
      errprint("Out-of-bounds prob %s for word %s",
        wordprob, item_to_string(word))
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
      (for ((word, count) <- model.iter_items if pred(item_to_string(word)))
        yield (word, count)).toSeq
    if (filtered.length == 0) None
    else {
      val (maxword, maxcount) = filtered maxBy (_._2)
      Some(maxword)
    }
  }
}

class DefaultUnigramLangModelBuilder(
  factory: LangModelFactory,
  ignore_case: Boolean,
  stopwords: Set[String],
  whitelist: Set[String],
  minimum_word_count: Int = 1
) extends LangModelBuilder(factory: LangModelFactory) {
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

  // Returns true if the word was counted, false if it was ignored due to
  // stoplisting and/or whitelisting
  protected def add_word_with_count(model: UnigramStorage, word: String,
      count: WordCount): Boolean = {
    val lword = maybe_lowercase(word)
    if (!stopwords.contains(lword) &&
        (whitelist.size == 0 || whitelist.contains(lword))) {
      model.add_item(memoizer.memoize(lword), count)
      true
    }
    else
      false
  }

  protected def imp_add_document(lm: LangModel, words: Iterable[String]) {
    val model = lm.asInstanceOf[UnigramLangModel].model
    for (word <- words)
      add_word_with_count(model, word, 1)
  }

  protected def imp_add_language_model(lm: LangModel, other: LangModel,
      partial: WordCount) {
    // FIXME: Implement partial!
    val model = lm.asInstanceOf[UnigramLangModel].model
    val othermodel = other.asInstanceOf[UnigramLangModel].model
    for ((word, count) <- othermodel.iter_items)
      model.add_item(word, count)
  }

  /**
   * Actual implementation of `add_keys_values` by subclasses.
   * External callers should use `add_keys_values`.
   */
  protected def imp_add_keys_values(lm: LangModel, keys: Array[String],
      values: Array[Int], num_words: Int) {
    val model = lm.asInstanceOf[UnigramLangModel].model
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
   * Incorporate a set of (key, value) pairs into the language model.
   * The number of pairs to add should be taken from `num_words`, not from
   * the actual length of the arrays passed in.  The code should be able
   * to handle the possibility that the same word appears multiple times,
   * adding up the counts for each appearance of the word.
   */
  protected def add_keys_values(lm: LangModel,
      keys: Array[String], values: Array[Int], num_words: Int) {
    assert(!lm.finished)
    assert(!lm.finished_before_global)
    assert(keys.length >= num_words)
    assert(values.length >= num_words)
    imp_add_keys_values(lm, keys, values, num_words)
  }

  protected def imp_finish_before_global(gendist: LangModel) {
    val lm = gendist.asInstanceOf[UnigramLangModel]
    val model = lm.model
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

  def create_lang_model(countstr: String) = {
    parse_counts(countstr)
    // Now set the language model on the document.
    val lm = factory.create_lang_model
    add_keys_values(lm, keys_dynarr.array, values_dynarr.array,
      keys_dynarr.length)
    lm
  }
}

/* A builder that filters the language models to contain only the words we
   care about, to save memory and time. */
class FilterUnigramLangModelBuilder(
    factory: LangModelFactory,
    filter_words: Seq[String],
    ignore_case: Boolean,
    stopwords: Set[String],
    whitelist: Set[String],
    minimum_word_count: Int = 1
  ) extends DefaultUnigramLangModelBuilder(
    factory, ignore_case, stopwords, whitelist, minimum_word_count
  ) {

  override def finish_before_global(xlang_model: LangModel) {
    super.finish_before_global(xlang_model)

    val lang_model = xlang_model.asInstanceOf[UnigramLangModel]
    val model = lang_model.model
    val oov = memoizer.memoize("-OOV-")

    // Filter the words we don't care about, to save memory and time.
    for ((word, count) <- model.iter_items
         if !(filter_words contains lang_model.item_to_string(word))) {
      model.remove_item(word)
      model.add_item(oov, count)
    }
  }
}

/**
 * General factory for UnigramLangModel language models.
 */ 
trait UnigramLangModelFactory extends LangModelFactory { }