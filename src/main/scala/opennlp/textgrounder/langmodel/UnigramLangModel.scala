///////////////////////////////////////////////////////////////////////////////
//  UnigramLangModel.scala
//
//  Copyright (C) 2010-2013 Ben Wing, The University of Texas at Austin
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

  // NOTE NOTE NOTE! Possible SCALABUG!! The toSeq needs to be added for some]
  // reason; if not, the accuracy of computations that loop over the keys drops
  // dramatically. (On the order of 10-15% when computing smoothing in
  // DiscountedUnigramLangModelFactory.imp_finish_after_global, a factor of
  // 2 when computing a model probability in `model_logprob`. I have no idea
  // why; I suspect a Scala bug. (SCALABUG) This bug was present back in Scala
  // 2.8 and 2.9 as well.
  def iter_keys = counts.keys.toSeq

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

  /**
   * Sum of all word weights for all word tokens in the language model.
   * This is used to normalize the computation in `model_logprob`
   * so that it computes a weighted average. This isn't strictly
   * necessary for plain ranking, but ensures that the scores are
   * more comparable across different documents when training a reranker.
   */
  var total_word_weight = 1.0

  override def imp_finish_before_global() {
    super.imp_finish_before_global()
    if (factory.word_weights.size > 0) {
      // We multiply each weight by the word count because logically we
      // are weighting each word.
      total_word_weight =
        (for ((word, count) <- model.iter_items) yield
          count * factory.word_weights.getOrElse(word,
            factory.missing_word_weight)).sum
    }
  }

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
  
  /**
   * Return the log-probability of a word, taking into account the
   * possibility that the probability is zero (in which case the
   * log-probability is defined to be zero also).
   *
   * @see #lookup_word
   */
  @inline final def word_logprob(word: Item) = {
    val value = lookup_word(word)
    assert(value >= 0)
    // The probability returned will be 0 for words never seen in the
    // training data at all, i.e. we don't even have any global values to
    // back off to. General practice is to ignore such words.
    if (value > 0)
      log(value)
    else 0.0
  }

  /**
   * Return `log p(thismodel|othermodel)`. This implements a log-linear
   * model, i.e. it is similar to a standard Naive-Bayes model that
   * assumes independence of the words in `thismodel` but allows for
   * the different words to be weighted differently, as specified in
   * `factory.word_weights`. (Essentially, this allows for standard
   * discriminative training using a log-linear model, e.g. maxent /
   * logistic regression.)
   *
   * Note that, for the purposes of determining a ranking of a document
   * over e.g. cells, which essentially involves computing
   * `p(othermodel|thismodel)`, the scores returned by this function can
   * only be compared with other scores returned for the same value of
   * `thismodel`; e.g. the scores can be used for ranking different cells
   * with respect to a given document but cannot be used for comparing
   * different documents. (FIXME: This can cause problems, e.g., in
   * using the returned score as a feature in a reranking model,
   * because that will compute a single weight to use in weighting
   * the base ranking score for all test documents, relative to
   * other features. In this case the problem is the additive
   * factor `-log p(doc)` that is ignored when comparing different
   * cells relative to a given document. Possibly it would be
   * sufficient to add a factor to all scores so that the top-
   * ranked cell always has a score of 0 (or any other fixed value).)
   */
  def model_logprob(xlangmodel: LangModel) = {
    val langmodel = xlangmodel.asInstanceOf[UnigramLangModel]
    val weights = factory.word_weights
    if (weights.size == 0)
      langmodel.model.iter_keys.map(word_logprob(_)).sum
    else {
      val sum =
        langmodel.model.iter_keys.map { word =>
          val weight = weights.getOrElse(word, factory.missing_word_weight)
          weight * word_logprob(word)
        }.sum
      sum / total_word_weight
    }
  }

  def get_most_contributing_grams(xlangmodel: LangModel,
      xrelative_to: Iterable[LangModel] = Iterable()) = {
    val langmodel = xlangmodel.asInstanceOf[UnigramLangModel]
    val relative_to = xrelative_to.map(_.asInstanceOf[UnigramLangModel])
    val words = langmodel.model.iter_keys
    val weights =
      if (relative_to.isEmpty)
        words.map { word => (word, word_logprob(word)) }
      else if (relative_to.size == 1) {
        val othermodel = relative_to.head
        words.map { word =>
          (word, word_logprob(word) - othermodel.word_logprob(word))
        }
      } else {
        words.map { word =>
          val factor = word_logprob(word)
          val relcontribs =
            relative_to.map { factor - _.word_logprob(word) }
          (word, relcontribs maxBy { _.abs})
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
  
  def mle_word_prob(word: Word): Double = {
    assert(finished)
    if (empty)
      throw new IllegalStateException("Attempt to lookup word %s in empty lang model %s"
        format (item_to_string(word), this))
    val wordcount = if (model contains word) model.get_item(word) else 0.0
    wordcount.toDouble/model.num_tokens
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

  def finish_before_global(lm: LangModel) {
    val model = lm.asInstanceOf[UnigramLangModel].model
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

  override def finish_before_global(xlm: LangModel) {
    super.finish_before_global(xlm)

    val lm = xlm.asInstanceOf[UnigramLangModel]
    val model = lm.model
    val oov = memoizer.memoize("-OOV-")

    // Filter the words we don't care about, to save memory and time.
    for ((word, count) <- model.iter_items
         if !(filter_words contains lm.item_to_string(word))) {
      model.remove_item(word)
      model.add_item(oov, count)
    }
  }
}

/**
 * General factory for UnigramLangModel language models.
 *
 * @param word_weights Weights to assign to each word, in the case where
 *   unequal weighting is desired. If empty, do equal weighting.
 * @param missing_word_weight Weight to assign to words not seen in
 *   `word_weight`.
 */ 
abstract class UnigramLangModelFactory(
    val word_weights: collection.Map[Word, Double],
    val missing_word_weight: Double
) extends LangModelFactory { }
