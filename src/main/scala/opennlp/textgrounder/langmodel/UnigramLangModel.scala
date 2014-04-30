///////////////////////////////////////////////////////////////////////////////
//  UnigramLangModel.scala
//
//  Copyright (C) 2010-2014 Ben Wing, The University of Texas at Austin
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
import util.error.warning
import util.print.errprint

import util.debug._

/**
 * Object describing how "words" are memoized. Memoization means mapping
 * words (which are strings) into integers, for faster and less
 * space-intensive operations on them).
 */
object Unigram extends StringGramAsIntMemoizer {
  def check_unigram_lang_model(lang_model: LangModel) = {
    lang_model match {
      case x: UnigramLangModel => x
      case _ => throw new IllegalArgumentException("You must use a unigram language model for these parameters")
    }
  }
}

/**
 * An interface for storing and retrieving vocabulary grams (e.g. words,
 * n-grams, etc.).
 */
class UnigramStorage extends GramStorage {

  /**
   * A map (or possibly a "sorted list" of tuples, to save memory?) of
   * (word, count) items, specifying the counts of all words seen
   * at least once.  These are given as double because in some cases
   * they may store "partial" or weighted counts. For example, when the
   * K-d tree code does interpolation; when --word-weights is given;
   * when --tf-idf is given. FIXME: This seems ugly in some ways; should
   * we just store the probability? What about things like
   * --minimum-word-count? How to implement? Perhaps don't transform away from
   * integers until after we've initialized the language model, or include a
   * separate structure to track global minimum-word-count stats?
   */
  val counts = Unigram.create_gram_double_map
  var tokens_accurate = true
  var num_tokens_val = 0.0

  def add_gram(gram: Gram, count: GramCount) {
    counts(gram) += count
    num_tokens_val += count
  }

  def set_gram(gram: Gram, count: GramCount) {
    counts(gram) = count
    tokens_accurate = false
  }

  def remove_gram(gram: Gram) {
    counts -= gram
    tokens_accurate = false
  }

  def contains(gram: Gram) = counts contains gram

  def get_gram(gram: Gram) = counts(gram)

  def iter_grams = counts.toIterable

  // Copy grams iterating over to avoid a ConcurrentModificationException
  def iter_grams_for_modify = iter_grams.toSeq

  // NOTE NOTE NOTE! Possible SCALABUG!! The toSeq needs to be added for some
  // reason; if not, the accuracy of computations that loop over the keys drops
  // dramatically. (On the order of 10-15% when computing smoothing in
  // DiscountedUnigramLangModelFactory.imp_finish_after_global, a factor of
  // 2 when computing a model probability in `model_logprob`.) I have no idea
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

  def num_types = counts.size
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
  val pmodel = new UnigramStorage()
  val model = pmodel

  def gram_to_string(gram: Gram) = Unigram.to_raw(gram)

  def gram_to_feature(gram: Gram) = Unigram.to_raw(gram)

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
  def slow_kl_divergence_debug(other: LangModel, partial: Boolean = true,
      return_contributing_words: Boolean = false) = {
    var kldiv = 0.0
    val contribs =
      if (return_contributing_words) mutable.Map[String, GramCount]() else null
    // 1.
    for (word <- iter_keys) {
      val p = gram_prob(word)
      val q = other.gram_prob(word)
      if (q == 0.0)
        { } // This is OK, we just skip these words
      else if (p <= 0.0 || q <= 0.0)
        errprint("Warning: problematic values: p=%s, q=%s, word=%s", p, q, word)
      else {
        kldiv += p*(log(p) - log(q))
        if (return_contributing_words)
          contribs(gram_to_string(word)) = p*(log(p) - log(q))
      }
    }

    if (partial)
      (kldiv, contribs)
    else {
      // Step 2.
      for (word <- other.iter_keys if !(this contains word)) {
        val p = gram_prob(word)
        val q = other.gram_prob(word)
        kldiv += p*(log(p) - log(q))
        if (return_contributing_words)
          contribs(gram_to_string(word)) = p*(log(p) - log(q))
      }

      val retval =
        kldiv + kl_divergence_34(other.asInstanceOf[UnigramLangModel])
      (retval, contribs)
    }
  }

  /**
   * Steps 3 and 4 of KL-divergence computation.
   * @see #slow_kl_divergence_debug
   */
  def kl_divergence_34(other: UnigramLangModel): Double

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
  def model_logprob(langmodel: LangModel) = {
    langmodel.iter_grams.map {
      case (word, count) => count * gram_logprob(word)
    }.sum
  }

  def get_most_contributing_grams(langmodel: LangModel,
      relative_to: Iterable[LangModel] = Iterable()) = {
    val words_counts = langmodel.iter_grams
    val weights =
      if (relative_to.isEmpty)
        words_counts.map {
          case (word, count) => (word, count * gram_logprob(word))
        }
      else if (relative_to.size == 1) {
        val othermodel = relative_to.head
        words_counts.map {
          case (word, count) =>
            (word, count *
              (gram_logprob(word) - othermodel.gram_logprob(word)))
        }
      } else {
        words_counts.map {
          case (word, count) =>
            val factor = gram_logprob(word)
            val relcontribs =
              relative_to.map { factor - _.gram_logprob(word) }
            (word, count * (relcontribs maxBy { _.abs}))
        }
      }
    weights.toSeq.sortWith { _._2.abs > _._2.abs }
  }
}

/**
 * Default builder for unigram language models.
 *
 * @param factory Corresponding factory object for creating language models.
 * @param ignore_case Whether to fold all words to lowercase.
 * @param stopwords Set of stopwords to ignore.
 * @param whitelist If non-empty, only allow the specified words to pass.
 * @param minimum_word_count Remove words with total count less than this
 *   (in each document). FIXME: This should work with the total count over
 *   all documents, not each individual document.
 * @param word_weights Weights to assign to each word, in the case where
 *   unequal weighting is desired. If empty, do equal weighting.
 * @param missing_word_weight Weight to assign to words not seen in
 *   `word_weight`.
 */
class DefaultUnigramLangModelBuilder(
  factory: LangModelFactory,
  ignore_case: Boolean,
  stopwords: Set[String],
  whitelist: Set[String],
  minimum_word_count: Int,
  word_weights: collection.Map[Gram, Double],
  missing_word_weight: Double
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
      if (debug("pretend-words-seen-once"))
        values_dynarr += 1
      else
        values_dynarr += count
    }
  }

  // Returns true if the word was counted, false if it was ignored due to
  // stoplisting and/or whitelisting
  protected def add_word_with_count(lm: LangModel, word: String,
      count: GramCount): Boolean = {
    val lword = maybe_lowercase(word)
    if (!stopwords.contains(lword) &&
        (whitelist.size == 0 || whitelist.contains(lword))) {
      lm.add_gram(Unigram.to_index(lword), count)
      true
    }
    else
      false
  }

  protected def imp_add_document(lm: LangModel, words: Iterable[String]) {
    for (word <- words)
      add_word_with_count(lm, word, 1)
  }

  protected def imp_add_language_model(lm: LangModel, other: LangModel,
      partial: GramCount) {
    // FIXME: Implement partial!
    for ((word, count) <- other.iter_grams)
      lm.add_gram(word, count)
  }

  /**
   * Actual implementation of `add_keys_values` by subclasses.
   * External callers should use `add_keys_values`.
   */
  protected def imp_add_keys_values(lm: LangModel, keys: Array[String],
      values: Array[Int], num_words: Int) {
    var addedTypes = 0
    var addedTokens = 0
    var totalTokens = 0
    for (i <- 0 until num_words) {
      if(add_word_with_count(lm, keys(i), values(i))) {
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
    val oov = Unigram.to_index("-OOV-")

    // If 'minimum_word_count' was given, then eliminate words whose count
    // is too small.
    if (minimum_word_count > 1) {
      for ((word, count) <- lm.iter_grams_for_modify
           if count < minimum_word_count) {
        lm.remove_gram(word)
        lm.add_gram(oov, count)
      }
    }

    // Adjust the counts to track the specified weights.
    if (word_weights.size > 0)
      for ((word, count) <- lm.iter_grams_for_modify) {
        val weight = word_weights.getOrElse(word, missing_word_weight)
        assert(weight >= 0)
        if (weight == 0)
          lm.remove_gram(word)
        else
          lm.set_gram(word, count * weight)
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
    minimum_word_count: Int,
    word_weights: collection.Map[Gram, Double],
    missing_word_weight: Double
  ) extends DefaultUnigramLangModelBuilder(
    factory, ignore_case, stopwords, whitelist, minimum_word_count,
    word_weights, missing_word_weight
  ) {

  override def finish_before_global(lm: LangModel) {
    super.finish_before_global(lm)

    val oov = Unigram.to_index("-OOV-")

    // Filter the words we don't care about, to save memory and time.
    for ((word, count) <- lm.iter_grams_for_modify
         if !(filter_words contains lm.gram_to_string(word))) {
      lm.remove_gram(word)
      lm.add_gram(oov, count)
    }
  }
}

/**
 * General factory for UnigramLangModel language models.
 */
trait UnigramLangModelFactory extends LangModelFactory { }
