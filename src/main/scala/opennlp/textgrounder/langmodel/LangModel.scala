///////////////////////////////////////////////////////////////////////////////
//  LangModel.scala
//
//  Copyright (C) 2010, 2011, 2012 Ben Wing, The University of Texas at Austin
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

import util.collection._
import util.io.FileHandler
import util.print._
import util.Serializer
import util.memoizer._

import util.debug._

import LangModel._

/**
 * Constants used in various places esp. debugging code, e.g. how many
 * of the most-common words in a language model to output.
 */
object LangModelConstants {
  val lang_model_words_to_print = 15
  //val use_sorted_list = false
}


/**
 * An exception thrown to indicate an error during lang model creation
 */
case class LangModelCreationException(
  message: String,
  cause: Option[Throwable] = None
) extends RethrowableRuntimeException(message)

/**
 * A trait that adds an implementation of `#kl_divergence` in terms of
 * a slow version with debugging info and a fast version, and optionally
 * compares the two.
 */
trait FastSlowKLDivergence {
  def get_kl_divergence_cache(): KLDivergenceCache

  /**
   * This is a basic implementation of the computation of the KL-divergence
   * between this lang model and another lang model, including possible
   * debug information.  Useful for checking against the other, faster
   * implementation in `fast_kl_divergence`.
   * 
   * @param xother The other lang model to compute against.
   * @param partial If true, only compute the contribution involving words
   *   that exist in our lang model; otherwise we also have to take into
   *   account words in the other lang model even if we haven't seen them,
   *   and often also (esp. in the presence of smoothing) the contribution
   *   of all other words in the vocabulary.
   * @param return_contributing_words If true, return a map listing
   *   the words (or n-grams, or whatever) in both lang models (or, for a
   *   partial KL-divergence, the words in our lang model) and the amount
   *   of total KL-divergence they compute, useful for debugging.
   *   
   * @return A tuple of (divergence, word_contribs) where the first
   *   value is the actual KL-divergence and the second is the map
   *   of word contributions as described above; will be null if
   *   not requested.
   */
  def slow_kl_divergence_debug(xother: LangModel, partial: Boolean = false,
      return_contributing_words: Boolean = false):
      (Double, collection.Map[String, WordCount])

  /**
   * Compute the KL-divergence using the "slow" algorithm of
   * #slow_kl_divergence_debug, but without requesting or returning debug
   * info.
   */
  def slow_kl_divergence(other: LangModel, partial: Boolean = false) = {
    val (kldiv, contribs) =
      slow_kl_divergence_debug(other, partial, false)
    kldiv
  }

  /**
   * A fast, optimized implementation of KL-divergence.  See the discussion in
   * `slow_kl_divergence_debug`.
   */
  def fast_kl_divergence(cache: KLDivergenceCache, other: LangModel,
      partial: Boolean = false): Double

  /**
   * Check fast and slow KL-divergence versions against each other.
   */
  def test_kl_divergence(cache: KLDivergenceCache, other: LangModel,
      partial: Boolean = false) = {
    val slow_kldiv = slow_kl_divergence(other, partial)
    val fast_kldiv = fast_kl_divergence(cache, other, partial)
    if (abs(fast_kldiv - slow_kldiv) > 1e-8) {
      errprint("Fast KL-div=%s but slow KL-div=%s", fast_kldiv, slow_kldiv)
      assert(fast_kldiv == slow_kldiv)
    }
    fast_kldiv
  }

  /**
   * The actual kl_divergence implementation.  The fast KL divergence
   * implementation can be tested using '--debug test-kl'.  This compares
   * fast and slow against each other, throwing an assertion failure if they
   * are more than a very small amount different (the small amount rather
   * than 0 to account for possible rounding error).
   */
  protected def imp_kl_divergence(cache: KLDivergenceCache, other: LangModel,
      partial: Boolean) = {
    if (debug("test-kl"))
      test_kl_divergence(cache, other, partial)
    else
      fast_kl_divergence(cache, other, partial)
  }
}

/**
 * A class that controls how to build a language model, whether the
 * source comes from a data file, a document, another lang model, etc.
 * The actual words added can be transformed in various ways, e.g.
 * case-folding the words (typically by converting to all lowercase), ignoring
 * words seen in a stoplist, converting some words to a generic -OOV-,
 * eliminating words seen less than a minimum nmber of times, etc.
 */
abstract class LangModelBuilder(factory: LangModelFactory) {
  /**
   * Actual implementation of `add_document` by subclasses.
   * External callers should use `add_document`.
   */
  protected def imp_add_document(lm: LangModel, words: Iterable[String])

  /**
   * Actual implementation of `add_language_model` by subclasses.
   * External callers should use `add_language_model`.
   */
  protected def imp_add_language_model(lm: LangModel, other: LangModel,
    partial: WordCount = 1.0)

  /**
   * Actual implementation of `finish_before_global` by subclasses.
   * External callers should use `finish_before_global`.
   */
  protected def imp_finish_before_global(lm: LangModel)

  /**
   * Incorporate a document into the lang model.  The document is described
   * by a sequence of words.
   */
  def add_document(lm: LangModel, words: Iterable[String]) {
    assert(!lm.finished)
    assert(!lm.finished_before_global)
    imp_add_document(lm, words)
  }

  /**
   * Incorporate the given lang model into our lang model.
   * `partial` is a scaling factor (between 0.0 and 1.0) used for
   * interpolating multiple lang models.
   */
  def add_language_model(lm: LangModel, other: LangModel,
      partial: WordCount = 1.0) {
    assert(!lm.finished)
    assert(!lm.finished_before_global)
    assert(partial >= 0.0 && partial <= 1.0)
    imp_add_language_model(lm, other, partial)
  }

  /**
   * Partly finish computation of lang model.  This is called when the
   * lang model has been completely populated with words, and no more
   * modifications (e.g. incorporation of words or other lang models) will
   * be made to the lang model.  It should do any additional changes that
   * depend on the lang model being complete, but which do not depend on
   * the global language-model statistics having been computed. (These
   * statistics can be computed only after *all* language models that
   * are used to create these global statistics have been completely
   * populated.)
   *
   * @see #finish_after_global
   * 
   */
  def finish_before_global(lm: LangModel) {
    assert(!lm.finished)
    assert(!lm.finished_before_global)
    imp_finish_before_global(lm)
    if (lm.empty)
      throw new LangModelCreationException(
        "Attempt to create an empty lang model: %s" format lm)
    lm.finished_before_global = true
  }

  /**
   * Create the language model of a document, given the value of the field
   * describing the lang model (typically called "unigram-counts" or
   * "ngram-counts" or "text").
   *
   * @param doc Document to set the lang model of.
   * @param diststr String from the document file, describing the lang model.
   */
  def create_lang_model(countstr: String): LangModel
}

trait KLDivergenceCache {
}

/**
 * A factory object for LangModels (language models).  Currently, there is
 * only one factory object in a particular instance of the application
 * (i.e. it's a singleton), but the particular factory used depends on a
 * command-line parameter.
 */
trait LangModelFactory {
  /**
   * Total number of word types seen (size of vocabulary)
   */
  var total_num_word_types = 0

  /**
   * Total number of word tokens seen
   */
  var total_num_word_tokens = 0

  /**
   * Corresponding builder object for building up the language model
   */
  val builder: LangModelBuilder

  /**
   * Create an empty language model.
   */
  def create_lang_model: LangModel

  /**
   * Add the given lang model to the global backoff statistics,
   * if any.
   */
  def note_lang_model_globally(lm: LangModel) { }

  /**
   * Finish computing any global backoff statistics.  This is called after
   * all of the relevant lang models have been created.  In practice, these
   * are those associated with training documents, which are read in during
   * `read_word_counts`.
   */
  def finish_global_backoff_stats()
}

/**
 * Normal version of memoizer which maps words to Ints.
 */
trait WordAsIntMemoizer {
  type Word = Int
  // Use Trove for fast, efficient hash tables.
  val hashfact = new TroveHashTableFactory
  // Alternatively, just use the normal Scala hash tables.
  // val hashfact = new ScalaHashTableFactory
  val memoizer = new ToIntMemoizer[String](hashfact)
  val invalid_word: Word = -1

  val blank_memoized_string = memoizer.memoize("")

  // These should NOT be declared to have a type of mutable.Map[Word, Int]
  // or whatever.  Doing so ensures that we go through the Map[] interface,
  // which requires lots of boxing and unboxing.
  def create_word_int_map = hashfact.create_int_int_map

  // Same here as for `create_word_int_map`

  def create_word_double_map = hashfact.create_int_double_map
}

/**
 * Version of memoizer which maps words to themselves as strings. This tests
 * that we don't make any assumptions about memoized words being Ints.
 */
trait WordAsStringMemoizer {
  type Word = String
  val memoizer = new IdentityMemoizer[String]
  val invalid_word: Word = null

  val blank_memoized_string = memoizer.memoize("")

  def create_word_int_map = intmap[Word]()

  def create_word_double_map = doublemap[Word]()
}

/**
 * Object describing how "words" are memoized and storing the memoizer
 * object used for memoization. Memoization means mapping words (which are
 * strings) into integers, for faster and less space-intensive operations
 * on them).
 */
object LangModel extends WordAsIntMemoizer {
  // FIXME! This is present to handle partial counts but these weren't
  // ever really used because they didn't seem to help.
  type WordCount = Double
}

/**
 * An interface for storing and retrieving vocabulary items (e.g. words,
 * n-grams, etc.).
 *
 * @tparam Item Type of the items stored.
 */
trait ItemStorage[Item] {
  // NOTE: Do not need to specialize the type on Int, given the current
  // definition of UnigramStorage; in fact, doing so interferes with the
  // ability of the compiler to inline what we ask it to inline in
  // UnigramStorage.

  /**
   * Add an item with the given count.  If the item exists already,
   * add the count to the existing value.
   */
  def add_item(item: Item, count: WordCount)

  /**
   * Set the item to the given count.  If the item exists already,
   * replace its value with the given one.
   */
  def set_item(item: Item, count: WordCount)

  /**
   * Remove an item, if it exists.
   */
  def remove_item(item: Item)

  /**
   * Return whether a given item is stored.
   */
  def contains(item: Item): Boolean

  /**
   * Return the count of a given item.
   */
  def get_item(item: Item): WordCount

  /**
   * Iterate over all items that are stored.
   */
  def iter_items: Iterable[(Item, WordCount)]

  /**
   * Iterate over all keys that are stored.
   */
  def iter_keys: Iterable[Item]

  /**
   * Total number of tokens stored.
   */
  def num_tokens: WordCount

  /**
   * Total number of item types (i.e. number of distinct items)
   * stored.
   */
  def num_types: Int

  /**
   * True if this model is empty, containing no items.
   */
  def empty = num_types == 0
}

/**
 * A language model, i.e. a statistical distribution over words in
 * a document, cell, etc.
 */
abstract class LangModel(val factory: LangModelFactory) {
  type Item
  val model: ItemStorage[Item]

  /**
   * Whether we have finished computing the lang model, and therefore can
   * reliably do probability lookups.
   */
  var finished = false
  /**
   * Whether we have already called `finish_before_global`.  If so, this
   * means we can't modify the lang model any more.
   */
  var finished_before_global = false

  /** Is this lang model empty? */
  def empty = model.empty

  /**
   * Incorporate a document into the lang model.
   */
  def add_document(words: Iterable[String]) {
    factory.builder.add_document(this, words)
  }

  /**
   * Incorporate the given lang model into our lang model.
   * `partial` is a scaling factor (between 0.0 and 1.0) used for
   * interpolating multiple lang models.
   */
  def add_language_model(other: LangModel, partial: WordCount = 1.0) {
    factory.builder.add_language_model(this, other, partial)
  }

  /**
   * Partly finish computation of lang model.  This is called when the
   * lang model has been completely populated with words, and no more
   * modifications (e.g. incorporation of words or other lang models) will
   * be made to the lang model.  It should do any additional changes that
   * depend on the lang model being complete, but which do not depend on
   * the global language-model statistics having been computed. (These
   * statistics can be computed only after *all* language models that
   * are used to create these global statistics have been completely
   * populated.)
   *
   * @see #finish_after_global
   * 
   */
  def finish_before_global() {
    factory.builder.finish_before_global(this)
  }

  /**
   * Actual implementation of `finish_after_global` by subclasses.
   * External callers should use `finish_after_global`.
   */
  protected def imp_finish_after_global()

  /**
   * Completely finish computation of the language model.  This is called
   * after finish_global_backoff_stats() on the factory method, and can be
   * used to compute values for the lang model that depend on the
   * global language-model statistics.
   */
  def finish_after_global() {
    assert(!finished)
    assert(finished_before_global)
    imp_finish_after_global()
    finished = true
  }

  def dunning_log_likelihood_1(a: Double, b: Double, c: Double, d: Double) = {
    val cprime = a+c
    val dprime = b+d
    val factor = (a+b)/(cprime+dprime)
    val e1 = cprime*factor
    val e2 = dprime*factor
    val f1 = if (a > 0) a*math.log(a/e1) else 0.0
    val f2 = if (b > 0) b*math.log(b/e2) else 0.0
    val g2 = 2*(f1+f2)
    g2
  }

  def dunning_log_likelihood_2(a: Double, b: Double, c: Double, d: Double) = {
    val N = a+b+c+d
    def m(x: Double) = if (x > 0) x*math.log(x) else 0.0
    2*(m(a) + m(b) + m(c) + m(d) + m(N) - m(a+b) - m(a+c) - m(b+d) - m(c+d))
  }

  /**
   * Return the Dunning log-likelihood of an item in two different corpora,
   * indicating how much more likely the item is in this corpus than the
   * other to be different.
   */
  def dunning_log_likelihood_2x1(item: Item, other: LangModel) = {
    val a = model.get_item(item).toDouble
    // This cast is kind of ugly but I don't see a way around it.
    val b = other.model.get_item(item.asInstanceOf[other.Item]).toDouble
    val c = model.num_tokens.toDouble - a
    val d = other.model.num_tokens.toDouble - b
    val val1 = dunning_log_likelihood_1(a, b, c, d)
    val val2 = dunning_log_likelihood_2(a, b, c, d)
    if (debug("dunning"))
      errprint("Comparing log-likelihood for %g %g %g %g = %g vs. %g",
        a, b, c, d, val1, val2)
    val1
  }

  def dunning_log_likelihood_2x2(item: Item, other_b: LangModel,
      other_c: LangModel, other_d: LangModel) = {
    val a = model.get_item(item).toDouble
    // This cast is kind of ugly but I don't see a way around it.
    val b = other_b.model.get_item(item.asInstanceOf[other_b.Item]).toDouble
    val c = other_c.model.get_item(item.asInstanceOf[other_c.Item]).toDouble
    val d = other_d.model.get_item(item.asInstanceOf[other_d.Item]).toDouble
    val val1 = dunning_log_likelihood_1(a, b, c, d)
    val val2 = dunning_log_likelihood_2(a, b, c, d)
    if (debug("dunning"))
      errprint("Comparing log-likelihood for %g %g %g %g = %g vs. %g",
        a, b, c, d, val1, val2)
    val2
  }

  /**
   * Actual implementation of `kl_divergence` by subclasses.
   * External callers should use `kl_divergence`.
   */
  protected def imp_kl_divergence(cache: KLDivergenceCache,
    other: LangModel, partial: Boolean): Double

  /**
   * Compute the KL-divergence between this lang model and another
   * lang model.
   * 
   * @param cache Cached information about this lang model, used to
   *   speed up computations.  Never needs to be supplied; null can always
   *   be given, to cause a new cache to be created.
   * @param other The other lang model to compute against.
   * @param partial If true, only compute the contribution involving words
   *   that exist in our lang model; otherwise we also have to take
   *   into account words in the other lang model even if we haven't
   *   seen them, and often also (esp. in the presence of smoothing) the
   *   contribution of all other words in the vocabulary.
   *   
   * @return The KL-divergence value.
   */
  def kl_divergence(cache: KLDivergenceCache, other: LangModel,
      partial: Boolean = false) = {
    assert(finished)
    assert(other.finished)
    imp_kl_divergence(cache, other, partial)
  }

  def get_kl_divergence_cache(): KLDivergenceCache = null

  /**
   * Compute the symmetric KL-divergence between two lang models by averaging
   * the respective one-way KL-divergences in each direction.
   * 
   * @param partial Same as in `kl_divergence`.
   */
  def symmetric_kldiv(cache: KLDivergenceCache, other: LangModel,
      partial: Boolean = false) = {
    0.5*this.kl_divergence(cache, other, partial) +
    0.5*this.kl_divergence(cache, other, partial)
  }

  /**
   * Implementation of the cosine similarity between this and another
   * lang model, using either unsmoothed or smoothed probabilities.
   *
   * @param partial Same as in `kl_divergence`.
   * @param smoothed If true, use smoothed probabilities, if smoothing exists;
   *   otherwise, do unsmoothed.
   */
  def cosine_similarity(other: LangModel, partial: Boolean = false,
    smoothed: Boolean = false): Double

  /**
   * For a document described by its language model, return the
   * log probability log p(langmodel|other langmodel) using a Naive Bayes
   * algorithm.
   *
   * @param langmodel Language model of document.
   */
  def get_nbayes_logprob(langmodel: LangModel): Double

  /**
   * Name of class, for `toString`.
   */
  def class_name: String

  /**
   * Convert an item (of type Item) to a string.
   */
  def item_to_string(item: Item): String

  /**
   * Extra text to add to the `toString` output, for extra class params.
   */
  def innerToString = ""

  /**
   * Convert to a readable string.
   *
   * @param num_items_to_print How many items (word, ngrams, etc.) to print.
   */
  def toString(num_items_to_print: Int) = {
    val finished_str =
      if (!finished) ", unfinished" else ""
    val num_actual_items_to_print =
      if (num_items_to_print < 0) model.num_types
      else num_items_to_print
    val need_dots = model.num_types > num_actual_items_to_print
    val items =
      for ((item, count) <-
        model.iter_items.toSeq.sortWith(_._2 > _._2).
          view(0, num_actual_items_to_print))
      yield "%s=%s" format (item_to_string(item), count) 
    val itemstr = (items mkString " ") + (if (need_dots) " ..." else "")
    "%s(%s types, %s tokens%s%s, %s)" format (
        class_name, model.num_types, model.num_tokens, innerToString,
        finished_str, itemstr)
  }

  /**
   * Convert to a readable string, with all words printed, for debug
   * purposes.
   */
  def debug_string = toString(-1)

  override def toString = toString(
    LangModelConstants.lang_model_words_to_print
  )

  /**
   * Return the grams (words or n-grams) that contribute most to the
   * Naive Bayes probability of the given document's language model,
   * evaluated over this language model. Each gram gets an additive weight of
   * count * log(prob), which is returned along with the gram.
   *
   * @param relative_to If empty, as above. If one other language model is
   *   given, return grams whose relative contribution to the Naive Bayes
   *   probability of this model compared with the other lang model is
   *   greatest, i.e. those for which
   *     weight = count * (log(thisprob) - log(otherprob))
   *   is greatest. If multiple other language models are given, use
   *   the same formula for comparing two language models but for each gram,
   *   compare this language model to each of the others in turn and take
   *   the maximum weight as the gram's weight.
   */
  def get_most_contributing_grams(langmodel: LangModel,
    relative_to: Iterable[LangModel] = Iterable()): Iterable[(Item, Double)]
}
