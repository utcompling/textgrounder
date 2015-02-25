///////////////////////////////////////////////////////////////////////////////
//  LangModel.scala
//
//  Copyright (C) 2010-2014 Ben Wing, The University of Texas at Austin
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
package object langmodel {
  // This is present to handle partial counts but these weren't ever really
  // used because they didn't seem to help. However, it's also currently used
  // to handle differently-weighted words, which (so far) also hasn't helped.
  // It's also used to handle importance weights when combining multiple
  // corpora together ('--combine-corpora concatenate').
  type GramCount = Double
  type Gram = Int
}

package langmodel {

import math._
import collection.mutable

import util.collection._
import util.error._
import util.io.FileHandler
import util.print._
import util.serialize.TextSerializer
import util.memoizer._

import util.debug._

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
   * @param other The other lang model to compute against.
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
  def slow_kl_divergence_debug(other: LangModel, partial: Boolean = true,
      return_contributing_words: Boolean = false):
      (Double, collection.Map[String, GramCount])

  /**
   * Compute the KL-divergence using the "slow" algorithm of
   * #slow_kl_divergence_debug, but without requesting or returning debug
   * info.
   */
  def slow_kl_divergence(other: LangModel, partial: Boolean = true) = {
    val (kldiv, contribs) =
      slow_kl_divergence_debug(other, partial,
        return_contributing_words = false)
    kldiv
  }

  /**
   * A fast, optimized implementation of KL-divergence.  See the discussion in
   * `slow_kl_divergence_debug`.
   */
  def fast_kl_divergence(other: LangModel, partial: Boolean = true,
    cache: KLDivergenceCache = null): Double

  /**
   * Check fast and slow KL-divergence versions against each other.
   */
  def test_kl_divergence(other: LangModel, partial: Boolean = true,
    cache: KLDivergenceCache = null) = {
    val slow_kldiv = slow_kl_divergence(other, partial)
    val fast_kldiv = fast_kl_divergence(other, partial, cache)
    if (abs(fast_kldiv - slow_kldiv) > 1e-8) {
      errprint("Fast KL-div=%s but slow KL-div=%s", fast_kldiv, slow_kldiv)
      assert_==(fast_kldiv, slow_kldiv)
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
  protected def imp_kl_divergence(other: LangModel, partial: Boolean,
      cache: KLDivergenceCache) = {
    if (debug("test-kl"))
      test_kl_divergence(other, partial, cache)
    else
      fast_kl_divergence(other, partial, cache)
  }
}

/**
 * A class that controls how to build a language model, whether the
 * source comes from a data file, a document, another lang model, etc.
 * The actual words added can be transformed in various ways, e.g.
 * case-folding the words (typically by converting to all lowercase), ignoring
 * words seen in a stoplist, converting some words to a generic -OOV-,
 * eliminating words seen less than a minimum nmber of times, etc.
 *
 * In this case, the difference between a builder and a factory is that
 * the factory handles the creation of the LangModel object and stores
 * global values needed by the language model during its use. Currently
 * this consists of back-off statistics and word weights. On the other
 * hand, the builder handles the population of the language model from
 * external sources. For each subclass of LangModel, there is a
 * corresponding factory class, but that isn't the case for builders.
 * For example, there is one default builder class for all unigram
 * language models and another builder class that allows for filtering
 * out all but a specified set of words. (FIXME: There isn't a clear
 * conceptual distinction between a whitelist of words and the set of
 * words allowed through the filter. They should be merged.)
 */
abstract class LangModelBuilder(factory: LangModelFactory) {
  /**
   * Actual implementation of `add_document` by subclasses.
   * External callers should use `add_document`.
   */
  protected def imp_add_document(lm: LangModel, words: Iterable[String],
    domain: String, weight: Double)

  /**
   * Actual implementation of `add_language_model` by subclasses.
   * External callers should use `add_language_model`.
   */
  protected def imp_add_language_model(lm: LangModel, other: LangModel,
    weight: Double = 1.0)

  /**
   * Incorporate a document into the lang model.  The document is described
   * by a sequence of words.
   */
  def add_document(lm: LangModel, words: Iterable[String], domain: String = "",
      weight: Double = 1.0) {
    assert(!lm.finished)
    assert(!lm.finished_before_global)
    imp_add_document(lm, words, domain, weight)
  }

  /**
   * Incorporate the given lang model into our lang model.
   * `weight` is a scaling factor used e.g. for interpolating
   * multiple lang models.
   */
  def add_language_model(lm: LangModel, other: LangModel, weight: Double = 1.0) {
    assert(!lm.finished)
    assert(!lm.finished_before_global)
    assert(weight >= 0.0, s"Weight $weight must be non-negative")
    imp_add_language_model(lm, other, weight)
  }

  /**
   * Partly finish computation of lang model.  This is called from
   * the lang model's `finish_before_global`, to handle builder-
   * specific changes to the lang model (e.g. implementing
   * `--minimum-word-count`).
   */
  def finish_before_global(lm: LangModel)

  /**
   * Create the language model of a document, given the value of the field
   * describing the lang model (typically called "unigram-counts" or
   * "ngram-counts" or "text").
   *
   * @param doc Document to set the lang model of.
   * @param countstr String from the document file, describing the lang model.
   * @param domain Domain (e.g. "in", "out") used for feature augmentation
   *   for domain adaptation ala Daume 2007 (EasyAdapt). If empty, don't do
   *   feature augmentation.
   * @param weight Factor to weight the counts.
   */
  def create_lang_model(countstr: String, domain: String,
    weight: Double): LangModel
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
trait GramAsIntMemoizer[Item] extends ToIntMemoizer[Item] {
  val invalid_gram: Gram = -1

  // These should NOT be declared to have a type of mutable.Map[Gram, Int]
  // or whatever.  Doing so ensures that we go through the Map[] interface,
  // which requires lots of boxing and unboxing.
  def create_gram_int_map = hashfact.create_int_int_map

  // Same here as for `create_gram_int_map`

  def create_gram_double_map = hashfact.create_int_double_map
}

/**
 * Normal version of memoizer which maps words to Ints.
 */
trait StringGramAsIntMemoizer extends GramAsIntMemoizer[String] {
}

/**
 * Version of memoizer which maps words to themselves as strings. This tests
 * that we don't make any assumptions about memoized words being Ints.
 */
trait StringGramAsStringMemoizer extends IdentityMemoizer[String] {
  type Gram = String

  val invalid_gram: Gram = null

  def create_gram_int_map = intmap[Gram]()

  def create_gram_double_map = doublemap[Gram]()
}

/**
 * An interface for storing and retrieving vocabulary items (e.g. words,
 * n-grams, etc.).
 */
trait GramStorage {
  /**
   * Add a gram with the given count.  If the gram exists already,
   * add the count to the existing value.
   */
  def add_gram(gram: Gram, count: GramCount)

  /**
   * Set the gram to the given count.  If the gram exists already,
   * replace its value with the given one.
   */
  def set_gram(gram: Gram, count: GramCount)

  /**
   * Remove a gram, if it exists.
   */
  def remove_gram(gram: Gram)

  /**
   * Return whether a given gram is stored.
   */
  def contains(gram: Gram): Boolean

  /**
   * Return the count of a given gram.
   */
  def get_gram(gram: Gram): GramCount

  /**
   * Iterate over all grams that are stored.
   */
  def iter_grams: Iterable[(Gram, GramCount)]

  /**
   * Iterate over all grams that are stored, when planning on modifying the
   * model during iteration.
   */
  def iter_grams_for_modify: Iterable[(Gram, GramCount)]

  /**
   * Iterate over all keys that are stored.
   */
  def iter_keys: Seq[Gram]

  /**
   * Total number of tokens stored.
   */
  def num_tokens: GramCount

  /**
   * Total number of gram types (i.e. number of distinct grams)
   * stored.
   */
  def num_types: Int

  /**
   * True if this model is empty, containing no grams.
   */
  def empty = num_types == 0
}

/**
 * A language model, i.e. a statistical distribution over words in
 * a document, cell, etc.
 */
abstract class LangModel(val factory: LangModelFactory) {
  val model: GramStorage

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

  /**
   * Add a gram with the given count.  If the gram exists already,
   * add the count to the existing value.
   */
  def add_gram(gram: Gram, count: GramCount): Unit =
    model.add_gram(gram, count)

  /**
   * Set the gram to the given count.  If the gram exists already,
   * replace its value with the given one.
   */
  def set_gram(gram: Gram, count: GramCount): Unit =
    model.set_gram(gram, count)

  /**
   * Remove a gram, if it exists.
   */
  def remove_gram(gram: Gram): Unit = model.remove_gram(gram)

  /**
   * Return whether a given gram is stored.
   */
  def contains(gram: Gram): Boolean = model.contains(gram)

  /**
   * Return the count of a given gram.
   */
  def get_gram(gram: Gram): GramCount = model.get_gram(gram)

  /**
   * Iterate over all grams that are stored.
   */
  def iter_grams: Iterable[(Gram, GramCount)] = model.iter_grams

  /**
   * Iterate over all grams that are stored, when planning on modifying the
   * model during iteration.
   */
  def iter_grams_for_modify: Iterable[(Gram, GramCount)] =
    model.iter_grams_for_modify

  /**
   * Iterate over all keys that are stored.
   */
  def iter_keys: Seq[Gram] = model.iter_keys

  /**
   * Total number of tokens stored.
   */
  def num_tokens: GramCount = model.num_tokens

  /**
   * Total number of gram types (i.e. number of distinct grams)
   * stored.
   */
  def num_types: Int = model.num_types

  /** Is this lang model empty? */
  def empty = model.empty

  /**
   * Incorporate a document into the lang model.
   */
  def add_document(words: Iterable[String], domain: String = "",
      weight: Double = 1.0) {
    factory.builder.add_document(this, words, domain, weight)
  }

  /**
   * Incorporate the given lang model into our lang model.
   * `weight` is a scaling factor used e.g. for
   * interpolating multiple lang models.
   */
  def add_language_model(other: LangModel, weight: Double = 1.0) {
    factory.builder.add_language_model(this, other, weight)
  }

  /**
   * Actual implementation of `finish_before_global` by subclasses.
   * External callers should use `finish_before_global`.
   */
  protected def imp_finish_before_global() {
    factory.builder.finish_before_global(this)
  }

  /**
   * Actual implementation of `finish_after_global` by subclasses.
   * External callers should use `finish_after_global`.
   */
  protected def imp_finish_after_global()

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
    assert(!finished)
    assert(!finished_before_global)
    imp_finish_before_global()
    // if (empty)
    //  throw new LangModelCreationException(
    //    "Attempt to create an empty lang model: %s" format this)
    finished_before_global = true
  }

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
   * Return the Dunning log-likelihood of a gram in two different corpora,
   * indicating how much more likely the gram is in this corpus than the
   * other to be different.
   */
  def dunning_log_likelihood_2x1(gram: Gram, other: LangModel) = {
    val a = get_gram(gram).toDouble
    val b = other.get_gram(gram).toDouble
    val c = num_tokens.toDouble - a
    val d = other.num_tokens.toDouble - b
    val val1 = dunning_log_likelihood_1(a, b, c, d)
    val val2 = dunning_log_likelihood_2(a, b, c, d)
    if (debug("dunning"))
      errprint("Comparing log-likelihood for %g %g %g %g = %g vs. %g",
        a, b, c, d, val1, val2)
    val1
  }

  def dunning_log_likelihood_2x2(gram: Gram, other_b: LangModel,
      other_c: LangModel, other_d: LangModel) = {
    val a = get_gram(gram).toDouble
    val b = other_b.get_gram(gram).toDouble
    val c = other_c.get_gram(gram).toDouble
    val d = other_d.get_gram(gram).toDouble
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
  protected def imp_kl_divergence(other: LangModel, partial: Boolean,
    cache: KLDivergenceCache): Double

  /**
   * Compute the KL-divergence between this lang model and another
   * lang model.
   *
   * @param other The other lang model to compute against.
   * @param partial If true, only compute the contribution involving words
   *   that exist in our lang model; otherwise we also have to take
   *   into account words in the other lang model even if we haven't
   *   seen them, and often also (esp. in the presence of smoothing) the
   *   contribution of all other words in the vocabulary.
   * @param cache Cached information about this lang model, used to
   *   speed up computations.  Never needs to be supplied; null can always
   *   be given, to cause a new cache to be created.
   *
   * @return The KL-divergence value.
   */
  def kl_divergence(other: LangModel, partial: Boolean = true,
      cache: KLDivergenceCache = null) = {
    assert(finished)
    assert(other.finished)
    imp_kl_divergence(other, partial, cache)
  }

  def get_kl_divergence_cache(): KLDivergenceCache = null

  /**
   * Compute the symmetric KL-divergence between two lang models by averaging
   * the respective one-way KL-divergences in each direction.
   *
   * @param partial Same as in `kl_divergence`.
   */
  def symmetric_kldiv(other: LangModel, partial: Boolean = true,
    cache: KLDivergenceCache = null) = {
    0.5*this.kl_divergence(other, partial, cache) +
    0.5*this.kl_divergence(other, partial, cache)
  }

  /**
   * Implementation of the cosine similarity between this and another
   * lang model, using either unsmoothed or smoothed probabilities.
   *
   * @param partial Same as in `kl_divergence`.
   * @param smoothed If true, use smoothed probabilities, if smoothing exists;
   *   otherwise, do unsmoothed.
   */
  def cosine_similarity(other: LangModel, partial: Boolean = true,
    smoothed: Boolean = false): Double

  /**
   * A fast implementation of the sum of unsmoothed probabilities of the
   * words in a document. Used mostly with '--tf-idf' or similar.
   */
  def sum_frequency(other: LangModel) = {
    assert(finished)
    val qfact = 1.0/other.num_tokens
    val qmodel = other.model
    var sum = 0.0
    for ((word, count) <- iter_grams) {
      val q = count * qmodel.get_gram(word) * qfact
      assert(!q.isNaN, s"Saw NaN: count = $count, gram_count = ${qmodel.get_gram(word)}, qfact = $qfact")
      sum += q
    }

    sum
  }

  /**
   * For a document described by its language model, return the
   * log probability log p(langmodel|other langmodel).
   *
   * @param langmodel Language model of document.
   */
  def model_logprob(langmodel: LangModel): Double

  /**
   * Name of class, for `toString`.
   */
  def class_name: String

  /**
   * Return the probability of a given gram in the lang model.
   */
  protected def imp_gram_prob(word: Gram): Double

  def gram_prob(gram: Gram): Double = {
    assert(finished)
    //assert(!empty,
    //  s"Attempt to lookup ${gram_to_string(gram)} in empty lang model ${this}")
    val prob = imp_gram_prob(gram)
    // Write this way because if negated as an attempt to catch bad values,
    // it won't catch NaN, which fails all comparisons.
    assert(prob >= 0 && prob <= 1,
      s"Out-of-bounds prob $prob for gram ${gram_to_string(gram)}")
    prob
  }

  /**
   * Return the log-probability of a word, taking into account the
   * possibility that the probability is zero (in which case the
   * log-probability is defined to be zero also).
   *
   * @see #gram_prob
   */
  def gram_logprob(word: Gram) = {
    val value = gram_prob(word)
    assert_>=(value, 0)
    // The probability returned will be 0 for words never seen in the
    // training data at all, i.e. we don't even have any global values to
    // back off to. General practice is to ignore such words.
    if (value > 0)
      log(value)
    else 0.0
  }

  def mle_gram_prob(gram: Gram): Double = {
    assert(finished)
    // Write this way so we're not tripped up by empty lang model
    if (contains(gram)) {
      assert_>(num_tokens, 0.0)
      get_gram(gram).toDouble/num_tokens
    } else 0.0
  }

  /**
   * Look for the most common gram matching a given predicate.
   * @param pred Predicate, passed the raw (unmemoized) form of a gram.
   *   Should return true if a gram matches.
   * @return Most common gram matching the predicate (wrapped with
   *   Some()), or None if no match.
   */
  def find_most_common_gram(pred: String => Boolean): Option[Gram] = {
    val filtered =
      (for ((gram, count) <- iter_grams if pred(gram_to_string(gram)))
        yield (gram, count)).toSeq
    if (filtered.length == 0) None
    else {
      val (maxgram, maxcount) = filtered maxBy (_._2)
      Some(maxgram)
    }
  }

  /**
   * Convert a gram (of type Gram) to a string.
   */
  def gram_to_string(gram: Gram): String

  /**
   * Convert a gram (of type Gram) to string for use as a feature name;
   * must not have any spaces in it.
   */
  def gram_to_feature(gram: Gram): String

  /**
   * Extra text to add to the `toString` output, for extra class params.
   */
  def innerToString = ""

  /**
   * Convert to a readable string.
   *
   * @param num_grams_to_print How many grams (word, ngrams, etc.) to print.
   */
  def toString(num_grams_to_print: Int) = {
    val finished_str =
      if (!finished) ", unfinished" else ""
    val num_actual_grams_to_print =
      if (num_grams_to_print < 0) num_types
      else num_grams_to_print
    val need_dots = num_types > num_actual_grams_to_print
    val grams =
      for ((gram, count) <- iter_grams.toSeq.sortWith(_._2 > _._2).
            view(0, num_actual_grams_to_print))
      yield "%s=%s" format (gram_to_string(gram), count)
    val gramstr = (grams mkString " ") + (if (need_dots) " ..." else "")
    "%s(%s types, %s tokens%s%s, %s)" format (
        class_name, num_types, num_tokens, innerToString,
        finished_str, gramstr)
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
    relative_to: Iterable[LangModel] = Iterable()): Iterable[(Gram, Double)]
}

}
