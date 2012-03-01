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
//////// WordDist.scala
////////
//////// Copyright (c) 2010, 2011 Ben Wing.
////////

package opennlp.textgrounder.worddist

import math._
import collection.mutable

import com.codahale.trove.{mutable => trovescala}

import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.ioutil.FileHandler
import opennlp.textgrounder.util.printutil.{errprint, warning}
import opennlp.textgrounder.util.Serializer

import opennlp.textgrounder.gridlocate.GridLocateDriver.Debug._
import opennlp.textgrounder.gridlocate.GenericTypes._

import WordDist.memoizer._

// val use_sorted_list = false

//////////////////////////////////////////////////////////////////////////////
//                             Word distributions                           //
//////////////////////////////////////////////////////////////////////////////

/**
 * A class for "memoizing" words, i.e. mapping them to some other type
 * (e.g. Int) that should be faster to compare and potentially require
 * less space.
 */
abstract class Memoizer {
  /**
   * The type of a memoized word.
   */
  type Word
  /**
   * Map a word as a string to its memoized form.
   */
  def memoize_string(word: String): Word
  /**
   * Map a word from its memoized form back to a string.
   */
  def unmemoize_string(word: Word): String

  /**
   * The type of a mutable map from memoized words to Ints.
   */
  type WordIntMap
  /**
   * Create a mutable map from memoized words to Ints.
   */
  def create_word_int_map(): WordIntMap
  /**
   * The type of a mutable map from memoized words to Doubles.
   */
  type WordDoubleMap
  /**
   * Create a mutable map from memoized words to Doubles.
   */
  def create_word_double_map(): WordDoubleMap

  lazy val blank_memoized_string = memoize_string("")

  def lowercase_memoized_word(word: Word) =
    memoize_string(unmemoize_string(word).toLowerCase)
}

/**
 * The memoizer we actually use.  Maps word strings to Ints.  Uses Trove
 * for extremely fast and memory-efficient hash tables, making use of the
 * Trove-Scala interface for easy access to the Trove hash tables.
 */
class IntStringMemoizer extends Memoizer {
  type Word = Int
  val invalid_word: Word = 0

  protected var next_word_count: Word = 1

  // For replacing strings with ints.  This should save space on 64-bit
  // machines (string pointers are 8 bytes, ints are 4 bytes) and might
  // also speed lookup.
  //protected val word_id_map = mutable.Map[String,Word]()
  protected val word_id_map = trovescala.ObjectIntMap[String]()

  // Map in the opposite direction.
  //protected val id_word_map = mutable.Map[Word,String]()
  protected val id_word_map = trovescala.IntObjectMap[String]()

  def memoize_string(word: String) = {
    val index = word_id_map.getOrElse(word, 0)
    if (index != 0) index
    else {
      val newind = next_word_count
      next_word_count += 1
      word_id_map(word) = newind
      id_word_map(newind) = word
      newind
    }
  }

  def unmemoize_string(word: Word) = id_word_map(word)

  def create_word_int_map() = trovescala.IntIntMap()
  type WordIntMap = trovescala.IntIntMap
  def create_word_double_map() = trovescala.IntDoubleMap()
  type WordDoubleMap = trovescala.IntDoubleMap
}

/**
 * The memoizer we actually use.  Maps word strings to Ints.  Uses Trove
 * for extremely fast and memory-efficient hash tables, making use of the
 * Trove-Scala interface for easy access to the Trove hash tables.
 */
class TestIntStringMemoizer extends IntStringMemoizer {
  override def memoize_string(word: String) = {
    val cur_nwi = next_word_count
    val index = super.memoize_string(word)
    if (debug("memoize")) {
      if (next_word_count != cur_nwi)
        errprint("Memoizing new string %s to ID %s", word, index)
      else
        errprint("Memoizing existing string %s to ID %s", word, index)
    }
    assert(super.unmemoize_string(index) == word)
    index
  }

  override def unmemoize_string(word: Word) = {
    if (!(id_word_map contains word)) {
      errprint("Can't find ID %s in id_word_map", word)
      errprint("Word map:")
      var its = id_word_map.toList.sorted
      for ((key, value) <- its)
        errprint("%s = %s", key, value)
      assert(false, "Exiting due to bad code in unmemoize_string")
      null
    } else {
      val string = super.unmemoize_string(word)
      if (debug("memoize"))
        errprint("Unmemoizing existing ID %s to string %s", word, string)
      assert(super.memoize_string(string) == word)
      string
    }
  }
}

/**
 * A memoizer for testing that doesn't actually do anything -- the memoized
 * words are also strings.  This tests that we don't make any assumptions
 * about memoized words being Ints.
 */
object IdentityMemoizer extends Memoizer {
  type Word = String
  val invalid_word: Word = null
  def memoize_string(word: String): Word = word
  def unmemoize_string(word: Word): String = word

  type WordIntMap = mutable.Map[Word, Int]
  def create_word_int_map() = intmap[Word]()
  type WordDoubleMap = mutable.Map[Word, Double]
  def create_word_double_map() = doublemap[Word]()
}

/**
 * A trait that adds an implementation of `#kl_divergence` in terms of
 * a slow version with debugging info and a fast version, and optionally
 * compares the two.
 */
trait FastSlowKLDivergence {
  /**
   * This is a basic implementation of the computation of the KL-divergence
   * between this distribution and another distribution, including possible
   * debug information.  Useful for checking against the other, faster
   * implementation in `fast_kl_divergence`.
   * 
   * @param xother The other distribution to compute against.
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
   * @return A tuple of (divergence, word_contribs) where the first
   *   value is the actual KL-divergence and the second is the map
   *   of word contributions as described above; will be null if
   *   not requested.
   */
  def slow_kl_divergence_debug(xother: WordDist, partial: Boolean = false,
      return_contributing_words: Boolean = false):
      (Double, collection.Map[Word, Double])

  /**
   * Compute the KL-divergence using the "slow" algorithm of
   * #slow_kl_divergence_debug, but without requesting or returning debug
   * info.
   */
  def slow_kl_divergence(other: WordDist, partial: Boolean = false) = {
    val (kldiv, contribs) =
      slow_kl_divergence_debug(other, partial, false)
    kldiv
  }

  /**
   * A fast, optimized implementation of KL-divergence.  See the discussion in
   * `slow_kl_divergence_debug`.
   */
  def fast_kl_divergence(other: WordDist, partial: Boolean = false): Double

  /**
   * Check fast and slow KL-divergence versions against each other.
   */
  def test_kl_divergence(other: WordDist, partial: Boolean = false) = {
    val slow_kldiv = slow_kl_divergence(other, partial)
    val fast_kldiv = fast_kl_divergence(other, partial)
    if (abs(fast_kldiv - slow_kldiv) > 1e-8) {
      errprint("Fast KL-div=%s but slow KL-div=%s", fast_kldiv, slow_kldiv)
      assert(fast_kldiv == slow_kldiv)
    }
    fast_kldiv
  }

  /**
   * The actual kl_divergence implementation.  The value `test_kldiv`
   * below can be set to true to compare fast and slow against either
   * other, throwing an assertion failure if they are more than a very
   * small amount different (the small amount rather than 0 to account for
   * possible rounding error).
   */
  protected def imp_kl_divergence(other: WordDist, partial: Boolean) = {
    val test_kldiv = false
    if (test_kldiv)
      test_kl_divergence(other, partial)
    else
      fast_kl_divergence(other, partial)
  }
}

/**
 * A class that controls how to construct a word distribution, whether the
 * source comes from a data file, a document, another distribution, etc.
 * The actual words added can be transformed in various ways, e.g.
 * case-folding the words (typically by converting to all lowercase), ignoring
 * words seen in a stoplist, converting some words to a generic -OOV-,
 * eliminating words seen less than a minimum nmber of times, etc.
 */
abstract class WordDistConstructor(factory: WordDistFactory) {
  /**
   * Actual implementation of `add_document` by subclasses.
   * External callers should use `add_document`.
   */
  protected def imp_add_document(dist: WordDist, words: Traversable[String])

  /**
   * Actual implementation of `add_word_distribution` by subclasses.
   * External callers should use `add_word_distribution`.
   */
  protected def imp_add_word_distribution(dist: WordDist, other: WordDist,
    partial: Double = 1.0)

  /**
   * Actual implementation of `add_keys_values` by subclasses.
   * External callers should use `add_keys_values`.
   */
  protected def imp_add_keys_values(dist: WordDist,
      keys: Array[String], values: Array[Int], num_words: Int)

  /**
   * Actual implementation of `finish_before_global` by subclasses.
   * External callers should use `finish_before_global`.
   */
  protected def imp_finish_before_global(dist: WordDist)

  /**
   * Incorporate a document into the distribution.
   */
  def add_document(dist: WordDist, words: Traversable[String]) {
    assert(!dist.finished)
    assert(!dist.finished_before_global)
    imp_add_document(dist, words)
  }

  /**
   * Incorporate the given distribution into our distribution.
   * `partial` is a scaling factor (between 0.0 and 1.0) used for
   * interpolating multiple distributions.
   */
  def add_word_distribution(dist: WordDist, other: WordDist,
      partial: Double = 1.0) {
    assert(!dist.finished)
    assert(!dist.finished_before_global)
    assert(partial >= 0.0 && partial <= 1.0)
    imp_add_word_distribution(dist, other, partial)
  }

  /**
   * Incorporate a set of (key, value) pairs into the distribution.
   * The number of pairs to add should be taken from `num_words`, not from
   * the actual length of the arrays passed in.  The code should be able
   * to handle the possibility that the same word appears multiple times,
   * adding up the counts for each appearance of the word.
   */
  def add_keys_values(dist: WordDist,
      keys: Array[String], values: Array[Int], num_words: Int) {
    assert(!dist.finished)
    assert(!dist.finished_before_global)
    assert(keys.length >= num_words)
    assert(values.length >= num_words)
    imp_add_keys_values(dist, keys, values, num_words)
  }

  /**
   * Partly finish computation of distribution.  This is called when the
   * distribution has been completely populated with words, and no more
   * modifications (e.g. incorporation of words or other distributions) will
   * be made to the distribution.  It should do any additional changes that
   * depend on the distribution being complete, but which do not depend on
   * the global word-distribution statistics having been computed. (These
   * statistics can be computed only after *all* word distributions that
   * are used to create these global statistics have been completely
   * populated.)
   *
   * @see #finish_after_global
   * 
   */
  def finish_before_global(dist: WordDist) {
    assert(!dist.finished)
    assert(!dist.finished_before_global)
    imp_finish_before_global(dist)
    dist.finished_before_global = true
  }

  /**
   * Create the word distribution of a document, given the value of the field
   * describing the distribution (typically called "counts" or "text").
   *
   * @param doc Document to set the distribution of.
   * @param diststr String from the document file, describing the distribution.
   * @param is_training_set True if this document is in the training set.
   *   Generally, global (e.g. back-off) statistics should be initialized
   *   only from training-set documents.
   */
  def initialize_distribution(doc: GenericDistDocument, countstr: String,
      is_training_set: Boolean)
}

/**
 * A factory object for WordDists (word distributions).  Currently, there is
 * only one factory object in a particular instance of the application
 * (i.e. it's a singleton), but the particular factory used depends on a
 * command-line parameter.
 */
abstract class WordDistFactory {
  /**
   * Total number of word types seen (size of vocabulary)
   */
  var total_num_word_types = 0

  /**
   * Total number of word tokens seen
   */
  var total_num_word_tokens = 0

  /**
   * Corresponding constructor object for building up the word distribution
   */
  var constructor: WordDistConstructor = _

  def set_word_dist_constructor(constructor: WordDistConstructor) {
    this.constructor = constructor
  }

  def create_word_dist(): WordDist = create_word_dist(note_globally = false)

  /**
   * Create an empty word distribution.  If `note_globally` is true,
   * the distribution is meant to be added to the global word-distribution
   * statistics (see below).
   */
  def create_word_dist(note_globally: Boolean): WordDist

  /**
   * Add the given distribution to the global word-distribution statistics,
   * if any.
   */
  def note_dist_globally(dist: WordDist) { }

  /**
   * Finish computing any global word-distribution statistics, e.g. tables for
   * doing back-off.  This is called after all of the relevant WordDists
   * have been created.  In practice, the "relevant" distributions are those
   * associated with training documents, which are read in
   * during `read_word_counts`.
   */
  def finish_global_distribution()
}

object WordDist {
  /**
   * Object describing how we memoize words (i.e. convert them to Int
   * indices, for faster operations on them).
   *
   * FIXME: Should probably be stored globally or at least elsewhere, since
   * memoization is more general than just for words in word distributions,
   * and is used elsewhere in the code for other things.  We should probably
   * move the memoization code into the `util` package.
   */
  val memoizer = new IntStringMemoizer
  //val memoizer = IdentityMemoizer
}

/**
 * A word distribution, i.e. a statistical distribution over words in
 * a document, cell, etc.
 */
abstract class WordDist(factory: WordDistFactory, val note_globally: Boolean) {
  /** Number of word tokens seen in the distribution. */
  var num_word_tokens: Double

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
   * Whether we have already called `finish_before_global`.  If so, this
   * means we can't modify the distribution any more.
   */
  var finished_before_global = false

  /**
   * Incorporate a document into the distribution.
   */
  def add_document(words: Traversable[String]) {
    factory.constructor.add_document(this, words)
  }

  /**
   * Incorporate the given distribution into our distribution.
   * `partial` is a scaling factor (between 0.0 and 1.0) used for
   * interpolating multiple distributions.
   */
  def add_word_distribution(other: WordDist, partial: Double = 1.0) {
    factory.constructor.add_word_distribution(this, other, partial)
  }

  /**
   * Incorporate a set of (key, value) pairs into the distribution.
   * The number of pairs to add should be taken from `num_words`, not from
   * the actual length of the arrays passed in.  The code should be able
   * to handle the possibility that the same word appears multiple times,
   * adding up the counts for each appearance of the word.
   */
  def add_keys_values(keys: Array[String], values: Array[Int],
      num_words: Int) {
    factory.constructor.add_keys_values(this, keys, values, num_words)
  }

  /**
   * Partly finish computation of distribution.  This is called when the
   * distribution has been completely populated with words, and no more
   * modifications (e.g. incorporation of words or other distributions) will
   * be made to the distribution.  It should do any additional changes that
   * depend on the distribution being complete, but which do not depend on
   * the global word-distribution statistics having been computed. (These
   * statistics can be computed only after *all* word distributions that
   * are used to create these global statistics have been completely
   * populated.)
   *
   * @see #finish_after_global
   * 
   */
  def finish_before_global() {
    factory.constructor.finish_before_global(this)
  }

  /**
   * Actual implementation of `finish_after_global` by subclasses.
   * External callers should use `finish_after_global`.
   */
  protected def imp_finish_after_global()

  /**
   * Completely finish computation of the word distribution.  This is called
   * after finish_global_distribution() on the factory method, and can be
   * used to compute values for the distribution that depend on the
   * global word-distribution statistics.
   */
  def finish_after_global() {
    assert(!finished)
    assert(finished_before_global)
    imp_finish_after_global()
    finished = true
  }

  /**
   * Actual implementation of `kl_divergence` by subclasses.
   * External callers should use `kl_divergence`.
   */
  protected def imp_kl_divergence(other: WordDist, partial: Boolean): Double

  /**
   * Compute the KL-divergence between this distribution and another
   * distribution.
   * 
   * @param other The other distribution to compute against.
   * @param partial If true, only compute the contribution involving words
   *   that exist in our distribution; otherwise we also have to take
   *   into account words in the other distribution even if we haven't
   *   seen them, and often also (esp. in the presence of smoothing) the
   *   contribution of all other words in the vocabulary.
   *   
   * @return The KL-divergence value.
   */
  def kl_divergence(other: WordDist, partial: Boolean = false) = {
    assert(finished)
    assert(other.finished)
    imp_kl_divergence(other, partial)
  }

  /**
   * Compute the symmetric KL-divergence between two distributions by averaging
   * the respective one-way KL-divergences in each direction.
   * 
   * @param partial Same as in `kl_divergence`.
   */
  def symmetric_kldiv(other: WordDist, partial: Boolean = false) = {
    0.5*this.kl_divergence(other, partial) +
    0.5*this.kl_divergence(other, partial)
  }

  /**
   * Implementation of the cosine similarity between this and another
   * distribution, using either unsmoothed or smoothed probabilities.
   *
   * @param partial Same as in `kl_divergence`.
   * @param smoothed If true, use smoothed probabilities, if smoothing exists;
   *   otherwise, do unsmoothed.
   */
  def cosine_similarity(other: WordDist, partial: Boolean = false,
    smoothed: Boolean = false): Double

  /**
   * For a document described by its distribution 'worddist', return the
   * log probability log p(worddist|other worddist) using a Naive Bayes
   * algorithm.
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
   * @return Most common word matching the predicate (wrapped with
   *   Some()), or None if no match.
   * 
   * FIXME: Probably should be moved similar to `lookup_word`.
   */
  def find_most_common_word(pred: String => Boolean): Option[Word] 
}
