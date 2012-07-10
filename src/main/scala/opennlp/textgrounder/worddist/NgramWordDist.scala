///////////////////////////////////////////////////////////////////////////////
//  NgramWordDist.scala
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

package opennlp.textgrounder.worddist

import math._
import collection.mutable
import util.control.Breaks._

import java.io._

import opennlp.textgrounder.util.collectionutil.DynamicArray
import opennlp.textgrounder.util.ioutil.{FileHandler, FileFormatException}
import opennlp.textgrounder.util.printutil.{errprint, warning}

import opennlp.textgrounder.gridlocate.GridLocateDriver.Debug._
import opennlp.textgrounder.gridlocate.GenericTypes._
import opennlp.textgrounder.gridlocate.DistDocument

import WordDist.memoizer._

object NgramStorage {
  type Ngram = Iterable[String]
}

/**
 * An interface for storing and retrieving ngrams.
 */
trait NgramStorage {
  type Ngram = NgramStorage.Ngram
  /**************************** Abstract functions ***********************/

  /**
   * Add an n-gram with the given count.  If the n-gram exists already,
   * add the count to the existing value.
   */
  def add_ngram(ngram: Ngram, count: Int)

  /**
   * Remove an n-gram, if it exists.
   */
  def remove_ngram(ngram: Ngram)

  /**
   * Return whether a given n-gram is stored.
   */
  def contains(ngram: Ngram): Boolean

  /**
   * Return the count of a given n-gram.
   */
  def get_ngram_count(ngram: Ngram): Int

  /**
   * Iterate over all n-grams that are stored.
   */
  def iter_ngrams: Iterable[(Ngram, Int)]

  /**
   * Total number of tokens stored.
   */
  def num_tokens: Long

  /**
   * Total number of n-gram types (i.e. number of distinct n-grams)
   * stored.
   */
  def num_types: Long
}

/**
 * An implementation for storing and retrieving ngrams using OpenNLP.
 */
class OpenNLPNgramStorer extends NgramStorage {

  import opennlp.tools.ngram._
  import opennlp.tools.util.StringList

  val model = new NGramModel()

  /**************************** Abstract functions ***********************/

  /**
   * Add an n-gram with the given count.  If the n-gram exists already,
   * add the count to the existing value.
   */
  def add_ngram(ngram: Ngram, count: Int) {
    val sl_ngram = new StringList(ngram.toSeq: _*)
    /* OpenNLP only lets you either add 1 to a possibly non-existing n-gram
       or set the count of an existing n-gram. */
    model.add(sl_ngram)
    if (count != 1) {
      val existing_count = model.getCount(sl_ngram)
      model.setCount(sl_ngram, existing_count + count - 1)
    }
  }

  /**
   * Remove an n-gram, if it exists.
   */
  def remove_ngram(ngram: Ngram) {
    val sl_ngram = new StringList(ngram.toSeq: _*)
    model.remove(sl_ngram)
  }

  /**
   * Return whether a given n-gram is stored.
   */
  def contains(ngram: Ngram) = {
    val sl_ngram = new StringList(ngram.toSeq: _*)
    model.contains(sl_ngram)
  }

  /**
   * Return whether a given n-gram is stored.
   */
  def get_ngram_count(ngram: Ngram) = {
    val sl_ngram = new StringList(ngram.toSeq: _*)
    model.getCount(sl_ngram)
  }

  /**
   * Iterate over all n-grams that are stored.
   */
  def iter_ngrams: Iterable[(Ngram, Int)] = {
    import collection.JavaConversions._
    // Iterators suck.  Should not be exposed directly.
    // Actually you can iterate over the model without using `iterator`
    // but it doesn't appear to work right -- it generates the entire
    // list before iterating over it.  Doing it as below iterates over
    // the list as it's generated.
    for (x <- model.iterator.toIterable)
      yield (x.iterator.toIterable, model.getCount(x))
  }

  /**
   * Total number of tokens stored.
   */
  def num_tokens: Long = model.numberOfGrams

  /**
   * Total number of n-gram types (i.e. number of distinct n-grams)
   * stored for n-grams of size `len`.
   */
  def num_types: Long = model.size
}

/**
 * An implementation for storing and retrieving ngrams using OpenNLP.
 */
//class SimpleNgramStorer extends NgramStorage {
//
//  /**
//   * A sequence of separate maps (or possibly a "sorted list" of tuples,
//   * to save memory?) of (ngram, count) items, specifying the counts of
//   * all ngrams seen at least once.  Each map contains the ngrams for
//   * a particular value of N.  The map for N = 0 is unused.
//   *
//   * These are given as double because in some cases they may store "partial"
//   * counts (in particular, when the K-d tree code does interpolation on
//   * cells).  FIXME: This seems ugly, perhaps there is a better way?
//   *
//   * FIXME: Currently we store ngrams using a Word, which is simply an index
//   * into a string table, where the ngram is shoehorned into a string using
//   * the format of the TextGrounder corpus (words separated by colons, with
//   * URL-encoding for embedded colons).  This is very wasteful of space,
//   * and inefficient too with extra encodings/decodings.  We need a better
//   * implementation with a trie and such.
//   */
//  var counts = Vector(null, create_word_double_map())
//  var num_tokens = Vector(0.0, 0.0)
//  def max_ngram_size = counts.length - 1
//  // var num_types = Vector(0)
//  def num_types(n: Int) = counts(n).size
//
//  def num_word_tokens = num_tokens(1)
//  def num_word_types = num_types(1)
//
//  def innerToString: String
//
//  /**
//   * Ensure that we will be able to store an N-gram for a given N.
//   */
//  def ensure_ngram_fits(n: Int) {
//    assert(counts.length == num_tokens.length)
//    while (n > max_ngram_size) {
//      counts :+= create_word_double_map()
//      num_tokens :+= 0.0
//    }
//  }
//
//  /**
//   * Record an n-gram (encoded into a string) and associated count.
//   */
//  def record_encoded_ngram(egram: String, count: Int) {
//    val n = egram.count(_ == ':') + 1
//    val mgram = memoize_string(egram)
//    ensure_ngram_fits(n)
//    counts(n)(mgram) += count
//    num_tokens(n) += count
// }
//
//}

/**
 * Basic n-gram word distribution with tables listing counts for each n-gram.
 * This is an abstract class because the smoothing algorithm (how to return
 * probabilities for a given n-gram) isn't specified.  This class takes care
 * of storing the n-grams.
 *
 * FIXME: For unigrams storage is less of an issue, but for n-grams we
 * may have multiple storage implementations, potentially swappable (i.e.
 * we can specify them separately from e.g. the smoothing method of other
 * properties).  This suggests that at some point we may find it useful
 * to outsource the storage implementation/management to another class.
 *
 * Some terminology:
 *
 * Name                             Short name   Scala type
 * -------------------------------------------------------------------------
 * Unencoded, unmemoized ngram      ngram        Iterable[String]
 * Unencoded ngram, memoized words  mwgram       Iterable[Word]
 * Encoded, unmemoized ngram        egram        String
 * Encoded, memoized ngram          mgram        Word
 *
 * Note that "memoizing", in its general sense, simply converts an ngram
 * into a single number (of type "Word") to identify the ngram.  There is
 * no theoretical reason why we have to encode the ngram into a single string
 * and then "memoize" the encoded string in this fashion.  Memoizing could
 * involve e.g. creating some sort of trie, with a pointer to the appropriate
 * memory location (or index) in the trie serving as the value returned back
 * after memoization.
 * 
 * @param factory A `WordDistFactory` object used to create this distribution.
 *   The object also stores global properties of various sorts (e.g. for
 *   smothing).
 * @param note_globally Whether n-grams added to this distribution should
 *   have an effect on the global statistics stored in the factory.
 */

abstract class NgramWordDist(
    factory: WordDistFactory,
    note_globally: Boolean
  ) extends WordDist(factory, note_globally) with FastSlowKLDivergence {
  import NgramStorage.Ngram

  val model = new OpenNLPNgramStorer

  def num_word_types = model.num_types
  def num_word_tokens = model.num_tokens

  def innerToString: String

  override def toString = {
    val finished_str =
      if (!finished) ", unfinished" else ""
    val num_words_to_print = 15
    val need_dots = model.num_types > num_words_to_print
    val items =
      for ((word, count) <- (model.iter_ngrams.toSeq.sortWith(_._2 > _._2).
                                   view(0, num_words_to_print)))
        yield "%s=%s" format (word mkString " ", count) 
    val words = (items mkString " ") + (if (need_dots) " ..." else "")
    "NgramWordDist(%d types, %s tokens%s%s, %s)" format (
        model.num_types, model.num_tokens, innerToString, finished_str, words)
  }

  def lookup_ngram(ngram: Ngram): Double

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
      return_contributing_words: Boolean = false):
      (Double, collection.Map[String, Double]) = {
    assert(false, "Not implemented")
    (0.0, null)
  }

  /**
   * Steps 3 and 4 of KL-divergence computation.
   * @see #slow_kl_divergence_debug
   */
  def kl_divergence_34(other: NgramWordDist): Double
  
  def get_nbayes_logprob(xworddist: WordDist) = {
    assert(false, "Not implemented")
    0.0
  }

  def find_most_common_word(pred: String => Boolean): Option[Word] = {
    assert(false, "Not implemented")
    None
  }
}

/**
 * Class for constructing an n-gram word distribution from a corpus or other
 * data source.
 */
class DefaultNgramWordDistConstructor(
  factory: WordDistFactory,
  ignore_case: Boolean,
  stopwords: Set[String],
  whitelist: Set[String],
  minimum_word_count: Int = 1,
  max_ngram_length: Int = 3
) extends WordDistConstructor(factory: WordDistFactory) {
  import NgramStorage.Ngram
  /**
   * Internal map holding the encoded ngrams and counts.
   */
  protected val parsed_ngrams = mutable.Map[String, Int]()

  /**
   * Given a field containing the encoded representation of the n-grams of a
   * document, parse and store internally.
   */
  protected def parse_counts(countstr: String) {
    parsed_ngrams.clear()
    val ngramcounts = countstr.split(" ")
    for (ngramcount <- ngramcounts) yield {
      val (egram, count) = DistDocument.shallow_split_counts_field(ngramcount)
      if (parsed_ngrams contains egram)
        throw FileFormatException(
          "Ngram %s seen twice in same counts list" format egram)
      parsed_ngrams(egram) = count
    }
  }

  var seen_documents = new scala.collection.mutable.HashSet[String]()

  /**
   * Returns true if the n-gram was counted, false if it was ignored due to
   * stoplisting and/or whitelisting. */
  protected def add_ngram_with_count(dist: NgramWordDist,
      ngram: Ngram, count: Int): Boolean = {
    val lgram = maybe_lowercase(ngram)
    // FIXME: Not right with stopwords or whitelist
    //if (!stopwords.contains(lgram) &&
    //    (whitelist.size == 0 || whitelist.contains(lgram))) {
    //  dist.add_ngram(lgram, count)
    //  true
    //}
    //else
    //  false
    dist.model.add_ngram(lgram, count)
    return true
  }

  protected def imp_add_document(gendist: WordDist,
      words: Iterable[String], max_ngram_length: Int) {
    val dist = gendist.asInstanceOf[NgramWordDist]
    for (ngram <- (1 to max_ngram_length).flatMap(words.sliding(_)))
      add_ngram_with_count(dist, ngram, 1)
  }

  protected def imp_add_document(gendist: WordDist,
      words: Iterable[String]) {
    imp_add_document(gendist, words, max_ngram_length)
  }

  protected def imp_add_word_distribution(gendist: WordDist,
      genother: WordDist, partial: Double) {
    // FIXME: Implement partial!
    val dist = gendist.asInstanceOf[NgramWordDist].model
    val other = genother.asInstanceOf[NgramWordDist].model
    for ((ngram, count) <- other.iter_ngrams)
      dist.add_ngram(ngram, count)
  }

  /**
   * Incorporate a set of (key, value) pairs into the distribution.
   * The number of pairs to add should be taken from `num_ngrams`, not from
   * the actual length of the arrays passed in.  The code should be able
   * to handle the possibility that the same ngram appears multiple times,
   * adding up the counts for each appearance of the ngram.
   */
  protected def add_parsed_ngrams(gendist: WordDist,
      grams: collection.Map[String, Int]) {
    val dist = gendist.asInstanceOf[NgramWordDist]
    assert(!dist.finished)
    assert(!dist.finished_before_global)
    var addedTypes = 0
    var addedTokens = 0
    var totalTokens = 0
    for ((egram, count) <- grams) {
      val ngram = DistDocument.decode_ngram_for_counts_field(egram)
      if (add_ngram_with_count(dist, ngram, count)) {
        addedTypes += 1
        addedTokens += count
      }
      totalTokens += count
    }
    // Way too much output to keep enabled
    //errprint("Fraction of word types kept:"+(addedTypes.toDouble/num_ngrams))
    //errprint("Fraction of word tokens kept:"+(addedTokens.toDouble/totalTokens))
  } 

  protected def imp_finish_before_global(dist: WordDist) {
    val model = dist.asInstanceOf[NgramWordDist].model
    val oov = Seq("-OOV-")

    /* Add the distribution to the global stats before eliminating
       infrequent words. */
    factory.note_dist_globally(dist)

    // If 'minimum_word_count' was given, then eliminate n-grams whose count
    // is too small by replacing them with -OOV-.
    // FIXME!!! This should almost surely operate at the word level, not the
    // n-gram level.
    if (minimum_word_count > 1) {
      for ((ngram, count) <- model.iter_ngrams if count < minimum_word_count) {
        model.remove_ngram(ngram)
        model.add_ngram(oov, count)
      }
    }
  }

  def maybe_lowercase(ngram: Ngram) =
    if (ignore_case) ngram.map(_ toLowerCase) else ngram

  def initialize_distribution(doc: GenericDistDocument, countstr: String,
      is_training_set: Boolean) {
    parse_counts(countstr)
    // Now set the distribution on the document; but don't use the test
    // set's distributions in computing global smoothing values and such.
    //
    // FIXME: What is the purpose of first_time_document_seen??? When does
    // it occur that we see a document multiple times?
    var first_time_document_seen = !seen_documents.contains(doc.title)

    val dist = factory.create_word_dist(note_globally =
      is_training_set && first_time_document_seen)
    add_parsed_ngrams(dist, parsed_ngrams)
    seen_documents += doc.title
    doc.dist = dist
  }
}

/**
 * General factory for NgramWordDist distributions.
 */ 
abstract class NgramWordDistFactory extends WordDistFactory { }
