///////////////////////////////////////////////////////////////////////////////
//  NgramLangModel.scala
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

import NgramStorage.RawNgram

/**
 * Normal version of memoizer which maps words to Ints.
 */
trait StringArrayGramAsIntMemoizer extends GramAsIntMemoizer[RawNgram] {
}

object Ngram extends StringArrayGramAsIntMemoizer {
  def check_ngram_lang_model(lang_model: LangModel) = {
    lang_model match {
      case x: NgramLangModel => x
      case _ => throw new IllegalArgumentException("You must use an ngram language model for these parameters")
    }
  }
}

object NgramStorage {
  type RawNgram = Array[String]
}

/**
 * An interface for storing and retrieving ngrams.
 */
trait NgramStorage extends GramStorage {
}

/**
 * An implementation for storing and retrieving ngrams using OpenNLP.
 */
class OpenNLPNgramStorer extends NgramStorage {
  import opennlp.tools.ngram._
  import opennlp.tools.util.StringList

  val model = new NGramModel()

  protected def get_sl_ngram(ngram: Gram) =
    new StringList(Ngram.unmemoize(ngram).toSeq: _*)

  /**************************** Abstract functions ***********************/

  /**
   * Add an n-gram with the given count.  If the n-gram exists already,
   * add the count to the existing value.
   */
  def add_gram(ngram: Gram, count: GramCount) {
    if (count != count.toInt)
      throw new IllegalArgumentException(
        "Partial count %s not allowed in this class" format count)
    val sl_ngram = get_sl_ngram(ngram)
    /* OpenNLP only lets you either add 1 to a possibly non-existing n-gram
       or set the count of an existing n-gram. */
    model.add(sl_ngram)
    if (count != 1) {
      val existing_count = model.getCount(sl_ngram)
      model.setCount(sl_ngram, existing_count + count.toInt - 1)
    }
  }

  def set_gram(ngram: Gram, count: GramCount) {
    if (count != count.toInt)
      throw new IllegalArgumentException(
        "Partial count %s not allowed in this class" format count)
    val sl_ngram = get_sl_ngram(ngram)
    /* OpenNLP only lets you either add 1 to a possibly non-existing n-gram
       or set the count of an existing n-gram. */
    model.add(sl_ngram)
    model.setCount(sl_ngram, count.toInt)
  }

  /**
   * Remove an n-gram, if it exists.
   */
  def remove_gram(ngram: Gram) {
    val sl_ngram = get_sl_ngram(ngram)
    model.remove(sl_ngram)
  }

  /**
   * Return whether a given n-gram is stored.
   */
  def contains(ngram: Gram) = {
    val sl_ngram = get_sl_ngram(ngram)
    model.contains(sl_ngram)
  }

  /**
   * Return whether a given n-gram is stored.
   */
  def get_gram(ngram: Gram) = {
    val sl_ngram = get_sl_ngram(ngram)
    model.getCount(sl_ngram)
  }

  /**
   * Iterate over all n-grams that are stored.
   */
  def iter_keys = {
    import collection.JavaConversions._
    // Iterators suck.  Should not be exposed directly.
    // Actually you can iterate over the model without using `iterator`
    // but it doesn't appear to work right -- it generates the entire
    // list before iterating over it.  Doing it as below iterates over
    // the list as it's generated.
    for (x <- model.iterator.toIterable)
      yield Ngram.memoize(x.iterator.toArray)
  }

  /**
   * Iterate over all n-grams that are stored.
   */
  def iter_grams = {
    import collection.JavaConversions._
    // Iterators suck.  Should not be exposed directly.
    // Actually you can iterate over the model without using `iterator`
    // but it doesn't appear to work right -- it generates the entire
    // list before iterating over it.  Doing it as below iterates over
    // the list as it's generated.
    for (x <- model.iterator.toIterable)
      yield (Ngram.memoize(x.iterator.toArray),
        model.getCount(x).toDouble)
  }

  /**
   * Total number of tokens stored.
   */
  def num_tokens = model.numberOfGrams.toDouble

  /**
   * Total number of n-gram types (i.e. number of distinct n-grams)
   * stored for n-grams of size `len`.
   */
  def num_types = model.size
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
//   * FIXME: Currently we store ngrams using a Gram, which is simply an index
//   * into a string table, where the ngram is shoehorned into a string using
//   * the format of the TextGrounder corpus (words separated by colons, with
//   * URL-encoding for embedded colons).  This is very wasteful of space,
//   * and inefficient too with extra encodings/decodings.  We need a better
//   * implementation with a trie and such.
//   */
//  var counts = Vector(null, create_gram_double_map)
//  var num_tokens = Vector(0.0, 0.0)
//  def max_ngram_size = counts.length - 1
//  // var num_types = Vector(0)
//  def num_types(n: Int) = counts(n).size
//
//  def num_word_tokens = num_tokens(1)
//  def num_word_types = num_types(1)
//
//  /**
//   * Ensure that we will be able to store an N-gram for a given N.
//   */
//  def ensure_ngram_fits(n: Int) {
//    assert(counts.length == num_tokens.length)
//    while (n > max_ngram_size) {
//      counts :+= create_gram_double_map
//      num_tokens :+= 0.0
//    }
//  }
//
//  /**
//   * Record an n-gram (encoded into a string) and associated count.
//   */
//  def record_encoded_ngram(egram: String, count: GramCount) {
//    val n = egram.count(_ == ':') + 1
//    val mgram = memoizer.memoize(egram)
//    ensure_ngram_fits(n)
//    counts(n)(mgram) += count
//    num_tokens(n) += count
// }
//
//}

/**
 * Basic n-gram language model with tables listing counts for each n-gram.
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
 * Some terminology (FIXME: Outdated):
 *
 * Name                             Short name   Scala type
 * -------------------------------------------------------------------------
 * Unencoded, unmemoized ngram      ngram        Iterable[String]
 * Unencoded ngram, memoized words  mwgram       Iterable[Gram]
 * Encoded, unmemoized ngram        egram        String
 * Encoded, memoized ngram          mgram        Gram
 *
 * Note that "memoizing", in its general sense, simply converts an ngram
 * into a single number (of type "Gram") to identify the ngram.  There is
 * no theoretical reason why we have to encode the ngram into a single string
 * and then "memoize" the encoded string in this fashion.  Memoizing could
 * involve e.g. creating some sort of trie, with a pointer to the appropriate
 * memory location (or index) in the trie serving as the value returned back
 * after memoization.
 * 
 * @param factory A `LangModelFactory` object used to create this lang model.
 *   The object also stores global properties of various sorts (e.g. for
 *   smothing).
 */

abstract class NgramLangModel(
    factory: LangModelFactory
  ) extends LangModel(factory) with FastSlowKLDivergence {
  val model = new OpenNLPNgramStorer

  def gram_to_string(gram: Gram) = Ngram.unmemoize(gram) mkString " "

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
   * 3. Words in neither lang model but seen in the global backoff stats.
   * 4. Words never seen at all.
   * 
   * The computation of steps 3 and 4 depends heavily on the particular
   * smoothing algorithm; in the absence of smoothing, these steps
   * contribute nothing to the overall KL-divergence.
   *
   */
  def slow_kl_divergence_debug(other: LangModel, partial: Boolean = true,
      return_contributing_words: Boolean = false):
      (Double, collection.Map[String, GramCount]) = ???

  /**
   * Steps 3 and 4 of KL-divergence computation.
   * @see #slow_kl_divergence_debug
   */
  def kl_divergence_34(other: NgramLangModel): Double
  
  def get_most_contributing_grams(langmodel: LangModel,
      relative_to: Iterable[LangModel] = Iterable()) = ???
}

/**
 * Class for building an n-gram language model from a corpus or other
 * data source.
 *
 * @param factory Factory object for creating lang models.
 * @param ignore_case Whether to fold the case of all words to lowercase.
 * @param stopwords List of words to ignore (FIXME: Not implemented)
 * @param whitelist List of words to allow; if specified, all others are
 *   ignored. (FIXME: Not implemented)
 * @param minimum_word_count Minimum count for a given n-gram. N-grams
 *   with a lower count after the lang model is complete will be
 *   replaced with the generic token -OOV- ("out of vocabulary").
 * @param max_ngram Maximum n-gram size to allow. This is a filter that
 *   operates on n-grams read directly from a corpus. If 0, allow
 *   all n-grams.
 * @param raw_text_max_ngram Maximum n-gram size to create, when creating
 *   n-grams from raw text (rather than reading from a pre-chunked
 *   corpus).
 */
class DefaultNgramLangModelBuilder(
  factory: LangModelFactory,
  ignore_case: Boolean,
  stopwords: Set[String],
  whitelist: Set[String],
  minimum_word_count: Int = 1,
  max_ngram: Int = 0,
  raw_text_max_ngram: Int = 3
) extends LangModelBuilder(factory: LangModelFactory) {
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
      val (egram, count) = textdb.shallow_split_count_map_field(ngramcount)
      if (parsed_ngrams contains egram)
        throw FileFormatException(
          "Ngram %s seen twice in same counts list" format egram)
      parsed_ngrams(egram) = count
    }
  }

  /**
   * Returns true if the n-gram was counted, false if it was ignored (e.g.
   * due to length restrictions, stoplisting or whitelisting). */
  protected def add_ngram_with_count(lm: NgramLangModel,
      ngram: Iterable[String], count: GramCount) = {
    if (max_ngram > 0 && ngram.size > max_ngram)
      false
    else {
      val lgram = maybe_lowercase(ngram)
      // FIXME: Not right with stopwords or whitelist
      //if (!stopwords.contains(lgram) &&
      //    (whitelist.size == 0 || whitelist.contains(lgram))) {
        lm.add_gram(Ngram.memoize(lgram.toArray), count)
        true
      //}
      //else
      //  false
    }
  }

  protected def imp_add_document(gendist: LangModel,
      words: Iterable[String], raw_text_max_ngram: Int) {
    val lm = gendist.asInstanceOf[NgramLangModel]
    for (ngram <- (1 to raw_text_max_ngram).flatMap(words.sliding(_)))
      add_ngram_with_count(lm, ngram, 1)
  }

  protected def imp_add_document(gendist: LangModel,
      words: Iterable[String]) {
    imp_add_document(gendist, words, raw_text_max_ngram)
  }

  protected def imp_add_language_model(gendist: LangModel,
      genother: LangModel, partial: GramCount) {
    // FIXME: Implement partial!
    val lm = gendist.asInstanceOf[NgramLangModel]
    val other = genother.asInstanceOf[NgramLangModel]
    for ((ngram, count) <- other.iter_grams)
      // FIXME: Possible slowdown because we are lowercasing a second
      // time when it's probably already been done. Currently done this
      // way rather than directly calling lm.add_gram() to
      // check for --max-ngram and similar restrictions, just in case
      // they were done differently in the source lang model.
      add_ngram_with_count(lm, Ngram.unmemoize(ngram), count)
  }

  /**
   * Incorporate a set of (key, value) pairs into the lang model.
   * The number of pairs to add should be taken from `num_ngrams`, not from
   * the actual length of the arrays passed in.  The code should be able
   * to handle the possibility that the same ngram appears multiple times,
   * adding up the counts for each appearance of the ngram.
   */
  protected def add_parsed_ngrams(gendist: LangModel,
      grams: collection.Map[String, Int]) {
    val lm = gendist.asInstanceOf[NgramLangModel]
    assert(!lm.finished)
    assert(!lm.finished_before_global)
    var addedTypes = 0
    var addedTokens = 0
    var totalTokens = 0
    for ((egram, count) <- grams) {
      val ngram = textdb.decode_ngram_for_map_field(egram)
      if (add_ngram_with_count(lm, ngram, count)) {
        addedTypes += 1
        addedTokens += count
      }
      totalTokens += count
    }
    // Way too much output to keep enabled
    //errprint("Fraction of word types kept:"+(addedTypes.toDouble/num_ngrams))
    //errprint("Fraction of word tokens kept:"+(addedTokens.toDouble/totalTokens))
  } 

  def finish_before_global(lm: LangModel) {
    // A table listing OOV ngrams, e.g. Seq(OOV), Seq(OOV, OOV), etc.
    val oov_hash = mutable.Map[Int, Gram]()

    // If 'minimum_word_count' was given, then eliminate n-grams whose count
    // is too small by replacing them with -OOV-.
    // FIXME!!! This should almost surely operate at the word level, not the
    // n-gram level.
    if (minimum_word_count > 1) {
      for ((ngram, count) <- lm.iter_grams if count < minimum_word_count) {
        lm.remove_gram(ngram)
        val siz = Ngram.unmemoize(ngram).size
        val oov = oov_hash.getOrElse(siz, {
          val newoov =
            Ngram.memoize((1 to siz).map(_ => "-OOV-").toArray)
          oov_hash(siz) = newoov
          newoov
        } )
        lm.add_gram(oov, count)
      }
    }
  }

  def maybe_lowercase(ngram: Iterable[String]) =
    if (ignore_case) ngram.map(_ toLowerCase) else ngram

  def create_lang_model(countstr: String) = {
    parse_counts(countstr)
    // Now set the lang model on the document.
    val lm = factory.create_lang_model
    add_parsed_ngrams(lm, parsed_ngrams)
    lm
  }
}

/**
 * General factory for NgramLangModel lang models.
 */ 
abstract class NgramLangModelFactory extends LangModelFactory { }
