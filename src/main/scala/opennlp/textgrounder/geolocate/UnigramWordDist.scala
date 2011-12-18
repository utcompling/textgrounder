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
//////// UnigramWordDist.scala
////////
//////// Copyright (c) 2010, 2011 Ben Wing.
////////

package opennlp.textgrounder.geolocate

import math._
import collection.mutable
import util.control.Breaks._

import java.io._

import opennlp.textgrounder.util.collectionutil.DynamicArray
import opennlp.textgrounder.util.ioutil.{FileHandler, FileFormatException}
import opennlp.textgrounder.util.printutil.{errprint, warning}

import GridLocateDriver.Debug._
import WordDist.memoizer._
import GenericTypes._

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

abstract class UnigramWordDist extends WordDist with FastSlowKLDivergence {
  /**
   * A map (or possibly a "sorted list" of tuples, to save memory?) of
   * (word, count) items, specifying the counts of all words seen
   * at least once.  These are given as double because in some cases
   * they may store "partial" counts (in particular, when the K-d tree
   * code does interpolation on cells).  FIXME: This seems ugly, perhaps
   * there is a better way?
   */
  val counts = create_word_double_map()
  var num_word_tokens = 0.0

  /** SCALABUG:
      Heap analysis revealed that Scala has holding the keys and values
      (but not `num_words`) as local variables when they were constructors;
      doesn't seem a good idea.  By redoing it this way, we avoid the
      problem. */
  def this(keys: Array[String], values: Array[Int], num_words: Int) {
    this()
    for (i <- 0 until num_words)
      counts(memoize_string(keys(i))) = values(i)
    num_word_tokens = counts.values.sum
  }
  
  def num_word_types = counts.size

  def innerToString: String

  override def toString = {
    val finished_str =
      if (!finished) ", unfinished" else ""
    val num_words_to_print = 15
    val need_dots = counts.size > num_words_to_print
    val items =
      for ((word, count) <- counts.toSeq.sortWith(_._2 > _._2).view(0, num_words_to_print))
      yield "%s=%s" format (unmemoize_string(word), count) 
    val words = (items mkString " ") + (if (need_dots) " ..." else "")
    "UnigramWordDist(%d types, %d tokens%s%s, %s)" format (
        num_word_types, num_word_tokens, innerToString, finished_str, words)
  }

  protected def imp_add_document(words: Traversable[String],
      ignore_case: Boolean, stopwords: Set[String]) {
    for (word <- words;
         wlower = if (ignore_case) word.toLowerCase() else word;
         if !stopwords(wlower)) {
      counts(memoize_string(wlower)) += 1
      num_word_tokens += 1
    }
  }

  protected def imp_add_word_distribution(xworddist: WordDist) {
    val worddist = xworddist.asInstanceOf[UnigramWordDist]
    for ((word, count) <- worddist.counts)
      counts(word) += count
    num_word_tokens += worddist.num_word_tokens
  }

  protected def imp_finish_before_global(minimum_word_count: Int) {
    // make sure counts not null (eg document in coords file but not counts file)
    if (counts == null) return

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
   * @see #slow_kl_divergence_debug
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
      (for ((word, count) <- counts if pred(unmemoize_string(word)))
        yield (word, count)).toSeq
    if (filtered.length == 0) None
    else {
      val (maxword, maxcount) = filtered maxBy (_._2)
      Some(maxword)
    }
  }
}

/** FIXME: This stuff should be functions on the WordDist itself, not
    on the factory. */
trait SimpleUnigramWordDistReader extends WordDistReader {
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

  /**
   * Called each time a word is seen.  This can accept or reject the word
   * (e.g. based on whether the count is high enough or the word is in
   * a stopwords list), and optionally change the word into something else
   * (e.g. the lowercased version or a generic -OOV-).
   *
   * @param word Raw word seen
   * @param count Raw count for the word
   * @return A modified form of the word, or None to reject the word.
   */
  def canonicalize_accept_word(doc: GenericDistDocument,
    word: String, count: Int): Option[String]

  def parse_counts(doc: GenericDistDocument, countstr: String) {
    keys_dynarr.clear()
    values_dynarr.clear()
    raw_keys_set.clear()
    val wordcounts = countstr.split(" ")
    for (wordcount <- wordcounts) yield {
      val split_wordcount = wordcount.split(":", -1)
      if (split_wordcount.length != 2)
        throw FileFormatException(
          "For unigram counts, items must be of the form WORD:COUNT, but %s seen"
          format wordcount)
      val Array(word, strcount) = split_wordcount
      if (word.length == 0)
        throw FileFormatException(
          "For unigram counts, WORD in WORD:COUNT must not be empty, but %s seen"
          format wordcount)
      val count = strcount.toInt
      if (raw_keys_set contains word)
        throw FileFormatException(
          "Word %s seen twice in same counts list" format word)
      raw_keys_set += word
      val opt_canon_word =
        canonicalize_accept_word(doc,
          DistDocument.decode_word_for_counts_field(word), count)
      if (opt_canon_word != None) {
        keys_dynarr += opt_canon_word.get
        values_dynarr += count
      }
    }
  }

  def initialize_distribution(doc: GenericDistDocument, countstr: String,
      is_training_set: Boolean) {
    parse_counts(doc, countstr)
    // Now set the distribution on the document; but don't use the test
    // set's distributions in computing global smoothing values and such.
    set_unigram_word_dist(doc, keys_dynarr.array, values_dynarr.array,
      keys_dynarr.length, is_training_set)
  }

  def set_unigram_word_dist(doc: GenericDistDocument,
      keys: Array[String], values: Array[Int], num_words: Int,
      is_training_set: Boolean)
}

/**
 * General factory for UnigramWordDist distributions.
 */ 
abstract class UnigramWordDistFactory extends
    WordDistFactory with SimpleUnigramWordDistReader {
  def canonicalize_accept_word(doc: GenericDistDocument, raw_word: String,
      count: Int) = {
    val lword = maybe_lowercase(doc, raw_word)
    /* minimum_word_count (--minimum-word-count) currently handled elsewhere.
       FIXME: Perhaps should be handled here. */
    if (!is_stopword(doc, lword))
      Some(lword)
    else
      None
  }
}

