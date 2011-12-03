///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2011 Ben Wing, Thomas Darr, Andy Luong, Erik Skiles, The University of Texas at Austin
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

package opennlp.textgrounder.geolocate

import math._
import collection.mutable
import util.control.Breaks._

import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.ioutil.{FileHandler, FileFormatException}
import opennlp.textgrounder.util.printutil.{errprint, warning}

import GeolocateDriver.Params
import GeolocateDriver.Debug._
import WordDist.memoizer._
import GenericTypes._

/**
 * Bigram word distribution with a table listing counts for each word,
 * initialized from the given key/value pairs.
 *
 * @param key Array holding keys, possibly over-sized, so that the internal
 *   arrays from DynamicArray objects can be used
 * @param values Array holding values corresponding to each key, possibly
 *   oversize
 * @param num_words Number of actual key/value pairs to be stored 
 *   statistics.
 */

abstract class BigramWordDist(
  unigramKeys: Array[Word],
  unigramValues: Array[Int],
  num_unigrams: Int,
  bigramKeys: Array[Word],
  bigramValues: Array[Int],
  num_bigrams: Int
) extends WordDist {

  val unicounts = create_word_int_map()
  for (i <- 0 until num_unigrams)
    unicounts(unigramKeys(i)) = unigramValues(i)
  var num_word_tokens = unicounts.values.sum
  
  def num_word_types = unicounts.size

  def innerToString: String

  val bicounts = create_word_int_map()
  for (i <- 0 until num_bigrams)
    bicounts(bigramKeys(i)) = bigramValues(i)
  var num_bigram_tokens = bicounts.values.sum

  def num_bigram_types = bicounts.size

  /** Total probability mass to be assigned to all words not
      seen in the article, estimated (motivated by Good-Turing
      smoothing) as the unadjusted empirical probability of
      having seen a word once.
   */
  var unseen_mass = 0.5
  /**
     Probability mass assigned in 'overall_word_probs' to all words not seen
     in the article.  This is 1 - (sum over W in A of overall_word_probs[W]).
     The idea is that we compute the probability of seeing a word W in
     article A as

     -- if W has been seen before in A, use the following:
          COUNTS[W]/TOTAL_TOKENS*(1 - UNSEEN_MASS)
     -- else, if W seen in any articles (W in 'overall_word_probs'),
        use UNSEEN_MASS * (overall_word_probs[W] / OVERALL_UNSEEN_MASS).
        The idea is that overall_word_probs[W] / OVERALL_UNSEEN_MASS is
        an estimate of p(W | W not in A).  We have to divide by
        OVERALL_UNSEEN_MASS to make these probabilities be normalized
        properly.  We scale p(W | W not in A) by the total probability mass
        we have available for all words not seen in A.
     -- else, use UNSEEN_MASS * globally_unseen_word_prob / NUM_UNSEEN_WORDS,
        where NUM_UNSEEN_WORDS is an estimate of the total number of words
        "exist" but haven't been seen in any articles.  One simple idea is
        to use the number of words seen once in any article.  This certainly
        underestimates this number if not too many articles have been seen
        but might be OK if many articles seen.
    */
  var overall_unseen_mass = 1.0

  def add_document(words: Traversable[String], ignore_case: Boolean = true,
      stopwords: Set[String] = Set[String]()) {
errprint("add_document")
    assert(!finished)
    var previous = "<START>";
    unicounts(memoize_string(previous)) += 1
    for {word <- words
         val wlower = if (ignore_case) word.toLowerCase() else word
         if !stopwords(wlower) } {
      unicounts(memoize_string(wlower)) += 1
      num_word_tokens += 1
      bicounts(memoize_string(previous + "_" + wlower)) += 1
      previous = wlower
    }
  }

  def add_word_distribution(xworddist: WordDist) {
    assert(!finished)
    val worddist = xworddist.asInstanceOf[BigramWordDist]
    for ((word, count) <- worddist.unicounts)
      unicounts(word) += count
    for ((bigram, count) <- worddist.bicounts)
      bicounts(bigram) += count
    num_word_tokens += worddist.num_word_tokens
    num_bigram_tokens += worddist.num_bigram_tokens
if(debug("bigram"))
  errprint("add_word_distribution: "  + num_word_tokens + " " +  num_bigram_tokens)
  }

  def finish_before_global(minimum_word_count: Int = 0) {
    // make sure counts not null (eg article in coords file but not counts file)
    if (unicounts == null || bicounts == null || finished) return

    // If 'minimum_word_count' was given, then eliminate words whose count
    // is too small.
    if (minimum_word_count > 1) {
      for ((word, count) <- unicounts if count < minimum_word_count) {
        num_word_tokens -= count
        unicounts -= word
      }
      for ((bigram, count) <- bicounts if count < minimum_word_count) {
        num_bigram_tokens -= count
        bicounts -= bigram
      }
    }
  }
    /**
     * This is a basic unigram implementation of the computation of the
     * KL-divergence between this distribution and another distribution.
     * Useful for checking against other, faster implementations.
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
     * @param xother The other distribution to compute against.
     * @param partial If true, only do step 1 above.
     *   
     * @return A tuple of (divergence, word_contribs) where the first
     *   value is the actual KL-divergence and the second is the map
     *   of word contributions as described above; will be null if
     *   not requested.
     */
  def kl_divergence(xother: WordDist, partial: Boolean = false) = {
    val other = xother.asInstanceOf[BigramWordDist]
    assert(finished)
    assert(other.finished)
    var kldiv = 0.0
    //val contribs =
    //  if (return_contributing_words) mutable.Map[Word, Double]() else null
    // 1.
    for (word <- bicounts.keys) {
      val p = lookup_word(word)
      val q = other.lookup_word(word)
      if (p <= 0.0 || q <= 0.0)
        errprint("Warning: problematic values: p=%s, q=%s, word=%s", p, q, word)
      else {
        kldiv += p*(log(p) - log(q))
        if (debug("bigram"))
          errprint("kldiv1: " + kldiv + " :p: " + p + " :q: " + q)
        //if (return_contributing_words)
        //  contribs(word) = p*(log(p) - log(q))
      }
    }

    if (partial)
      kldiv
    else {
      // Step 2.
      for (word <- other.bicounts.keys if !(bicounts contains word)) {
        val p = lookup_bigram(word)
        val q = other.lookup_bigram(word)
        kldiv += p*(log(p) - log(q))
        if (debug("bigram"))
          errprint("kldiv2: " + kldiv + " :p: " + p + " :q: " + q)
        //if (return_contributing_words)
        //  contribs(word) = p*(log(p) - log(q))
      }

      val retval = kldiv + kl_divergence_34(other)
      //(retval, contribs)
      retval
    }
  }

  /**
   * Steps 3 and 4 of KL-divergence computation.
   * @see #kl_divergence
   */
  def kl_divergence_34(other: BigramWordDist): Double
  
  def get_nbayes_logprob(xworddist: WordDist) = {
    val worddist = xworddist.asInstanceOf[BigramWordDist]
    var logprob = 0.0
    for ((word, count) <- worddist.bicounts) {
      val value = lookup_bigram(word)
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

  def lookup_bigram(word: Word): Double

  def find_most_common_word(pred: String => Boolean) = {
    val filtered =
      (for ((word, count) <- unicounts if pred(unmemoize_string(word)))
        yield (word, count)).toSeq
    if (filtered.length == 0) None
    else {
      val (maxword, maxcount) = filtered maxBy (_._2)
      Some(maxword)
    }
  }
}

trait SimpleBigramWordDistReader extends WordDistReader {
  /**
   * Initial size of the internal DynamicArray objects; an optimization.
   */
  protected val initial_dynarr_size = 1000
  /**
   * Internal DynamicArray holding the keys (canonicalized words).
   */
  protected val keys_dynarr =
    new DynamicArray[Word](initial_alloc = initial_dynarr_size)
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
  /** Same as `keys_dynarr`, for bigrams. */
  protected val bigram_keys_dynarr =
    new DynamicArray[Word](initial_alloc = initial_dynarr_size)
  /** Same as `values_dynarr`, for bigrams. */
  protected val bigram_values_dynarr =
    new DynamicArray[Int](initial_alloc = initial_dynarr_size)
  /** Same as `raw_keys_set`, for bigrams. */
  protected val raw_bigram_keys_set = mutable.Set[(String, String)]()

  /**
   * Called each time a unigram word count is seen.  This can accept or
   * reject the word (e.g. based on whether the count is high enough or
   * the word is in a stopwords list), and optionally change the word into
   * something else (e.g. the lowercased version or a generic -OOV-).
   * It should also memoize the word appropriately.
   *
   * @param doc Document whose distribution is being initialized.
   * @param word Raw word seen.  NOTE: Unlike in the case of UnigramWordDist,
   *   the word is still encoded in the way it's found in the document file.
   *   (In particular, this means that colons are replaced with %3A, and
   *   percent signs by %25.)
   * @param count Raw count for the word
   * @return A memoized version of a modified form of the word, or
   *   None to reject the word.
   */
  def canonicalize_accept_unigram(doc: GenericDistDocument,
      word: String, count: Int): Option[Word]

  /**
   * Called each time a bigram word count is seen.  This can accept or
   * reject the bigram, much like for `canonicalize_accept_unigram`.
   * Bigrams can be memoized using `memoize_bigram`.
   *
   * @param doc Document whose distribution is being initialized.
   * @param bigram Tuple of `(word1, word2)` describing bigram.
   * @param count Raw count for the bigram
   * @return A memoized version of a modified form of the bigram, or
   *   None to reject the bigram.
   */
  def canonicalize_accept_bigram(doc: GenericDistDocument,
      bigram: (String, String), count: Int): Option[Word]

  /**
   * Memoize a bigram.  The words should be encoded to remove ':' signs,
   * just as they are stored in the document file.
   */
  def memoize_bigram(bigram: (String, String)) = {
    val word1 = bigram._1
    val word2 = bigram._2
    assert(!(word1 contains ':'))
    assert(!(word2 contains ':'))
    memoize_string(word1 + ":" + word2)
  }

  def parse_counts(doc: GenericDistDocument, countstr: String) {
    keys_dynarr.clear()
    values_dynarr.clear()
    raw_keys_set.clear()
    bigram_keys_dynarr.clear()
    bigram_values_dynarr.clear()
    raw_bigram_keys_set.clear()
    val wordcounts = countstr.split(" ")
    for (wordcount <- wordcounts) yield {
      val split_wordcount = wordcount.split(":", -1)
      def check_nonempty_word(word: String) {
        if (word.length == 0)
          throw FileFormatException(
            "For unigram/bigram counts, WORD must not be empty, but %s seen"
            format wordcount)
      }
      if (split_wordcount.length == 2) {
        val Array(word, strcount) = split_wordcount
        check_nonempty_word(word)
        val count = strcount.toInt
        if (raw_keys_set contains word)
          throw FileFormatException(
            "Word %s seen twice in same counts list" format word)
        raw_keys_set += word
        val opt_canon_word = canonicalize_accept_unigram(doc, word, count)
        if (opt_canon_word != None) {
          keys_dynarr += opt_canon_word.get
          values_dynarr += count
        }
      } else if (split_wordcount.length == 3) {
        val Array(word1, word2, strcount) = split_wordcount
        check_nonempty_word(word1)
        check_nonempty_word(word2)
        val word = (word1, word2)
        val count = strcount.toInt
        if (raw_bigram_keys_set contains word)
          throw FileFormatException(
            "Word %s seen twice in same counts list" format word)
        raw_bigram_keys_set += word
        val opt_canon_word = canonicalize_accept_bigram(doc, word, count)
        if (opt_canon_word != None) {
          keys_dynarr += opt_canon_word.get
          values_dynarr += count
        }
      } else
        throw FileFormatException(
          "For bigram counts, items must be of the form WORD:COUNT or WORD:WORD:COUNT, but %s seen"
          format wordcount)
    }
  }

  def initialize_distribution(doc: GenericDistDocument, countstr: String,
      is_training_set: Boolean) = {
    parse_counts(doc, countstr)
    // Now set the distribution on the document; but don't use the test
    // set's distributions in computing global smoothing values and such.
    set_bigram_word_dist(doc, keys_dynarr.array, values_dynarr.array,
      keys_dynarr.length, bigram_keys_dynarr.array,
      bigram_values_dynarr.array, bigram_keys_dynarr.length,
      is_training_set)
  }

  def set_bigram_word_dist(doc: GenericDistDocument,
      keys: Array[Word], values: Array[Int], num_words: Int,
      bigram_keys: Array[Word], bigram_values: Array[Int], num_bigrams: Int,
      is_training_set: Boolean): Boolean
}

/**
 * General factory for BigramWordDist distributions.
 */ 
abstract class BigramWordDistFactory extends
    WordDistFactory with SimpleBigramWordDistReader {
  def canonicalize_word(doc: GenericDistDocument, word: String) = {
    val lword = maybe_lowercase(doc, word)
    if (!is_stopword(doc, lword)) lword else "-OOV-"
  }

  def canonicalize_accept_unigram(doc: GenericDistDocument, raw_word: String,
      count: Int) = {
    val lword = maybe_lowercase(doc, raw_word)
    /* minimum_word_count (--minimum-word-count) currently handled elsewhere.
       FIXME: Perhaps should be handled here. */
    if (!is_stopword(doc, lword))
      Some(memoize_string(lword))
    else
      None
  }

  def canonicalize_accept_bigram(doc: GenericDistDocument,
      raw_bigram: (String, String), count: Int) = {
    val cword1 = canonicalize_word(doc, raw_bigram._1)
    val cword2 = canonicalize_word(doc, raw_bigram._2)
    /* minimum_word_count (--minimum-word-count) currently handled elsewhere.
       FIXME: Perhaps should be handled here. */
    Some(memoize_bigram((cword1, cword1)))
  }
}

