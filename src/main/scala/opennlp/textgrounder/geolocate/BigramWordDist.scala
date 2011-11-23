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
import opennlp.textgrounder.util.ioutil.FileHandler
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
    unicounts(memoize_word(previous)) += 1
    for {word <- words
         val wlower = if (ignore_case) word.toLowerCase() else word
         if !stopwords(wlower) } {
      unicounts(memoize_word(wlower)) += 1
      num_word_tokens += 1
      bicounts(memoize_word(previous + "_" + wlower)) += 1
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
     * @returns A tuple of (divergence, word_contribs) where the first
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
   * @seeo #kl_divergence
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
      (for ((word, count) <- unicounts if pred(unmemoize_word(word)))
        yield (word, count)).toSeq
    if (filtered.length == 0) None
    else {
      val (maxword, maxcount) = filtered maxBy (_._2)
      Some(maxword)
    }
  }
}

trait BigramWordDistReader extends WordDistReader {
  val initial_dynarr_size = 1000
  val keys_dynarr =
    new DynamicArray[Word](initial_alloc = initial_dynarr_size)
  val values_dynarr =
    new DynamicArray[Int](initial_alloc = initial_dynarr_size)
  val bigram_keys_dynarr =
    new DynamicArray[Word](initial_alloc = initial_dynarr_size)
  val bigram_values_dynarr =
    new DynamicArray[Int](initial_alloc = initial_dynarr_size)


  var num_word_tokens = 0
  var num_bigram_tokens = 0
  var title: String = _

  def do_read_word_counts(table: GenericDistDocumentTable,
      filehand: FileHandler, filename: String, stopwords: Set[String]) {
    errprint("Reading word and bigram counts from %s...", filename)
    errprint("")
    // Written this way because there's another line after the for loop,
    // corresponding to the else clause of the Python for loop
    breakable {
      for (line <- filehand.openr(filename)) {
        if (line.startsWith("Article title: ")) {
          if (title != null && num_word_tokens > 0) {
            if (!handle_one_document(table, title))
              break
          }
          // Extract title and set it
          val titlere = "Article title: (.*)$".r
          line match {
            case titlere(ti) => title = ti
            case _ => assert(false)
          }
          keys_dynarr.clear()
          values_dynarr.clear()
          num_word_tokens = 0
          bigram_keys_dynarr.clear()
          bigram_values_dynarr.clear()
          num_bigram_tokens = 0
        } else if (line.startsWith("Article coordinates) ") ||
          line.startsWith("Article ID: "))
          ()
        else {
          val linere = "(.*) = ([0-9]+)$".r
          line match {
            case linere(xword, xcount) => {
              var words = xword
              if (!Params.preserve_case_words) words = words.toLowerCase
              val count = xcount.toInt
              
              var tokens = words.split("\\s")
              if (tokens.length == 1) {
                if (!(stopwords contains words) ||
                    Params.include_stopwords_in_document_dists) {
                  num_word_tokens += count
                  keys_dynarr += memoize_word(words)
                  values_dynarr += count
	            }
              }
              else if (tokens.length == 2) {
                num_bigram_tokens += count
                bigram_keys_dynarr += memoize_word(words)
                bigram_values_dynarr += count
              }
            }
            case _ =>
              warning("Strange line, can't parse: title=%s: line=%s",
                title, line)
          }
        }
      }
      handle_one_document(table, title)
    }
  }

  def set_word_dist(doc: GenericDistDocument, is_training_set: Boolean,
      is_eval_set: Boolean) = {
    if (num_word_tokens == 0)
      false
    else {
      // If we are evaluating on the dev set, skip the test set and vice
      // versa, to save memory and avoid contaminating the results.
      if (is_training_set || is_eval_set) {
        // Now set the distribution on the document; but don't use the test
        // set's distributions in computing global smoothing values and such.
        set_bigram_word_dist(doc, keys_dynarr.array, values_dynarr.array,
          keys_dynarr.length, bigram_keys_dynarr.array,
          bigram_values_dynarr.array, bigram_keys_dynarr.length,
          note_globally = is_training_set)
        true
      }
      else false
    }
  }

  def set_bigram_word_dist(doc: GenericDistDocument, keys: Array[Word],
    values: Array[Int], num_words: Int, bigram_keys: Array[Word],
    bigram_values: Array[Int], num_bigrams: Int, note_globally: Boolean)
}

/**
 * General factory for BigramWordDist distributions.
 */ 
abstract class BigramWordDistFactory extends
    WordDistFactory with BigramWordDistReader {
}

