///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2011 Ben Wing, The University of Texas at Austin
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

import java.io._

import opennlp.textgrounder.util.collectionutil.DynamicArray
import opennlp.textgrounder.util.ioutil.{errprint, warning, FileHandler}
import opennlp.textgrounder.util.MeteredTask
import opennlp.textgrounder.util.osutil.output_resource_usage

import GeolocateDriver.Params
import GeolocateDriver.Debug._
import WordDist.memoizer._

/**
 * Extract out the behavior related to the pseudo Good-Turing smoother.
 */
class PGTSmoothedBigramWordDistFactory extends
    BigramWordDistFactory {
  // Total number of types seen once
  var total_num_types_seen_once = 0

  // Estimate of number of unseen word types for all articles
  var total_num_unseen_word_types = 0

  /**
   * Overall probabilities over all articles of seeing a word in an article,
   * for all words seen at least once in any article, computed using the
   * empirical frequency of a word among all articles, adjusted by the mass
   * to be assigned to globally unseen words (words never seen at all), i.e.
   * the value in 'globally_unseen_word_prob'.  We start out by storing raw
   * counts, then adjusting them.
   */
  var overall_word_probs = create_word_double_map()
  var owp_adjusted = false

  // The total probability mass to be assigned to words not seen at all in
  // any article, estimated using Good-Turing smoothing as the unadjusted
  // empirical probability of having seen a word once.
  var globally_unseen_word_prob = 0.0

  // For articles whose word counts are not known, use an empty list to
  // look up in.
  // unknown_article_counts = ([], [])

  def finish_global_distribution() = {
    /* We do in-place conversion of counts to probabilities.  Make sure
       this isn't done twice!! */
    assert (!owp_adjusted)
    owp_adjusted = true
    // Now, adjust overall_word_probs accordingly.
    //// FIXME: A simple calculation reveals that in the scheme where we use
    //// globally_unseen_word_prob, total_num_types_seen_once cancels out and
    //// we never actually have to compute it.
    total_num_types_seen_once = overall_word_probs.values count (_ == 1.0)
    globally_unseen_word_prob =
      total_num_types_seen_once.toDouble/WordDist.total_num_word_tokens
    for ((word, count) <- overall_word_probs)
      overall_word_probs(word) = (
        count.toDouble/WordDist.total_num_word_tokens*
        (1.0 - globally_unseen_word_prob))
    // A very rough estimate, perhaps totally wrong
    total_num_unseen_word_types =
      total_num_types_seen_once max (WordDist.total_num_word_types/20)
    if (debug("bigram"))
      errprint("Total num types = %s, total num tokens = %s, total num_seen_once = %s, globally unseen word prob = %s, total mass = %s",
               WordDist.total_num_word_types, WordDist.total_num_word_tokens,
               total_num_types_seen_once,
               globally_unseen_word_prob,
               globally_unseen_word_prob + (overall_word_probs.values sum))
  }

  def set_bigram_word_dist(doc: DistDocument, keys: Array[Word],
    values: Array[Int], num_words: Int, bigram_keys: Array[Word],
    bigram_values: Array[Int], num_bigrams: Int, note_globally: Boolean) {
    doc.dist =
      new PGTSmoothedBigramWordDist(this, keys, values, num_words,
        bigram_keys, bigram_values, num_bigrams, note_globally = note_globally)
  }
  
  def create_word_dist() =
    new PGTSmoothedBigramWordDist(this, Array[Word](), Array[Int](), 0, Array[Word](), Array[Int](), 0)
}
