///////////////////////////////////////////////////////////////////////////////
//  PseudoGoodTuringUnigramWordDist.scala
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

package opennlp.textgrounder.worddist

import opennlp.textgrounder.util.printutil.errprint

import opennlp.textgrounder.gridlocate.GridLocateDriver.Debug._

/**
 * This class implements a simple version of Good-Turing smoothing where we
 * assign probability mass to unseen words equal to the probability mass of
 * all words seen once, and rescale the remaining probabilities accordingly.
 * ("Proper" Good-Turing is more general and adjusts the probabilities of
 * words seen N times according to the number of words seen N-1 times.
 * FIXME: I haven't thought carefully enough to make sure that this
 * simplified version actually makes theoretical sense, although I assume it
 * does because it can be seen as a better version of add-one or
 * add-some-value smoothing, where instead of just adding some arbitrary
 * value to unseen words, we determine the amount to add in a way that makes
 * theoretical sense (and in particular ensures that we don't assign 99%
 * of the mass or whatever to unseen words, as can happen to add-one smoothing
 * especially for bigrams or trigrams).
 */ 
class PseudoGoodTuringUnigramWordDistFactory(
    interpolate_string: String
  ) extends DiscountedUnigramWordDistFactory(interpolate_string == "yes") {
  // Total number of types seen once
  var total_num_types_seen_once = 0

  // The total probability mass to be assigned to words not seen at all in
  // any document, estimated using Good-Turing smoothing as the unadjusted
  // empirical probability of having seen a word once.
  var globally_unseen_word_prob = 0.0

  // For documents whose word counts are not known, use an empty list to
  // look up in.
  // unknown_document_counts = ([], [])

  override def finish_global_distribution() = {
    // Now, adjust overall_word_probs accordingly.
    //// FIXME: A simple calculation reveals that in the scheme where we use
    //// globally_unseen_word_prob, total_num_types_seen_once cancels out and
    //// we never actually have to compute it.
    /* Definitely no longer used in the "new way".
    total_num_types_seen_once = overall_word_probs.values count (_ == 1.0)
    globally_unseen_word_prob =
      total_num_types_seen_once.toDouble/total_num_word_tokens
    */
    // A very rough estimate, perhaps totally wrong
    total_num_unseen_word_types =
      total_num_types_seen_once max (total_num_word_types/20)
    if (debug("tons"))
      errprint("Total num types = %s, total num tokens = %s, total num_seen_once = %stotal mass = %s",
               total_num_word_types, total_num_word_tokens,
               total_num_types_seen_once,
               (overall_word_probs.values sum))
    super.finish_global_distribution()
  }

  def create_word_dist(note_globally: Boolean) =
    new PseudoGoodTuringUnigramWordDist(this, note_globally)
}

class PseudoGoodTuringUnigramWordDist(
    factory: WordDistFactory,
    note_globally: Boolean
) extends DiscountedUnigramWordDist(
    factory, note_globally
  ) {
  /**
   * Here we compute the value of `overall_unseen_mass`, which depends
   * on the global `overall_word_probs` computed from all of the
   * distributions.
   */
  override protected def imp_finish_after_global() {
    // Compute probabilities.  Use a very simple version of Good-Turing
    // smoothing where we assign to unseen words the probability mass of
    // words seen once, and adjust all other probs accordingly.
    val num_types_seen_once = counts.values count (_ == 1)
    unseen_mass =
      if (num_word_tokens > 0)
        // If no words seen only once, we will have a problem if we assign 0
        // to the unseen mass, as unseen words will end up with 0 probability.
        // However, if we assign a value of 1.0 to unseen_mass (which could
        // happen in case all words seen exactly once), then we will end
        // up assigning 0 probability to seen words.  So we arbitrarily
        // limit it to 0.5, which is pretty damn much mass going to unseen
        // words.
        0.5 min ((1.0 max num_types_seen_once)/num_word_tokens)
      else 0.5
    super.imp_finish_after_global()
  }
}
