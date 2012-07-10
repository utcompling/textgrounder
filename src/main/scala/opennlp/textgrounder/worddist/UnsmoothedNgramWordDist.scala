///////////////////////////////////////////////////////////////////////////////
//  UnsmoothedNgramWordDist.scala
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

class UnsmoothedNgramWordDistFactory extends NgramWordDistFactory {
  def create_word_dist(note_globally: Boolean) =
    new UnsmoothedNgramWordDist(this, note_globally)

  def finish_global_distribution() {
  }
}

class UnsmoothedNgramWordDist(
  gen_factory: WordDistFactory,
  note_globally: Boolean
) extends NgramWordDist(gen_factory, note_globally) {
  import NgramStorage.Ngram

  type TThis = UnsmoothedNgramWordDist

  def innerToString = ""

  // For some reason, retrieving this value from the model is fantastically slow
  var num_tokens = 0.0

  protected def imp_finish_after_global() {
    num_tokens = model.num_tokens
  }

  def fast_kl_divergence(cache: KLDivergenceCache, other: WordDist,
      partial: Boolean = false) = {
    assert(false, "Not implemented")
    0.0
  }

  def cosine_similarity(other: WordDist, partial: Boolean = false,
      smoothed: Boolean = false) = {
    assert(false, "Not implemented")
    0.0
  }

  def kl_divergence_34(other: NgramWordDist) = {
    assert(false, "Not implemented")
    0.0
  }
 
  /**
   * Actual implementation of steps 3 and 4 of KL-divergence computation, given
   * a value that we may want to compute as part of step 2.
   */
  def inner_kl_divergence_34(other: TThis,
      overall_probs_diff_words: Double) = {
    assert(false, "Not implemented")
    0.0
  }

  def lookup_ngram(ngram: Ngram) =
    model.get_ngram_count(ngram).toDouble / num_tokens
}
