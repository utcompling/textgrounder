///////////////////////////////////////////////////////////////////////////////
//  UnsmoothedNgramLangModel.scala
//
//  Copyright (C) 2010-2013 Ben Wing, The University of Texas at Austin
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

class UnsmoothedNgramLangModelFactory(
  create_builder: LangModelFactory => LangModelBuilder
) extends NgramLangModelFactory {
  val builder = create_builder(this)
  def create_lang_model = new UnsmoothedNgramLangModel(this)

  def finish_global_backoff_stats() {
  }
}

class UnsmoothedNgramLangModel(
  gen_factory: LangModelFactory
) extends NgramLangModel(gen_factory) {
  type TThis = UnsmoothedNgramLangModel

  def class_name = "UnsmoothedNgramLangModel"

  // For some reason, retrieving this value from the model is fantastically slow
  var num_tokens = 0.0

  protected def imp_finish_after_global() {
    num_tokens = model.num_tokens
  }

  def fast_kl_divergence(other: LangModel, partial: Boolean = true,
    cache: KLDivergenceCache = null) = ???

  def cosine_similarity(other: LangModel, partial: Boolean = true,
      smoothed: Boolean = false) = ???

  def kl_divergence_34(other: NgramLangModel) = ???
 
  /**
   * Actual implementation of steps 3 and 4 of KL-divergence computation, given
   * a value that we may want to compute as part of step 2.
   */
  def inner_kl_divergence_34(other: TThis,
      overall_probs_diff_words: Double) = ???

  def lookup_ngram(ngram: Ngram) =
    model.get_item(ngram).toDouble / num_tokens
}
