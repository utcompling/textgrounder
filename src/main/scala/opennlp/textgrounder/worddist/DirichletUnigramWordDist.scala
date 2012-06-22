///////////////////////////////////////////////////////////////////////////////
//  DirichletUnigramWordDist.scala
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

/**
 * This class implements Dirichlet discounting, where the discount factor
 * depends on the size of the document.
 */ 
class DirichletUnigramWordDistFactory(
    interpolate_string: String,
    val dirichlet_factor: Double
  ) extends DiscountedUnigramWordDistFactory(interpolate_string != "no") {
  def create_word_dist(note_globally: Boolean) =
    new DirichletUnigramWordDist(this, note_globally)
}

class DirichletUnigramWordDist(
    factory: WordDistFactory,
    note_globally: Boolean
) extends DiscountedUnigramWordDist(
    factory, note_globally
  ) {
  override protected def imp_finish_after_global() {
    unseen_mass = 1.0 -
      (num_word_tokens.toDouble /
        (num_word_tokens +
          factory.asInstanceOf[DirichletUnigramWordDistFactory].
            dirichlet_factor))
    super.imp_finish_after_global()
  }
}
