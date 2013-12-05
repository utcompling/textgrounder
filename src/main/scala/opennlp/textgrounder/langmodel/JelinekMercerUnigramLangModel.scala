///////////////////////////////////////////////////////////////////////////////
//  JelinekMercerUnigramLangModel.scala
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

/**
 * This class implements Jelinek-Mercer discounting, the simplest type of
 * discounting where we just use a constant discount factor.
 */ 
class JelinekMercerUnigramLangModelFactory(
    create_builder: LangModelFactory => LangModelBuilder,
    interpolate_string: String,
    tf_idf: Boolean,
    val jelinek_factor: Double
) extends DiscountedUnigramLangModelFactory(
  create_builder, interpolate_string != "no", tf_idf
) {
  def create_lang_model = new JelinekMercerUnigramLangModel(this)
}

class JelinekMercerUnigramLangModel(
  factory: JelinekMercerUnigramLangModelFactory
) extends DiscountedUnigramLangModel(factory) {
  override protected def imp_finish_after_global() {
    unseen_mass = factory.jelinek_factor
    super.imp_finish_after_global()
  }

  def class_name = "JelinekMercerUnigramLangModel"
}
