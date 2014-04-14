///////////////////////////////////////////////////////////////////////////////
//  RawFeatureVector.scala
//
//  Copyright (C) 2014 Ben Wing, The University of Texas at Austin
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
package learning

trait RawFeatureVector {
  def pretty_format(prefix: String): String
}

case class RawSimpleFeatureVector(
  data: Iterable[(FeatureValue, String, Double)]
) extends RawFeatureVector {
  def pretty_format(prefix: String) = ???
}

case class RawAggregateFeatureVector(
  data: Iterable[Iterable[(FeatureValue, String, Double)]]
) extends RawFeatureVector {
  def pretty_format(prefix: String) = ???
}

case class RawAggregateTrainingData(
  data: Iterable[(RawAggregateFeatureVector, LabelIndex)]
) extends GenTrainingData[RawAggregateFeatureVector]
