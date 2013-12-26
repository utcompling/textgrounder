///////////////////////////////////////////////////////////////////////////////
//  package.scala
//
//  Copyright (C) 2012-2013 Ben Wing, The University of Texas at Austin
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

package object learning {
  type FeatIndex = Int
  type LabelIndex = Int
  // type Vector = breeze.linalg.DenseVector[Double]
  // type Vector = Array[Double]

  // If we want to use double-valued feature vectors internally, do this:
//  type CompressedSparseFeatureVectorType = DoubleCompressedSparseFeatureVector
//  val feature_vector_implementation = "DoubleCompressed"
//  def to_feat_value(x: Double) = x

  // If we want to use float-valued feature vectors internally, do this:
  type CompressedSparseFeatureVectorType = FloatCompressedSparseFeatureVector
  val feature_vector_implementation = "FloatCompressed"
  def to_feat_value(x: Double) = x.toFloat
}

