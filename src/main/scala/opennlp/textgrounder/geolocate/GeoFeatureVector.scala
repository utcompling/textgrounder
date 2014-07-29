///////////////////////////////////////////////////////////////////////////////
//  GeoFeatureVector.scala
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
package geolocate

import util.spherical._
import util.Twokenize

import gridlocate._
import learning.{SparseFeatureVectorFactory, FeatBinary}

/**
 * A factory for generating features for a doc-cell candidate consisting of
 * separate features for each word.
 */
class TwitterDocFeatVecFactory(
  val featvec_factory: SparseFeatureVectorFactory,
  val binning_status: BinningStatus,
  val ty: String
) extends DocFeatVecFactory[SphereCoord] {
  def get_string_features(str: String, ty: String) = {
    val punc_to_filter = Set(",", "/")
    val words = Twokenize(str.toLowerCase).filter { word =>
      !(punc_to_filter contains word) }
    // Do words
    (for (word <- words) yield
      (FeatBinary, "%s$%s" format (ty, word), 1.0)) ++
    // Do bigrams
    (for (Seq(w1, w2) <- words.sliding(2).toSeq) yield
      (FeatBinary, "%s$%s$%s" format (ty, w1, w2), 1.0)) ++
    // Do the whole thing
    Iterable((FeatBinary, "%s$%s" format (ty, words.mkString("$")), 1.0))
  }

  def imp_get_features(doc: SphereDoc) = {
    doc match {
      case d: TwitterDoc => {
        ty match {
          case "lang" => Iterable((FeatBinary, "lang$%s" format d.lang, 1.0))
          case "location" => get_string_features(d.location, ty)
          case "timezone" =>
            get_string_features(d.timezone, ty) ++
            Iterable((FeatBinary, "utc-offset$%s" format d.utc_offset, 1.0))
        }
      }
      case _ => Iterable()
    }
  }
}
