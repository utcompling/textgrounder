///////////////////////////////////////////////////////////////////////////////
//  GeolocateRoughRanker.scala
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

import util.argparser._
import util.spherical.SphereCoord
import util.experiment._
import gridlocate._

class GeolocateRoughRankerParameters(
  parser: ArgParser
) extends GeolocateParameters(parser) {
}

class GeolocateRoughRankerDriver extends
    GeolocateDriver with StandaloneExperimentDriverStats {
  type TParam = GeolocateRoughRankerParameters
  type TRunRes = PointwiseScoreGridRanker[SphereCoord]

  def run() = {
    val ranker = create_ranker
    ranker match {
      case e:PointwiseScoreGridRanker[SphereCoord] => e
      case _ =>
        param_error("Rough ranker must be of the pointwise type")
    }
  }
}
