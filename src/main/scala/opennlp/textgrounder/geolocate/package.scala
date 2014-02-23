///////////////////////////////////////////////////////////////////////////////
//  package.scala
//
//  Copyright (C) 2012-2014 Ben Wing, The University of Texas at Austin
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

import gridlocate._
import util.spherical._

package geolocate {
  /**
   * A general trait holding SphereDoc-specific code for storing the
   * result of evaluation on a document.  Here we simply compute the
   * true and predicted "degree distances" -- i.e. measured in degrees,
   * rather than in actual distance along a great circle.
   */
  class SphereDocEvalResult(
    stats: DocEvalResult[SphereCoord]
  ) {
    /**
     * Distance in degrees between document's coordinate and central
     * point of correct cell
     */
    val correct_degdist = degree_dist(stats.document.coord, stats.correct_central_point)
    /**
     * Distance in degrees between doc's coordinate and predicted
     * coordinate
     */
    val pred_degdist = degree_dist(stats.document.coord, stats.pred_coord)
  }
}

package object geolocate {
  implicit def to_SphereDocEvalResult(
    stats: DocEvalResult[SphereCoord]
  ) = new SphereDocEvalResult(stats)

  type SphereDoc = GridDoc[SphereCoord]
  type SphereCell = GridCell[SphereCoord]
  type SphereGrid = Grid[SphereCoord]
  def get_sphere_docfact(grid: SphereGrid) =
    grid.docfact.asInstanceOf[SphereDocFactory]
}
