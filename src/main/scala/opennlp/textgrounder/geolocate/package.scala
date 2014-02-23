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
}

package object geolocate {
  implicit def to_SphereCorrectCellInfo(
    cci: CorrectCellInfo[SphereCoord]
  ) = new SphereCorrectCellInfo(cci)

  type SphereDoc = GridDoc[SphereCoord]
  type SphereCell = GridCell[SphereCoord]
  type SphereGrid = Grid[SphereCoord]
  def get_sphere_docfact(grid: SphereGrid) =
    grid.docfact.asInstanceOf[SphereDocFactory]
}
