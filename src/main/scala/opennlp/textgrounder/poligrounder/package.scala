///////////////////////////////////////////////////////////////////////////////
//  package.scala
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

import util.time._

package poligrounder {
  trait TimeCoordMixin {
    def coord_as_double(coor: TimeCoord) = coor match {
      case null => Double.NaN
      case TimeCoord(x) => x.toDouble / 1000
    }

    def format_coord(coord: TimeCoord) = coord.format

    def distance_between_coords(c1: TimeCoord, c2: TimeCoord) =
      (coord_as_double(c2) - coord_as_double(c1)).abs

    def output_distance(dist: Double) = "%s seconds" format dist
  }
}
