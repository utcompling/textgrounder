///////////////////////////////////////////////////////////////////////////////
//  coord.scala
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
package util
package coord

/** A type class for auxiliary functions related to a coordinate type. */
@annotation.implicitNotFound(msg = "No implicit CoordHandler defined for ${Co}.")
trait CoordHandler[Co] {
  /**
   * Return distance between two coordinates.
   */
  def distance_between_coords(c1: Co, c2: Co): Double
  /**
   * True if c1 &lt; c2.
   */
  def less_than_coord(c1: Co, c2: Co): Boolean
  /**
   * Format a coordinate for human-readable display.
   */
  def format_coord(coord2: Co): String
  /**
   * Output a distance with attached units
   */
  def output_distance(dist: Double): String
}
