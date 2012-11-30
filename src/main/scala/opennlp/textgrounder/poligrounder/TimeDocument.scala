///////////////////////////////////////////////////////////////////////////////
//  TimeDoc.scala
//
//  Copyright (C) 2011, 2012 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder.poligrounder

import collection.mutable

import opennlp.textgrounder.{util => tgutil}
import tgutil.distances._
import tgutil.textdbutil.Schema
import tgutil.printutil._
import tgutil.Serializer._

import opennlp.textgrounder.gridlocate.{GeoDoc,GeoDocFactory,GeoGrid}

import opennlp.textgrounder.worddist.WordDistFactory

class TimeDoc(
  schema: Schema,
  word_dist_factory: WordDistFactory
) extends GeoDoc[TimeCoord](schema, word_dist_factory) {
  var coord: TimeCoord = _
  var user: String = _
  def has_coord = coord != null
  def title = if (coord != null) coord.toString else "unknown time"

  def xmldesc =
    <TimeDoc>
      {
        if (has_coord)
          <timestamp>{ coord }</timestamp>
      }
    </TimeDoc>

  override def set_field(name: String, value: String) {
    name match {
      case "min-timestamp" => coord = get_x_or_null[TimeCoord](value)
      case "user" => user = value
      case _ => super.set_field(name, value)
    }
  }

  def coord_as_double(coor: TimeCoord) = coor match {
    case null => Double.NaN
    case TimeCoord(x) => x.toDouble / 1000
  }

  def distance_to_coord(coord2: TimeCoord) = {
    (coord_as_double(coord2) - coord_as_double(coord)).abs
  }
  def output_distance(dist: Double) = "%s seconds" format dist
}

/**
 * A GeoDocFactory specifically for documents with coordinates described
 * by a TimeCoord.
 * We delegate the actual document creation to a subfactory specific to the
 * type of corpus (e.g. Wikipedia or Twitter).
 */
class TimeDocFactory(
  override val driver: PoligrounderDriver,
  word_dist_factory: WordDistFactory
) extends GeoDocFactory[TimeCoord](
  driver, word_dist_factory
) {
  def create_document(schema: Schema) =
    new TimeDoc(schema, word_dist_factory)
}

