///////////////////////////////////////////////////////////////////////////////
//  TimeDocument.scala
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

import opennlp.textgrounder.util.distances._
import opennlp.textgrounder.util.corpusutil.Schema
import opennlp.textgrounder.util.printutil._

import opennlp.textgrounder.gridlocate.{DistDocument,DistDocumentTable,CellGrid}
import opennlp.textgrounder.gridlocate.DistDocumentConverters._

import opennlp.textgrounder.worddist.WordDistFactory

class TimeDocument(
  schema: Schema,
  table: TimeDocumentTable
) extends DistDocument[TimeCoord](schema, table) {
  var coord: TimeCoord = _
  def has_coord = coord != null
  def title = if (coord != null) coord.toString else "unknown time"

  def struct =
    <TimeDocument>
      {
        if (has_coord)
          <timestamp>{ coord }</timestamp>
      }
    </TimeDocument>

  override def set_field(name: String, value: String) {
    name match {
      case "timestamp" => coord = get_x_or_null[TimeCoord](value)
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
 * A DistDocumentTable specifically for documents with coordinates described
 * by a TimeCoord.
 * We delegate the actual document creation to a subtable specific to the
 * type of corpus (e.g. Wikipedia or Twitter).
 */
class TimeDocumentTable(
  override val driver: PoligrounderDriver,
  word_dist_factory: WordDistFactory
) extends DistDocumentTable[TimeCoord, TimeDocument, TimeCellGrid](
  driver, word_dist_factory
) {
  def create_document(schema: Schema) = new TimeDocument(schema, this)
}

