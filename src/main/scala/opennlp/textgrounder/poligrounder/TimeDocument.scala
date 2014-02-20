///////////////////////////////////////////////////////////////////////////////
//  TimeDocument.scala
//
//  Copyright (C) 2011-2014 Ben Wing, The University of Texas at Austin
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
package poligrounder

import collection.mutable

import util.spherical._
import util.textdb.{Row, Schema}
import util.print._
import util.time._
import util.serialize.TextSerializer._

import gridlocate._

class TimeDoc(
  schema: Schema,
  lang_model: DocLangModel,
  val coord: TimeCoord,
  val user: String
) extends GridDoc[TimeCoord](schema, lang_model) with TimeCoordMixin {
  def has_coord = coord != null
  def title = if (coord != null) coord.toString else "unknown time"
}

/**
 * A GridDocFactory specifically for documents with coordinates described
 * by a TimeCoord.
 * We delegate the actual document creation to a subfactory specific to the
 * type of corpus (e.g. Wikipedia or Twitter).
 */
class TimeDocFactory(
  override val driver: PoligrounderDriver,
  lang_model_factory: DocLangModelFactory
) extends GridDocFactory[TimeCoord](
  driver, lang_model_factory
) {
  def create_document(row: Row, lang_model: DocLangModel) =
    new TimeDoc(row.schema, lang_model,
      row.get[TimeCoord]("min-timestamp"),
      row.get[String]("user")
    )
}

