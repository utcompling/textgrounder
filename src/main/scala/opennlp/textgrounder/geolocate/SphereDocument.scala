//  Copyright (C) 2011 Ben Wing, The University of Texas at Austin
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

////////
//////// SphereDocument.scala
////////
//////// Copyright (c) 2011 Ben Wing.
////////

package opennlp.textgrounder.geolocate

import opennlp.textgrounder.util.distances._
import opennlp.textgrounder.util.ioutil._
import opennlp.textgrounder.util.printutil.warning
import GeoDocumentConverters._

abstract class SphereDocument(
  schema: Seq[String],
  table: SphereDocumentTable
) extends DistDocument[SphereCoord](schema, table) {
  var coord: SphereCoord = _
  def has_coord = coord != null

  override def set_field(name: String, value: String) {
    name match {
      case "coord" => coord = get_x_or_null[SphereCoord](value)
      case _ => super.set_field(name, value)
    }
  }

  def distance_to_coord(coord2: SphereCoord) = spheredist(coord, coord2)
  def degree_distance_to_coord(coord2: SphereCoord) = degree_dist(coord, coord2)
}

/**
 * A subtable holding SphereDocuments corresponding to a specific corpus
 * type (e.g. Wikipedia or Twitter).
 */
abstract class SphereDocumentSubtable[DocType <: SphereDocument](
  val table: SphereDocumentTable
) {
  /**
   * Create and return a document of the current type.
   */
  def create_document(schema: Seq[String]): DocType

  /**
   * Given the schema and field values read from a document file, create
   * and return a document.  Return value can be null if the document is
   * to be skipped; otherwise, it will be recorded in the appropriate split.
   */
  def create_and_init_document(schema: Seq[String], fieldvals: Seq[String]) = {
    val doc = create_document(schema)
    if (doc != null)
      doc.set_fields(fieldvals)
    doc
  }

  /**
   * Do any subtable-specific operations needed after all documents have
   * been loaded.
   */
  def finish_document_loading() { }
}

/**
 * A DistDocumentTable specifically for documents with coordinates described
 * by a SphereCoord (latitude/longitude coordinates on the Earth).
 * We delegate the actual document creation to a subtable specific to the
 * type of corpus (e.g. Wikipedia or Twitter).
 */
class SphereDocumentTable(
  driver: GeolocateDriver,
  word_dist_factory: WordDistFactory
) extends DistDocumentTable[SphereCoord, SphereDocument](
  driver, word_dist_factory
) {
  val wikipedia_subtable = new WikipediaDocumentSubtable(this)
  val twitter_subtable = new TwitterDocumentSubtable(this)
  val generic_subtable = new GenericSphereDocumentSubtable(this)

  def create_document(schema: Seq[String]): SphereDocument = {
    throw new UnsupportedOperationException("This shouldn't be called directly; instead, use create_and_init_document()")
  }

  override def create_and_init_document(schema: Seq[String],
      fieldvals: Seq[String]) = {
    find_subtable(schema, fieldvals).
      create_and_init_document(schema, fieldvals)
  }

  /**
   * Find the subtable for the field values of a document as read from
   * from a document file.  Currently this simply locates the 'corpus-type'
   * parameter and calls `find_subtable(java.lang.String)` to find
   * the appropriate table.
   */
  def find_subtable(schema: Seq[String], fieldvals: Seq[String]):
      SphereDocumentSubtable[_ <: SphereDocument]  = {
    val cortype = get_field_value_or_none(schema, fieldvals, "corpus-type")
    if (cortype == null)
      generic_subtable
    else find_subtable(cortype)
  }

  /**
   * Find the document table for a given corpus type.
   */
  def find_subtable(cortype: String) = {
    if (cortype == "wikipedia")
      wikipedia_subtable 
    else if (cortype == "twitter")
      twitter_subtable
    else {
      warning("Unrecognized corpus type: %s", cortype)
      generic_subtable 
    }
  }

  /**
   * Iterate over all the subtables that exist.
   */
  def iterate_subtables() = {
    List(wikipedia_subtable, twitter_subtable,
      generic_subtable)
  }

  override def finish_document_loading() {
    for (subtable <- iterate_subtables())
      subtable.finish_document_loading()
    super.finish_document_loading()
  }
}

/**
 * Abstract class for a grid of cells covering the earth.
 */
abstract class SphereCellGrid(
  override val table: SphereDocumentTable
) extends CellGrid[SphereCoord, SphereDocument, SphereCell](table) {
}

/**
 * A generic SphereDocument for when the corpus type is missing or
 * unrecognized. (FIXME: Do we really need this?  Should we just throw an
 * error or ignore it?)
 */
class GenericSphereDocument(
  schema: Seq[String],
  subtable: GenericSphereDocumentSubtable
) extends SphereDocument(schema, subtable.table) {
  var title: String = _

  override def set_field(name: String, value: String) {
    name match {
      case "title" => title = value
      case _ => super.set_field(name, value)
    }
  }

  def struct =
    <GenericSphereDocument>
      <title>{ title }</title>
      {
        if (has_coord)
          <location>{ coord }</location>
      }
    </GenericSphereDocument>
}

class GenericSphereDocumentSubtable(
  table: SphereDocumentTable
) extends SphereDocumentSubtable[GenericSphereDocument](table) {
  def create_document(schema: Seq[String]) =
    new GenericSphereDocument(schema, this)
}
