///////////////////////////////////////////////////////////////////////////////
//  SphereDoc.scala
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

package opennlp.textgrounder.geolocate

import collection.mutable

import opennlp.textgrounder.{util => tgutil}
import tgutil.distances._
import tgutil.textdbutil.Schema
import tgutil.printutil.warning

import opennlp.textgrounder.gridlocate.{GeoDoc,GeoDocTable}
import opennlp.textgrounder.gridlocate.GeoDocConverters._

import opennlp.textgrounder.worddist.WordDistFactory

abstract class RealSphereDoc(
  schema: Schema,
  word_dist_factory: WordDistFactory
) extends GeoDoc[SphereCoord](schema, word_dist_factory) {
  var coord: SphereCoord = _
  def has_coord = coord != null

  override def set_field(name: String, value: String) {
    name match {
      case "coord" => coord = get_x_or_null[SphereCoord](value)
      case _ => super.set_field(name, value)
    }
  }

  def distance_to_coord(coord2: SphereCoord) = spheredist(coord, coord2)
  def output_distance(dist: Double) = km_and_miles(dist)
}

/**
 * A subtable holding SphereDocs corresponding to a specific corpus
 * type (e.g. Wikipedia or Twitter).
 */
abstract class SphereDocSubtable[TDoc <: SphereDoc](
  val table: SphereDocTable
) {
  /**
   * Create and return a document of the current type.
   */
  def create_document(schema: Schema): TDoc

  /**
   * Given the schema and field values read from a document file, create
   * and return a document.  Return value can be null if the document is
   * to be skipped; otherwise, it will be recorded in the appropriate split.
   */
  def create_and_init_document(schema: Schema, fieldvals: Seq[String],
      record_in_table: Boolean): Option[TDoc] = {
    val doc = create_document(schema)
    doc.set_fields(fieldvals)
    Some(doc)
  }

  /**
   * Do any subtable-specific operations needed after all documents have
   * been loaded.
   */
  def finish_document_loading() { }
}

/**
 * A GeoDocTable specifically for documents with coordinates described
 * by a SphereCoord (latitude/longitude coordinates on the Earth).
 * We delegate the actual document creation to a subtable specific to the
 * type of corpus (e.g. Wikipedia or Twitter).
 */
class SphereDocTable(
  override val driver: GeolocateDriver,
  word_dist_factory: WordDistFactory
) extends GeoDocTable[SphereCoord](
  driver, word_dist_factory
) {
  val corpus_type_to_subtable =
    mutable.Map[String, SphereDocSubtable[_ <: SphereDoc]]()

  def register_subtable(corpus_type: String, subtable:
      SphereDocSubtable[_ <: SphereDoc]) {
    corpus_type_to_subtable(corpus_type) = subtable
  }

  register_subtable("wikipedia", new WikipediaDocSubtable(this))
  register_subtable("twitter-tweet", new TwitterTweetDocSubtable(this))
  register_subtable("twitter-user", new TwitterUserDocSubtable(this))
  register_subtable("generic", new GenericSphereDocSubtable(this))

  def wikipedia_subtable =
    corpus_type_to_subtable("wikipedia").asInstanceOf[WikipediaDocSubtable]

  def create_document(schema: Schema): SphereDoc = {
    throw new UnsupportedOperationException("This shouldn't be called directly; instead, use create_and_init_document()")
  }

  override def imp_create_and_init_document(schema: Schema,
      fieldvals: Seq[String], record_in_table: Boolean) = {
    find_subtable(schema, fieldvals).
      create_and_init_document(schema, fieldvals, record_in_table)
  }

  /**
   * Find the subtable for the field values of a document as read from
   * from a document file.  Currently this simply locates the 'corpus-type'
   * parameter and calls `find_subtable(java.lang.String)` to find
   * the appropriate table.
   */
  def find_subtable(schema: Schema, fieldvals: Seq[String]):
      SphereDocSubtable[_ <: SphereDoc]  = {
    val cortype = schema.get_field_or_else(fieldvals, "corpus-type", "generic")
    find_subtable(cortype)
  }

  /**
   * Find the document table for a given corpus type.
   */
  def find_subtable(cortype: String) = {
    if (corpus_type_to_subtable contains cortype)
      corpus_type_to_subtable(cortype)
    else {
      warning("Unrecognized corpus type: %s", cortype)
      corpus_type_to_subtable("generic")
    }
  }

  /**
   * Iterate over all the subtables that exist.
   */
  def iter_subtables() = corpus_type_to_subtable.values

  override def finish_document_loading() {
    for (subtable <- iter_subtables())
      subtable.finish_document_loading()
    super.finish_document_loading()
  }
}

/**
 * A generic SphereDoc for when the corpus type is missing or
 * unrecognized. (FIXME: Do we really need this?  Should we just throw an
 * error or ignore it?)
 */
class GenericSphereDoc(
  schema: Schema,
  word_dist_factory: WordDistFactory
) extends RealSphereDoc(schema, word_dist_factory) {
  var title: String = _

  override def set_field(name: String, value: String) {
    name match {
      case "title" => title = value
      case _ => super.set_field(name, value)
    }
  }

  def struct =
    <GenericSphereDoc>
      <title>{ title }</title>
      {
        if (has_coord)
          <location>{ coord }</location>
      }
    </GenericSphereDoc>
}

class GenericSphereDocSubtable(
  table: SphereDocTable
) extends SphereDocSubtable[GenericSphereDoc](table) {
  def create_document(schema: Schema) =
    new GenericSphereDoc(schema, table.word_dist_factory)
}
