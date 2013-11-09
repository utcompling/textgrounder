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

package opennlp.textgrounder
package geolocate

import collection.mutable

import util.spherical._
import util.textdb.Schema
import util.print.warning_once
import util.Serializer._

import gridlocate._

abstract class RealSphereDoc(
  schema: Schema,
  lang_model: DocLangModel,
  val coord: SphereCoord,
  override val salience: Option[Double]
) extends GridDoc[SphereCoord](schema, lang_model) {
  def has_coord = coord != null

  def distance_to_coord(coord2: SphereCoord) = spheredist(coord, coord2)
  def output_distance(dist: Double) = km_and_miles(dist)
}

/**
 * A subfactory holding SphereDocs corresponding to a specific corpus
 * type (e.g. Wikipedia or Twitter).
 */
abstract class SphereDocSubfactory[TDoc <: SphereDoc](
  val docfact: SphereDocFactory
) {
  /**
   * Given the schema and field values read from a document file, create
   * and return a document.  Return value can be None if the document is
   * to be skipped; otherwise, it will be recorded in the appropriate split.
   */
  def create_and_init_document(schema: Schema, fieldvals: IndexedSeq[String],
      lang_model: DocLangModel, coord: SphereCoord,
      record_in_factory: Boolean): Option[TDoc]
}

/**
 * A GridDocFactory specifically for documents with coordinates described
 * by a SphereCoord (latitude/longitude coordinates on the Earth).
 * We delegate the actual document creation to a subfactory specific to the
 * type of corpus (e.g. Wikipedia or Twitter).
 */
class SphereDocFactory(
  override val driver: GeolocateDriver,
  lang_model_factory: DocLangModelFactory
) extends GridDocFactory[SphereCoord](
  driver, lang_model_factory
) {
  val corpus_type_to_subfactory =
    mutable.Map[String, SphereDocSubfactory[_ <: SphereDoc]]()

  def register_subfactory(corpus_type: String, subfactory:
      SphereDocSubfactory[_ <: SphereDoc]) {
    corpus_type_to_subfactory(corpus_type) = subfactory
  }

  register_subfactory("wikipedia", new WikipediaDocSubfactory(this))
  register_subfactory("twitter-tweet", new TwitterTweetDocSubfactory(this))
  register_subfactory("twitter-user", new TwitterUserDocSubfactory(this))
  register_subfactory("generic", new GenericSphereDocSubfactory(this))

  def wikipedia_subfactory =
    corpus_type_to_subfactory("wikipedia").asInstanceOf[WikipediaDocSubfactory]

  override def imp_create_and_init_document(schema: Schema,
      fieldvals: IndexedSeq[String], lang_model: DocLangModel,
      record_in_subfactory: Boolean) = {
    val coord = schema.get_value_or_else[SphereCoord](fieldvals, "coord", null)
    find_subfactory(schema, fieldvals).
      create_and_init_document(schema, fieldvals, lang_model, coord,
        record_in_subfactory)
  }

  /**
   * Find the subfactory for the field values of a document as read from
   * from a document file.  Currently this simply locates the 'corpus-type'
   * parameter and calls `find_subfactory(java.lang.String)` to find
   * the appropriate subfactory.
   */
  def find_subfactory(schema: Schema, fieldvals: IndexedSeq[String]):
      SphereDocSubfactory[_ <: SphereDoc]  = {
    val cortype = schema.get_field_or_else(fieldvals, "corpus-type", "generic")
    find_subfactory(cortype)
  }

  /**
   * Find the document subfactory for a given corpus type.
   */
  def find_subfactory(cortype: String) = {
    if (corpus_type_to_subfactory contains cortype)
      corpus_type_to_subfactory(cortype)
    else {
      warning_once("Unrecognized corpus type: %s", cortype)
      corpus_type_to_subfactory("generic")
    }
  }
}

/**
 * A generic SphereDoc for when the corpus type is missing or
 * unrecognized. (FIXME: Do we really need this?  Should we just throw an
 * error or ignore it?)
 */
class GenericSphereDoc(
  schema: Schema,
  lang_model: DocLangModel,
  coord: SphereCoord,
  salience: Option[Double],
  val title: String
) extends RealSphereDoc(schema, lang_model, coord, salience) {

  def xmldesc =
    <GenericSphereDoc>
      <title>{ title }</title>
      {
        if (has_coord)
          <location>{ coord }</location>
      }
    </GenericSphereDoc>
}

class GenericSphereDocSubfactory(
  docfact: SphereDocFactory
) extends SphereDocSubfactory[GenericSphereDoc](docfact) {
  def create_and_init_document(schema: Schema, fieldvals: IndexedSeq[String],
      lang_model: DocLangModel, coord: SphereCoord, record_in_factory: Boolean) = Some(
    new GenericSphereDoc(schema, lang_model, coord,
      schema.get_value_if[Double](fieldvals, "salience"),
      schema.get_value_or_else[String](fieldvals, "title", "unknown"))
    )
}
