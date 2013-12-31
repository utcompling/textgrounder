///////////////////////////////////////////////////////////////////////////////
//  SphereDoc.scala
//
//  Copyright (C) 2011-2013 Ben Wing, The University of Texas at Austin
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
import util.textdb.{Row, Schema}
import util.error.warning_once
import util.serialize.TextSerializer._

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
   * Given a row read from a document file, create and return a document.
   * Return value can be None if the document is to be skipped; otherwise,
   * it will be recorded in the appropriate split.
   */
  def create_document(row: Row, lang_model: DocLangModel,
      coord: SphereCoord): TDoc
  def record_training_document_in_subfactory(doc: SphereDoc) { }
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

  def create_document(row: Row,
     lang_model: DocLangModel) = {
    val coord = row.get_or_else[SphereCoord]("coord", null)
    find_subfactory(row).
      create_document(row, lang_model, coord)
  }

  override def record_training_document_in_subfactory(doc: SphereDoc) {
    val cortype = doc.schema.get_fixed_field_or_else("corpus-type", "generic")
    find_subfactory(cortype).record_training_document_in_subfactory(doc)
  }

  /**
   * Find the subfactory for the field values of a document as read from
   * from a document file.  Currently this simply locates the 'corpus-type'
   * parameter and calls `find_subfactory(java.lang.String)` to find
   * the appropriate subfactory.
   */
  def find_subfactory(row: Row): SphereDocSubfactory[_ <: SphereDoc] = {
    val cortype = row.gets_or_else("corpus-type", "generic")
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
}

class GenericSphereDocSubfactory(
  docfact: SphereDocFactory
) extends SphereDocSubfactory[GenericSphereDoc](docfact) {
  def create_document(row: Row, lang_model: DocLangModel,
      coord: SphereCoord) =
    new GenericSphereDoc(row.schema, lang_model, coord,
      row.get_if[Double]("salience"),
      row.get_or_else[String]("title", "unknown")
    )
}
