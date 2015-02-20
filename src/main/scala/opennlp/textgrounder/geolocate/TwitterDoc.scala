///////////////////////////////////////////////////////////////////////////////
//  TwitterDoc.scala
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
package geolocate

import util.spherical._
import util.textdb.Schema

import gridlocate._

abstract class TwitterDoc(
  schema: Schema,
  lang_model: DocLangModel,
  coord: SphereCoord,
  salience: Option[Double],
  val lang: String,
  val location: String,
  val timezone: String,
  val utc_offset: String
) extends RealSphereDoc(schema, lang_model, coord, salience)

class TwitterTweetDoc(
  schema: Schema,
  lang_model: DocLangModel,
  coord: SphereCoord,
  salience: Option[Double],
  lang: String,
  location: String,
  timezone: String,
  utc_offset: String,
  val id: Long
) extends TwitterDoc(schema, lang_model, coord, salience, lang, location,
    timezone, utc_offset) {
  def title = id.toString
}

class TwitterTweetDocSubfactory(
  docfact: SphereDocFactory
) extends SphereDocSubfactory[TwitterTweetDoc](docfact) {
  def create_document(rawdoc: RawDoc, lang_model: DocLangModel,
      coord: SphereCoord) =
    new TwitterTweetDoc(rawdoc.row.schema, lang_model, coord,
      rawdoc.row.get_if[Double]("salience"),
      rawdoc.row.get_or_else[String]("lang", ""),
      rawdoc.row.get_or_else[String]("location", ""),
      rawdoc.row.get_or_else[String]("timezone", ""),
      rawdoc.row.get_or_else[String]("utc_offset", ""),
      rawdoc.row.get_or_else[Long]("title", 0L))
}

class TwitterUserDoc(
  schema: Schema,
  lang_model: DocLangModel,
  coord: SphereCoord,
  salience: Option[Double],
  lang: String,
  location: String,
  timezone: String,
  utc_offset: String,
  val user: String
) extends TwitterDoc(schema, lang_model, coord, salience, lang, location,
    timezone, utc_offset) {
  def title = user
}

class TwitterUserDocSubfactory(
  docfact: SphereDocFactory
) extends SphereDocSubfactory[TwitterUserDoc](docfact) {
  def create_document(rawdoc: RawDoc, lang_model: DocLangModel,
      coord: SphereCoord) =
    new TwitterUserDoc(rawdoc.row.schema, lang_model, coord,
      rawdoc.row.get_if[Double]("salience"),
      rawdoc.row.get_or_else[String]("lang", ""),
      rawdoc.row.get_or_else[String]("location", ""),
      rawdoc.row.get_or_else[String]("timezone", ""),
      rawdoc.row.get_or_else[String]("utc_offset", ""),
      rawdoc.row.get_or_else[String]("user", ""))
}
