///////////////////////////////////////////////////////////////////////////////
//  TwitterDoc.scala
//
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

package opennlp.textgrounder
package geolocate

import util.spherical._
import util.textdb.Schema

import gridlocate._

class TwitterTweetDoc(
  schema: Schema,
  lang_model: DocLangModel,
  coord: SphereCoord,
  val id: Long
) extends RealSphereDoc(schema, lang_model, coord) {
  def title = id.toString

  def xmldesc =
    <TwitterTweetDoc>
      <id>{ id }</id>
      {
        if (has_coord)
          <location>{ coord }</location>
      }
    </TwitterTweetDoc>
}

class TwitterTweetDocSubfactory(
  docfact: SphereDocFactory
) extends SphereDocSubfactory[TwitterTweetDoc](docfact) {
  def create_and_init_document(schema: Schema, fieldvals: IndexedSeq[String],
      lang_model: DocLangModel, coord: SphereCoord,
      record_in_factory: Boolean) = Some(
    new TwitterTweetDoc(schema, lang_model, coord,
      schema.get_value_or_else[Long](fieldvals, "title", 0L))
    )
}

class TwitterUserDoc(
  schema: Schema,
  lang_model: DocLangModel,
  coord: SphereCoord,
  val user: String
) extends RealSphereDoc(schema, lang_model, coord) {
  def title = user

  def xmldesc =
    <TwitterUserDoc>
      <user>{ user }</user>
      {
        if (has_coord)
          <location>{ coord }</location>
      }
    </TwitterUserDoc>
}

class TwitterUserDocSubfactory(
  docfact: SphereDocFactory
) extends SphereDocSubfactory[TwitterUserDoc](docfact) {
  def create_and_init_document(schema: Schema, fieldvals: IndexedSeq[String],
      lang_model: DocLangModel, coord: SphereCoord,
      record_in_factory: Boolean) = Some(
    new TwitterUserDoc(schema, lang_model, coord,
        schema.get_value_or_else[String](fieldvals, "user", ""))
    )
}
