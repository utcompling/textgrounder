///////////////////////////////////////////////////////////////////////////////
//  TwitterDocument.scala
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

package opennlp.textgrounder.geolocate

import opennlp.textgrounder.util.textdbutil.Schema

import opennlp.textgrounder.worddist.WordDistFactory
import opennlp.textgrounder.worddist.WordDist._

class TwitterTweetDocument(
  schema: Schema,
  word_dist_factory: WordDistFactory
) extends RealSphereDocument(schema, word_dist_factory) {
  var id = 0L
  def title = id.toString

  override def set_field(field: String, value: String) {
    field match {
      case "title" => id = value.toLong
      case _ => super.set_field(field, value)
    }
  }

  def struct =
    <TwitterTweetDocument>
      <id>{ id }</id>
      {
        if (has_coord)
          <location>{ coord }</location>
      }
    </TwitterTweetDocument>
}

class TwitterTweetDocumentSubtable(
  table: SphereDocumentTable
) extends SphereDocumentSubtable[TwitterTweetDocument](table) {
  def create_document(schema: Schema) =
    new TwitterTweetDocument(schema, table.word_dist_factory)
}

class TwitterUserDocument(
  schema: Schema,
  word_dist_factory: WordDistFactory
) extends RealSphereDocument(schema, word_dist_factory) {
  var userind = blank_memoized_string
  def title = memoizer.unmemoize(userind)

  override def set_field(field: String, value: String) {
    field match {
      case "user" => userind = memoizer.memoize(value)
      case _ => super.set_field(field, value)
    }
  }

  def struct =
    <TwitterUserDocument>
      <user>{ memoizer.unmemoize(userind) }</user>
      {
        if (has_coord)
          <location>{ coord }</location>
      }
    </TwitterUserDocument>
}

class TwitterUserDocumentSubtable(
  table: SphereDocumentTable
) extends SphereDocumentSubtable[TwitterUserDocument](table) {
  def create_document(schema: Schema) =
    new TwitterUserDocument(schema, table.word_dist_factory)
}
