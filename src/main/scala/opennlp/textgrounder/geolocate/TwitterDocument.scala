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

import opennlp.textgrounder.util.corpusutil.Schema

import opennlp.textgrounder.worddist.WordDist.memoizer._

class TwitterTweetDocument(
  schema: Schema,
  subtable: TwitterTweetDocumentSubtable
) extends SphereDocument(schema, subtable.table) {
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
  def create_document(schema: Schema) = new TwitterTweetDocument(schema, this)
}

class TwitterUserDocument(
  schema: Schema,
  subtable: TwitterUserDocumentSubtable
) extends SphereDocument(schema, subtable.table) {
  var userind = blank_memoized_string
  def title = unmemoize_string(userind)

  override def set_field(field: String, value: String) {
    field match {
      case "user" => userind = memoize_string(value)
      case _ => super.set_field(field, value)
    }
  }

  def struct =
    <TwitterUserDocument>
      <user>{ unmemoize_string(userind) }</user>
      {
        if (has_coord)
          <location>{ coord }</location>
      }
    </TwitterUserDocument>
}

class TwitterUserDocumentSubtable(
  table: SphereDocumentTable
) extends SphereDocumentSubtable[TwitterUserDocument](table) {
  def create_document(schema: Schema) = new TwitterUserDocument(schema, this)
}
