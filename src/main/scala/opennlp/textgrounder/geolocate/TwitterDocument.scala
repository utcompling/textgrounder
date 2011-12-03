///////////////////////////////////////////////////////////////////////////////
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
//////// TwitterDocument.scala
////////
//////// Copyright (c) 2011 Ben Wing.
////////

package opennlp.textgrounder.geolocate

class TwitterDocument(
  schema: Seq[String],
  subtable: TwitterDocumentSubtable
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
    <TwitterDocument>
      <id>{ id }</id>
      {
        if (has_coord)
          <location>{ coord }</location>
      }
    </TwitterDocument>
}

class TwitterDocumentSubtable(
  table: SphereDocumentTable
) extends SphereDocumentSubtable[TwitterDocument](table) {
  def create_document(schema: Seq[String]) = new TwitterDocument(schema, this)
}
