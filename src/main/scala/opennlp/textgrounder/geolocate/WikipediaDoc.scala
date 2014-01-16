///////////////////////////////////////////////////////////////////////////////
//  WikipediaDoc.scala
//
//  Copyright (C) 2010-2014 Ben Wing, The University of Texas at Austin
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

import util.collection._
import util.spherical._
import util.textdb.Schema
import util.error.warning
import util.print.errprint
import util.serialize.TextSerializer._
import util.string.capfirst
import util.textdb.Row
import util.debug._

import gridlocate._

/**
 * A document corresponding to a Wikipedia article.
 *
 * Defined fields for Wikipedia:
 *
 * id: Wikipedia article ID (for display purposes only).
 *
 * Other Wikipedia params that we mostly ignore, and don't record:
 *
 * namespace: Namespace of document (e.g. "Main", "Wikipedia", "File"); if
 *   the namespace isn't Main, we currently don't record the article at all,
 *   even if it has a coordinate (e.g. some images do)
 * is_list_of: Whether document title is "List of *"
 * is_disambig: Whether document is a disambiguation page.
 * is_list: Whether document is a list of any type ("List of *", disambig,
 *          or in Category or Book namespaces)
 */
class WikipediaDoc(
  schema: Schema,
  lang_model: DocLangModel,
  coord: SphereCoord,
  salience: Option[Double],
  val title: String,
  val id: Long
) extends RealSphereDoc(schema, lang_model, coord, salience) {
  override def get_field(field: String) = {
    field match {
      case "id" => id.toString
      case "incoming_links" => put_int_or_none(salience.map { _.toInt })
      case _ => super.get_field(field)
    }
  }

  override def toString = {
    "%s (id=%s)".format(super.toString, id)
  }
}

/**
 * Document subfactory for documents corresponding to Wikipedia articles.
 */
class WikipediaDocSubfactory(
  override val docfact: SphereDocFactory
) extends SphereDocSubfactory[WikipediaDoc](docfact) {
  override def create_document(row: Row, lang_model: DocLangModel,
      coord: SphereCoord) = {
    val namespace = row.gets_or_else("namepace", "")
    // docs with namespace != Main should be filtered during preproc
    assert(namespace == "" || namespace == "Main")
    new WikipediaDoc(row.schema, lang_model, coord,
      id = row.get_or_else[Long]("id", 0L),
      title = row.get_or_else[String]("title", ""),
      salience = row.get_if[Int]("incoming_links").map { _.toDouble }
    )
  }

  // val wikipedia_fields = Seq("incoming_links")

  /**
   * For each toponym, list of documents matching the name.
   */
  val lower_toponym_to_document = bufmap[String, WikipediaDoc]()

  /**
   * Compute the short form of a document name.  If short form includes a
   * division (e.g. "Tucson, Arizona"), return a tuple (SHORTFORM, DIVISION);
   * else return a tuple (SHORTFORM, None).
   */
  def compute_short_form(name: String) = {
    val includes_div_re = """(.*?), (.*)$""".r
    val includes_parentag_re = """(.*) \(.*\)$""".r
    name match {
      case includes_div_re(tucson, arizona) => (tucson, arizona)
      case includes_parentag_re(tucson, city) => (tucson, null)
      case _ => (name, null)
    }
  }

  /**
   * Add document to related lists mapping lowercased form, short form, etc.
   */ 
  override def record_training_document_in_subfactory(xdoc: SphereDoc) {
    val doc = xdoc.asInstanceOf[WikipediaDoc]
    val name = doc.title
    // Must pass in properly cased name
    // errprint("name=%s, capfirst=%s", name, capfirst(name))
    // println("length=%s" format name.length)
    // if (name.length > 1) {
    //   println("name(0)=0x%x" format name(0).toInt)
    //   println("name(1)=0x%x" format name(1).toInt)
    //   println("capfirst(0)=0x%x" format capfirst(name)(0).toInt)
    // }
    assert(name != null)
    assert(name.length > 0)
    assert(name == capfirst(name))
    val loname = name.toLowerCase
    val (short, div) = compute_short_form(loname)
    if (!(lower_toponym_to_document(loname) contains doc))
      lower_toponym_to_document(loname) += doc
    if (short != loname &&
        !(lower_toponym_to_document(short) contains doc))
      lower_toponym_to_document(short) += doc
  }

  def construct_candidates(toponym: String) = {
    lower_toponym_to_document(toponym.toLowerCase)
  }

  def word_is_toponym(word: String) = {
    lower_toponym_to_document contains word.toLowerCase
  }
}
