///////////////////////////////////////////////////////////////////////////////
//  WikipediaDoc.scala
//
//  Copyright (C) 2010-2013 Ben Wing, The University of Texas at Austin
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
import util.print.{errprint, warning}
import util.Serializer._
import util.text.capfirst

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

  def xmldesc =
    <WikipediaDoc>
      <title>{ title }</title>
      <id>{ id }</id>
      {
        if (has_coord)
          <location>{ coord }</location>
      }
    </WikipediaDoc>

  override def toString = {
    "%s (id=%s)".format(super.toString, id)
  }

  def adjusted_salience =
    WikipediaDoc.adjust_salience(salience)
}

object WikipediaDoc {
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

  def log_adjust_salience(salience: Double) = {
    if (salience == 0) // Whether from unknown count or count is actually zero
      0.01 // So we don't get errors from log(0)
    else salience
  }

  def adjust_salience(salience: Option[Double]) = {
    val ail =
      salience match {
        case None => {
          if (debug("some"))
            warning("Strange, object has no salience value")
          0.0
        }
        case Some(il) => {
          if (debug("some"))
            errprint("--> Salience is %s", il)
          il
        }
      }
    ail
  }
}

/**
 * Document subfactory for documents corresponding to Wikipedia articles.
 */
class WikipediaDocSubfactory(
  override val docfact: SphereDocFactory
) extends SphereDocSubfactory[WikipediaDoc](docfact) {
  override def create_and_init_document(schema: Schema,
      fieldvals: IndexedSeq[String], lang_model: DocLangModel,
      coord: SphereCoord, record_in_factory: Boolean) = {
    /* FIXME: Perhaps we should filter the document file when we generate it,
       to remove stuff not in the Main namespace. */
    val namespace = schema.get_field_or_else(fieldvals, "namepace", "")
    if (namespace != "" && namespace != "Main") {
      errprint("Skipped document %s, namespace %s is not Main",
        schema.get_field_or_else(fieldvals, "title", "unknown title??"),
        namespace)
      None
    } else {
      val doc = new WikipediaDoc(schema, lang_model, coord,
        id = schema.get_value_or_else[Long](fieldvals, "id", 0L),
        title = schema.get_value_or_else[String](fieldvals, "title", ""),
        salience =
          schema.get_value_if[Int](fieldvals, "incoming_links").map {
            _.toDouble })
      if (record_in_factory)
        record_document(doc)
      Some(doc)
    }
  }

  // val wikipedia_fields = Seq("incoming_links")

  /**
   * For each toponym, list of documents matching the name.
   */
  val lower_toponym_to_document = bufmap[String, WikipediaDoc]()

  /**
   * Add document to related lists mapping lowercased form, short form, etc.
   */ 
  def record_document(doc: WikipediaDoc) {
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
    val (short, div) = WikipediaDoc.compute_short_form(loname)
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
