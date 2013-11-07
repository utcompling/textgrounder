///////////////////////////////////////////////////////////////////////////////
//  WikipediaDoc.scala
//
//  Copyright (C) 2010, 2011, 2012 Ben Wing, The University of Texas at Austin
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
 * salience: Salience value (number of incoming links), or None if unknown.
 * redir: If this is a redirect, document title that it redirects to; else
 *          an empty string.
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
  val title: String,
  val redir: String,
  // FIXME! Make this a val and rename directly to 'salience'.
  // This requires handling all the redirect crap during preprocessing.
  var salience_value: Option[Double] = None,
  val id: Long = 0L
) extends RealSphereDoc(schema, lang_model, coord, salience_value) {
  override def get_field(field: String) = {
    field match {
      case "id" => id.toString
      case "redir" => redir
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
      {
        if (redir.length > 0)
          <redirectTo>{ redir }</redirectTo>
      }
    </WikipediaDoc>

  override def toString = {
    val redirstr =
      if (redir.length > 0) ", redirect to %s".format(redir) else ""
    "%s (id=%s%s)".format(super.toString, id, redirstr)
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
 *
 * Handling of redirect articles:
 *
 * FIXME! Such articles should not appear at all. The stuff below should
 * be done during preprocessing and then the redirect articles filtered
 * out.
 *
 * (1) Documents that are redirects to articles without geotags (i.e.
 *     coordinates) should have been filtered out during preprocessing;
 *     we want to keep only content articles with coordinates, and redirect
 *     articles to such articles. (But in the future we will want to make
 *     use of non-geotagged articles, e.g. in label propagation.)
 * (2) Documents that redirect to articles with coordinates have associated
 *     WikipediaDoc objects created for them.  These objects have their
 *     `redir` field set to the name of the article redirected to. (Objects
 *     for non-redirect articles have this field blank.)
 * (3) However, these objects should not appear in the lists of documents by
 *     split.
 * (4) When we read documents in, when we encounter a non-redirect article,
 *     we call `record_document` to record it, and add it to the cell grid.
 *     For redirect articles, however, we simply note them in a list, to be
 *     processed later.
 * (5) When we've loaded all documents, we go through the list of redirect
 *     articles and for each one, we look up the article pointed to and
 *     call `record_document` with the two articles.  We do it this way
 *     because we don't know the order in which we will load a redirecting
 *     vs. redirected-to article.
 * (6) The effect of the final `record_document` call for redirect articles
 *     is that (a) the salience (incoming-link count) of the redirecting
 *     article gets added to the redirected-to article, and (b) the name
 *     of the redirecting article gets recorded as an additional name of
 *     the redirected-to article. (FIXME: This should be done as a
 *     preprocessing step!)
 * (7) Note that currently we don't actually keep a mapping of all the names
 *     of a given WikipediaDoc; instead, we have tables that
 *     map names of various sorts to associated articles.  The articles
 *     pointed to in these maps are only content articles, except when there
 *     happen to be double redirects, i.e. redirects to other redirects.
 *     Wikipedia daemons actively remove such double redirects by pursuing
 *     the chain of redirects to the end.  We don't do such following
 *     ourselves; hence we may have some redirect articles listed in the
 *     maps. (FIXME, we should probably ignore these articles rather than
 *     record them.) Note that this means that we don't need the
 *     WikipediaDoc objects for redirect articles once we've finished
 *     loading the corpus; they should end up garbage collected.
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
        redir = schema.get_value_or_else[String](fieldvals, "redir", ""),
        title = schema.get_value_or_else[String](fieldvals, "title", ""),
        salience_value =
          schema.get_value_if[Int](fieldvals, "incoming_links").map {
            _.toDouble })
      if (doc.redir.length > 0) {
        if (record_in_factory)
          redirects += doc
        None
      } else {
        if (record_in_factory)
          record_document(doc, doc)
        Some(doc)
      }
    }
  }

  // val wikipedia_fields = Seq("incoming_links", "redir")

  /**
   * Mapping from document names to WikipediaDoc objects, using the actual
   * case of the document.
   */
  val name_to_document = mutable.Map[String, WikipediaDoc]()

  /**
   * For each toponym, list of documents matching the name.
   */
  val lower_toponym_to_document = bufmap[String, WikipediaDoc]()

  /**
   * Total salience for all documents in each split.
   */
  val salience_by_split =
    docfact.driver.countermap("salience_by_split")

  /**
   * List of documents that are Wikipedia redirect articles, accumulated
   * during loading and processed at the end.
   */
  val redirects = mutable.Buffer[WikipediaDoc]()

  /**
   * Look up a document named NAME and return the associated document.
   * Note that document names are case-sensitive but the first letter needs to
   * be capitalized.
   */
  def lookup_document(name: String) = {
    assert(name != null)
    assert(name.length > 0)
    name_to_document.getOrElse(capfirst(name), null.asInstanceOf[WikipediaDoc])
  }

  /**
   * Record the document as having NAME as one of its names (there may be
   * multiple names, due to redirects).  Also add to related lists mapping
   * lowercased form, short form, etc.
   */ 
  def record_document_name(name: String, doc: WikipediaDoc) {
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
    name_to_document(name) = doc
    val loname = name.toLowerCase
    val (short, div) = WikipediaDoc.compute_short_form(loname)
    if (!(lower_toponym_to_document(loname) contains doc))
      lower_toponym_to_document(loname) += doc
    if (short != loname &&
        !(lower_toponym_to_document(short) contains doc))
      lower_toponym_to_document(short) += doc
  }

  /**
   * Record either a normal document ('docfrom' same as 'docto') or a
   * redirect ('docfrom' redirects to 'docto').
   */
  def record_document(docfrom: WikipediaDoc, docto: WikipediaDoc) {
    record_document_name(docfrom.title, docto)

    // Handle salience (i.e. incoming link count).
    val split = docto.split
    val from_salience = docfrom.adjusted_salience
    salience_by_split(split) += from_salience.toLong
    if (docfrom.redir != "" && from_salience != 0) {
      // Add salience pointing to a redirect to salience pointing to the
      // document redirected to, so that the total salience of a document
      // includes any redirects to that document.
      docto.salience_value =
        Some(docto.adjusted_salience + from_salience)
    }
  }

  override def finish_document_loading() {
    for (x <- redirects) {
      val reddoc = lookup_document(x.redir)
      if (reddoc != null)
        record_document(x, reddoc)
    }
    /* FIXME: Consider setting the variable itself to null so that no
       further additions can happen, to catch bad code. */
    redirects.clear()
    super.finish_document_loading()
  }

  def construct_candidates(toponym: String) = {
    lower_toponym_to_document(toponym.toLowerCase)
  }

  def word_is_toponym(word: String) = {
    lower_toponym_to_document contains word.toLowerCase
  }
}
