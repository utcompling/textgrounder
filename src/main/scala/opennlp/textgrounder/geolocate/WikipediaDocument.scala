///////////////////////////////////////////////////////////////////////////////
//  WikipediaDocument.scala
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

package opennlp.textgrounder.geolocate

import collection.mutable

import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.textdbutil.Schema
import opennlp.textgrounder.util.printutil.{errprint, warning}
import opennlp.textgrounder.util.textutil.capfirst

import opennlp.textgrounder.gridlocate.DistDocumentConverters._
import opennlp.textgrounder.gridlocate.GridLocateDriver.Debug._

import opennlp.textgrounder.worddist.WordDist.memoizer._

/**
 * A document corresponding to a Wikipedia article.
 *
 * Defined fields for Wikipedia:
 *
 * id: Wikipedia article ID (for display purposes only).
 * incoming_links: Number of incoming links, or None if unknown.
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
class WikipediaDocument(
  schema: Schema,
  subtable: WikipediaDocumentSubtable
) extends SphereDocument(schema, subtable.table) {
  var id = 0L
  var incoming_links_value: Option[Int] = None
  override def incoming_links = incoming_links_value
  var redirind = blank_memoized_string
  def redir = unmemoize_string(redirind)
  var titleind = blank_memoized_string
  def title = unmemoize_string(titleind)

  override def set_field(field: String, value: String) {
    field match {
      case "id" => id = value.toLong
      case "title" => titleind = memoize_string(value)
      case "redir" => redirind = memoize_string(value)
      case "incoming_links" => incoming_links_value = get_int_or_none(value)
      case _ => super.set_field(field, value)
    }
  }

  override def get_field(field: String) = {
    field match {
      case "id" => id.toString
      case "redir" => redir
      case "incoming_links" => put_int_or_none(incoming_links)
      case _ => super.get_field(field)
    }
  }

  def struct =
    <WikipediaDocument>
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
    </WikipediaDocument>

  override def toString = {
    val redirstr =
      if (redir.length > 0) ", redirect to %s".format(redir) else ""
    "%s (id=%s%s)".format(super.toString, id, redirstr)
  }

  def adjusted_incoming_links =
    WikipediaDocument.adjust_incoming_links(incoming_links)
}

object WikipediaDocument {
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

  def log_adjust_incoming_links(links: Int) = {
    if (links == 0) // Whether from unknown count or count is actually zero
      0.01 // So we don't get errors from log(0)
    else links
  }

  def adjust_incoming_links(incoming_links: Option[Int]) = {
    val ail =
      incoming_links match {
        case None => {
          if (debug("some"))
            warning("Strange, object has no link count")
          0
        }
        case Some(il) => {
          if (debug("some"))
            errprint("--> Link count is %s", il)
          il
        }
      }
    ail
  }
}

/**
 * Document table for documents corresponding to Wikipedia articles.
 *
 * Handling of redirect articles:
 *
 * (1) Documents that are redirects to articles without geotags (i.e.
 *     coordinates) should have been filtered out during preprocessing;
 *     we want to keep only content articles with coordinates, and redirect
 *     articles to such articles. (But in the future we will want to make
 *     use of non-geotagged articles, e.g. in label propagation.)
 * (2) Documents that redirect to articles with coordinates have associated
 *     WikipediaDocument objects created for them.  These objects have their
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
 *     is that (a) the incoming-link count of the redirecting article gets
 *     added to the redirected-to article, and (b) the name of the redirecting
 *     article gets recorded as an additional name of the redirected-to
 *     article.
 * (7) Note that currently we don't actually keep a mapping of all the names
 *     of a given WikipediaDocument; instead, we have tables that
 *     map names of various sorts to associated articles.  The articles
 *     pointed to in these maps are only content articles, except when there
 *     happen to be double redirects, i.e. redirects to other redirects.
 *     Wikipedia daemons actively remove such double redirects by pursuing
 *     the chain of redirects to the end.  We don't do such following
 *     ourselves; hence we may have some redirect articles listed in the
 *     maps. (FIXME, we should probably ignore these articles rather than
 *     record them.) Note that this means that we don't need the
 *     WikipediaDocument objects for redirect articles once we've finished
 *     loading the table; they should end up garbage collected.
 */
class WikipediaDocumentSubtable(
  override val table: SphereDocumentTable
) extends SphereDocumentSubtable[WikipediaDocument](table) {
  def create_document(schema: Schema) = new WikipediaDocument(schema, this)

  override def create_and_init_document(schema: Schema, fieldvals: Seq[String],
      record_in_table: Boolean) = {
   /**
    * FIXME: Perhaps we should filter the document file when we generate it,
    * to remove stuff not in the Main namespace.  We also need to remove
    * the duplication between this function and would_add_document_to_list().
    */
    val namespace = schema.get_field_or_else(fieldvals, "namepace")
    if (namespace != null && namespace != "Main") {
      errprint("Skipped document %s, namespace %s is not Main",
        schema.get_field_or_else(fieldvals, "title", "unknown title??"),
        namespace)
      null
    } else {
      val doc = create_document(schema)
      doc.set_fields(fieldvals)
      if (doc.redir.length > 0) {
        if (record_in_table)
          redirects += doc
        null
      } else {
        if (record_in_table)
          record_document(doc, doc)
        doc
      }
    }
  }

  // val wikipedia_fields = Seq("incoming_links", "redir")

  /**
   * Mapping from document names to WikipediaDocument objects, using the actual
   * case of the document.
   */
  val name_to_document = mutable.Map[Word, WikipediaDocument]()

  /**
   * Map from short name (lowercased) to list of documents.
   * The short name for a document is computed from the document's name.  If
   * the document name has a comma, the short name is the part before the
   * comma, e.g. the short name of "Springfield, Ohio" is "Springfield".
   * If the name has no comma, the short name is the same as the document
   * name.  The idea is that the short name should be the same as one of
   * the toponyms used to refer to the document.
   */
  val short_lower_name_to_documents = bufmap[Word, WikipediaDocument]()

  /**
   * Map from tuple (NAME, DIV) for documents of the form "Springfield, Ohio",
   * lowercased.
   */
  val lower_name_div_to_documents =
    bufmap[(Word, Word), WikipediaDocument]()

  /**
   * For each toponym, list of documents matching the name.
   */
  val lower_toponym_to_document = bufmap[Word, WikipediaDocument]()

  /**
   * Mapping from lowercased document names to WikipediaDocument objects
   */
  val lower_name_to_documents = bufmap[Word, WikipediaDocument]()

  /**
   * Total # of incoming links for all documents in each split.
   */
  val incoming_links_by_split =
    table.driver.countermap("incoming_links_by_split")

  /**
   * List of documents that are Wikipedia redirect articles, accumulated
   * during loading and processed at the end.
   */
  val redirects = mutable.Buffer[WikipediaDocument]()

  /**
   * Look up a document named NAME and return the associated document.
   * Note that document names are case-sensitive but the first letter needs to
   * be capitalized.
   */
  def lookup_document(name: String) = {
    assert(name != null)
    assert(name.length > 0)
    name_to_document.getOrElse(memoize_string(capfirst(name)),
      null.asInstanceOf[WikipediaDocument])
  }

  /**
   * Record the document as having NAME as one of its names (there may be
   * multiple names, due to redirects).  Also add to related lists mapping
   * lowercased form, short form, etc.
   */ 
  def record_document_name(name: String, doc: WikipediaDocument) {
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
    name_to_document(memoize_string(name)) = doc
    val loname = name.toLowerCase
    val loname_word = memoize_string(loname)
    lower_name_to_documents(loname_word) += doc
    val (short, div) = WikipediaDocument.compute_short_form(loname)
    val short_word = memoize_string(short)
    if (div != null) {
      val div_word = memoize_string(div)
      lower_name_div_to_documents((short_word, div_word)) += doc
    }
    short_lower_name_to_documents(short_word) += doc
    if (!(lower_toponym_to_document(loname_word) contains doc))
      lower_toponym_to_document(loname_word) += doc
    if (short_word != loname_word &&
        !(lower_toponym_to_document(short_word) contains doc))
      lower_toponym_to_document(short_word) += doc
  }

  /**
   * Record either a normal document ('docfrom' same as 'docto') or a
   * redirect ('docfrom' redirects to 'docto').
   */
  def record_document(docfrom: WikipediaDocument, docto: WikipediaDocument) {
    record_document_name(docfrom.title, docto)

    // Handle incoming links.
    val split = docto.split
    val fromlinks = docfrom.adjusted_incoming_links
    incoming_links_by_split(split) += fromlinks
    if (docfrom.redir != "" && fromlinks != 0) {
      // Add count of links pointing to a redirect to count of links
      // pointing to the document redirected to, so that the total incoming
      // link count of a document includes any redirects to that document.
      docto.incoming_links_value =
        Some(docto.adjusted_incoming_links + fromlinks)
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
    val lw = memoize_string(toponym.toLowerCase)
    lower_toponym_to_document(lw)
  }

  def word_is_toponym(word: String) = {
    val lw = memoize_string(word.toLowerCase)
    lower_toponym_to_document contains lw
  }
}
