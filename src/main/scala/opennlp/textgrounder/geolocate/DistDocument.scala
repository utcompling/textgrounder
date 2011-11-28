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
//////// DistDocument.scala
////////
//////// Copyright (c) 2010, 2011 Ben Wing.
////////

package opennlp.textgrounder.geolocate

import collection.mutable
import util.matching.Regex

import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.distances._
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.ioutil.FileHandler
import opennlp.textgrounder.util.MeteredTask
import opennlp.textgrounder.util.osutil.output_resource_usage
import opennlp.textgrounder.util.printutil.errprint
import opennlp.textgrounder.util.Serializer
import opennlp.textgrounder.util.textutil.capfirst

/////////////////////////////////////////////////////////////////////////////
//                      Wikipedia/Twitter/etc. documents                   //
/////////////////////////////////////////////////////////////////////////////

//////////////////////  DistDocument table

/**
 * Class maintaining tables listing all documents and mapping between
 * names, ID's and documents.  Objects corresponding to redirect articles
 * in Wikipedia should not be present anywhere in this table; instead, the
 * name of the redirect article should point to the document object for the
 * article pointed to by the redirect.
 */
abstract class DistDocumentTable[CoordType : Serializer,
  DocumentType <: DistDocument[CoordType]](
  val driver: GeolocateDriver,
  val word_dist_factory: WordDistFactory
) {
  /**********************************************************************/
  /*                   Begin DistDocumentTable proper                   */
  /**********************************************************************/

  /**
   * Mapping from document names to DocumentType objects, using the actual case of
   * the document.
   */
  val name_to_document = mutable.Map[String, DocumentType]()

  /**
   * List of documents in each split.
   */
  val documents_by_split = bufmap[String, DocumentType]()

  /**
   * Num of documents with word-count information but not in table.
   */
  val num_documents_with_word_counts_but_not_in_table =
    new driver.TaskCounterWrapper("documents_with_word_counts_but_not_in_table")

  /**
   * Num of documents with word-count information (whether or not in table).
   */
  val num_documents_with_word_counts =
    new driver.TaskCounterWrapper("documents_with_word_counts")

  /** 
   * Num of documents in each split with word-count information seen.
   */
  val num_word_count_documents_by_split =
    driver.countermap("word_count_documents_by_split")

  /**
   * Num of documents in each split with a computed distribution.
   * (Not the same as the previous since we don't compute the distribution of
   * documents in either the test or dev set depending on which one is used.)
   */
  val num_dist_documents_by_split =
    driver.countermap("num_dist_documents_by_split")

  /**
   * Total # of word tokens for all documents in each split.
   */
  val word_tokens_by_split =
    driver.countermap("word_tokens_by_split")

  /**
   * Total # of incoming links for all documents in each split.
   */
  val incoming_links_by_split =
    driver.countermap("incoming_links_by_split")

  /**
   * Map from short name (lowercased) to list of documents.
   * The short name for a document is computed from the document's name.  If
   * the document name has a comma, the short name is the part before the
   * comma, e.g. the short name of "Springfield, Ohio" is "Springfield".
   * If the name has no comma, the short name is the same as the document
   * name.  The idea is that the short name should be the same as one of
   * the toponyms used to refer to the document.
   */
  val short_lower_name_to_documents = bufmap[String, DocumentType]()

  /**
   * Map from tuple (NAME, DIV) for documents of the form "Springfield, Ohio",
   * lowercased.
   */
  val lower_name_div_to_documents = bufmap[(String, String), DocumentType]()

  /**
   * For each toponym, list of documents matching the name.
   */
  val lower_toponym_to_document = bufmap[String, DocumentType]()

  /**
   * Mapping from lowercased document names to DocumentType objects
   */
  val lower_name_to_documents = bufmap[String, DocumentType]()

  /**
   * Schema 
   */
  /**
   * Look up a document named NAME and return the associated document.
   * Note that document names are case-sensitive but the first letter needs to
   * be capitalized.
   */
  def lookup_document(name: String) = {
    assert(name != null)
    name_to_document.getOrElse(capfirst(name), null.asInstanceOf[DocumentType])
  }

  /**
   * Record the document as having NAME as one of its names (there may be
   * multiple names, due to redirects).  Also add to related lists mapping
   * lowercased form, short form, etc.
   */ 
  def record_document_name(name: String, doc: DocumentType) {
    // Must pass in properly cased name
    // errprint("name=%s, capfirst=%s", name, capfirst(name))
    // println("length=%s" format name.length)
    // if (name.length > 1) {
    //   println("name(0)=0x%x" format name(0).toInt)
    //   println("name(1)=0x%x" format name(1).toInt)
    //   println("capfirst(0)=0x%x" format capfirst(name)(0).toInt)
    // }
    assert(name == capfirst(name))
    name_to_document(name) = doc
    val loname = name.toLowerCase
    lower_name_to_documents(loname) += doc
    val (short, div) = GeoDocument.compute_short_form(loname)
    if (div != null)
      lower_name_div_to_documents((short, div)) += doc
    short_lower_name_to_documents(short) += doc
    if (!(lower_toponym_to_document(loname) contains doc))
      lower_toponym_to_document(loname) += doc
    if (short != loname && !(lower_toponym_to_document(short) contains doc))
      lower_toponym_to_document(short) += doc
  }

  /**
   * Record either a normal document ('docfrom' same as 'docto') or a
   * redirect ('docfrom' redirects to 'docto').
   */
  def record_document(docfrom: DocumentType, docto: DocumentType) {
    record_document_name(docfrom.title, docto)
    val redir = !(docfrom eq docto)
    val split = docto.split
    val fromlinks = docfrom.adjusted_incoming_links
    incoming_links_by_split(split) += fromlinks
    if (!redir) {
      documents_by_split(split) += docto
    } else if (fromlinks != 0) {
      // Add count of links pointing to a redirect to count of links
      // pointing to the document redirected to, so that the total incoming
      // link count of a document includes any redirects to that document.
      docto.incoming_links = Some(docto.adjusted_incoming_links + fromlinks)
    }
  }

  def create_document(schema: Seq[String], fieldvals: Seq[String]):
      DocumentType

  def would_add_document_to_list(doc: DocumentType) = {
    if ((doc.params contains "namespace") && doc.params("namespace") != "Main")
      false
    else if (doc.redir.length > 0)
      false
    else doc.optcoord != None
  }

  /**
   * A file processor that reads corpora containing document metadata,
   * creates a DistDocument for each document described, and adds it to
   * this document table.
   *
   * @param schema fields of the document-data files, as determined from
   *   a schema file
   * @param suffix Suffix specifying the type of document file wanted
   *   (e.g. "-counts" or "-document-metadata"
   * @param cell_grid Cell grid to add newly created DistDocuments to
   */
  class DistDocumentFileProcessor(
    suffix: String, cell_grid: CellGrid[CoordType,DocumentType,_]
  ) extends GeoDocumentFileProcessor(suffix, driver) {
    /**
     * List of documents that are Wikipedia redirect articles.
     */
    val redirects = mutable.Buffer[DocumentType]()

    def handle_document(fieldvals: Seq[String]) = {
      val doc = create_document(schema, fieldvals)

      if (doc.redir.length > 0)
        redirects += doc
      else if (doc.optcoord != None) {
        record_document(doc, doc)
        cell_grid.add_document_to_cell(doc)
      }
      true
    }

    def process_lines(lines: Iterator[String],
        filehand: FileHandler, file: String,
        compression: String, realname: String) = {
      val task = new MeteredTask("document", "reading")
      // Stop if we've reached the maximum
      var should_stop = false
      for (line <- lines if !should_stop) {
        parse_row(line)
        if (task.item_processed(maxtime = driver.params.max_time_per_stage))
          should_stop = true
        if ((driver.params.num_training_docs > 0 &&
          task.num_processed >= driver.params.num_training_docs)) {
          errprint("")
          errprint("Stopping because limit of %s documents reached",
            driver.params.num_training_docs)
          should_stop = true
        }
      }
      task.finish()
      output_resource_usage()
      !should_stop
    }
  }

  /**
   * Read the documents from the given corpus.  Documents listed in
   * the document file(s) are created, listed in this table,
   * and added to the cell grid corresponding to the table.
   *
   * @param filehand The FileHandler for working with the file.
   * @param dir Directory containing the corpus.
   * @param suffix Suffix specifying the type of document file wanted
   *   (e.g. "-counts" or "-document-metadata"
   * @param cell_grid Cell grid into which the documents are added.
   */
  def read_documents(filehand: FileHandler, dir: String, suffix: String,
      cell_grid: CellGrid[CoordType,DocumentType,_]) {

    val distproc = new DistDocumentFileProcessor(suffix, cell_grid)

    distproc.read_schema_from_corpus(filehand, dir)
    distproc.process_files(filehand, Seq(dir))

    for (x <- distproc.redirects) {
      val reddoc = lookup_document(x.redir)
      if (reddoc != null)
        record_document(x, reddoc)
    }
  }

  def finish_document_distributions() {
    // Figure out the value of OVERALL_UNSEEN_MASS for each document.
    for ((split, table) <- documents_by_split) {
      var totaltoks = 0
      var numdocs = 0
      for (doc <- table) {
        if (doc.dist != null) {
          /* FIXME: Move this finish() earlier, and split into
             before/after global. */
          doc.dist.finish(minimum_word_count = driver.params.minimum_word_count)
          totaltoks += doc.dist.num_word_tokens
          numdocs += 1
        }
      }
      num_dist_documents_by_split(split) += numdocs
      word_tokens_by_split(split) += totaltoks
    }
  }

  def clear_training_document_distributions() {
    for (doc <- documents_by_split("training"))
      doc.dist = null
  }

  def finish_word_counts() {
    word_dist_factory.finish_global_distribution()
    finish_document_distributions()
    errprint("")
    errprint("-------------------------------------------------------------------------")
    errprint("Document count statistics:")
    var total_docs_in_table = 0L
    var total_docs_with_word_counts = 0L
    var total_docs_with_dists = 0L
    for ((split, totaltoks) <- word_tokens_by_split) {
      errprint("For split '%s':", split)
      val docs_in_table = documents_by_split(split).length
      val docs_with_word_counts = num_word_count_documents_by_split(split).value
      val docs_with_dists = num_dist_documents_by_split(split).value
      total_docs_in_table += docs_in_table
      total_docs_with_word_counts += docs_with_word_counts
      total_docs_with_dists += docs_with_dists
      errprint("  %s documents in document table", docs_in_table)
      errprint("  %s documents with word counts seen (and in table)", docs_with_word_counts)
      errprint("  %s documents with distribution computed, %s total tokens, %.2f tokens/document",
        docs_with_dists, totaltoks.value,
        // Avoid division by zero
        totaltoks.value.toDouble / (docs_in_table + 1e-100))
    }
    errprint("Total: %s documents with word counts seen",
      num_documents_with_word_counts.value)
    errprint("Total: %s documents in document table", total_docs_in_table)
    errprint("Total: %s documents with word counts seen but not in document table",
      num_documents_with_word_counts_but_not_in_table.value)
    errprint("Total: %s documents with word counts seen (and in table)",
      total_docs_with_word_counts)
    errprint("Total: %s documents with distribution computed",
      total_docs_with_dists)
  }

  def construct_candidates(toponym: String) = {
    val lotop = toponym.toLowerCase
    lower_toponym_to_document(lotop)
  }

  def word_is_toponym(word: String) = {
    val lw = word.toLowerCase
    lower_toponym_to_document contains lw
  }
}

///////////////////////// DistDocuments

/**
 * A document for geolocation, with a word distribution.  Documents can come
 * from Wikipedia articles, individual tweets, Twitter feeds (all tweets from
 * a user), etc.
 */ 
abstract class DistDocument[CoordType : Serializer](
  schema: Seq[String],
  fieldvals: Seq[String],
  val table: DistDocumentTable[CoordType,_]
) extends GeoDocument[CoordType](schema, fieldvals) with EvaluationDocument {
  /**
   * Object containing word distribution of this document.
   */
  var dist: WordDist = _

  override def handle_parameter(name: String, v: String) = {
    if (name == "counts") {
      table.num_word_count_documents_by_split(this.split) += 1
      table.num_documents_with_word_counts += 1
      // Set the distribution on the document.  But, if we are evaluating on
      // the dev set, skip the test set and vice versa to save memory,
      // and don't use the eval set's distributions in computing global
      // smoothing values and such, to avoid contaminating the results.
      val is_training_set = (this.split == "training")
      val is_eval_set = (this.split == table.driver.params.eval_set)
      if (is_training_set || is_eval_set) {
        table.word_dist_factory.initialize_distribution(this, v,
          is_training_set)
      }
      true
    } else super.handle_parameter(name, v)
  }

  // def __repr__() = "DistDocument(%s)" format toString.encode("utf-8")

  def shortstr() = "%s" format title

  def struct() =
    <DistDocument>
      <title>{ title }</title>
      <id>{ id }</id>
      {
        if (optcoord != None)
          <location>{ coord }</location>
      }
      {
        if (redir.length > 0)
          <redirectTo>{ redir }</redirectTo>
      }
    </DistDocument>

  def distance_to_coord(coord2: CoordType): Double
}

class SphereDocument(
  schema: Seq[String],
  fieldvals: Seq[String],
  table: SphereDocumentTable
) extends DistDocument[SphereCoord](schema, fieldvals, table) {
  def distance_to_coord(coord2: SphereCoord) = spheredist(coord, coord2)
  def degree_distance_to_coord(coord2: SphereCoord) = degree_dist(coord, coord2)
}

class SphereDocumentTable(
  driver: GeolocateDriver,
  word_dist_factory: WordDistFactory
) extends DistDocumentTable[SphereCoord, SphereDocument](
  driver, word_dist_factory
) {
  def create_document(schema: Seq[String], fieldvals: Seq[String]) =
    new SphereDocument(schema, fieldvals, this)
}

