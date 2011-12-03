///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010, 2011 Ben Wing, The University of Texas at Austin
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
 * names, ID's and documents.
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

  def create_document(schema: Seq[String]): DocumentType

  def create_and_init_document(schema: Seq[String],
      fieldvals: Seq[String]) = {
    val doc = create_document(schema)
    if (doc != null)
      doc.set_fields(fieldvals)
    doc
  }

  def create_and_record_document(schema: Seq[String], fieldvals: Seq[String],
      cell_grid: CellGrid[CoordType,DocumentType,_]) = {
    val doc = create_and_init_document(schema, fieldvals)
    if (doc != null && doc.has_coord) {
      record_document(doc, cell_grid)
      true
    }
    else
      false
  }

  def record_document(doc: DocumentType,
    cell_grid: CellGrid[CoordType,DocumentType,_]) {
    documents_by_split(doc.split) += doc
    cell_grid.add_document_to_cell(doc)
  }

  /**
   * A file processor that reads corpora containing document metadata,
   * creates a DistDocument for each document described, and adds it to
   * this document table.
   *
   * @param schema fields of the document-data files, as determined from
   *   a schema file
   * @param suffix Suffix specifying the type of document file wanted
   *   (e.g. "counts" or "document-metadata"
   * @param cell_grid Cell grid to add newly created DistDocuments to
   */
  class DistDocumentFileProcessor(
    suffix: String, cell_grid: CellGrid[CoordType,DocumentType,_]
  ) extends GeoDocumentFileProcessor(suffix, driver) {
    def handle_document(fieldvals: Seq[String]) = {
      create_and_record_document(schema, fieldvals, cell_grid)
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

    override def end_processing(filehand: FileHandler,
        files: Iterable[String]) {
      finish_document_loading()
      super.end_processing(filehand, files)
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
   *   (e.g. "counts" or "document-metadata"
   * @param cell_grid Cell grid into which the documents are added.
   */
  def read_documents(filehand: FileHandler, dir: String, suffix: String,
      cell_grid: CellGrid[CoordType,DocumentType,_]) {

    val distproc = new DistDocumentFileProcessor(suffix, cell_grid)

    distproc.read_schema_from_corpus(filehand, dir)
    distproc.process_files(filehand, Seq(dir))
  }

  def clear_training_document_distributions() {
    for (doc <- documents_by_split("training"))
      doc.dist = null
  }

  def finish_document_loading() {
    // Compute overall distribution values (e.g. back-off statistics).
    word_dist_factory.finish_global_distribution()

    // Now compute per-document values dependent on the overall distribution
    // statistics just computed.
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

    // Now output statistics on number of documents seen, etc.
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
}

///////////////////////// DistDocuments

/**
 * A document for geolocation, with a word distribution.  Documents can come
 * from Wikipedia articles, individual tweets, Twitter feeds (all tweets from
 * a user), etc.
 */ 
abstract class DistDocument[CoordType : Serializer](
  schema: Seq[String],
  val table: DistDocumentTable[CoordType,_]
) extends GeoDocument[CoordType](schema) with EvaluationDocument {
  /**
   * Object containing word distribution of this document.
   */
  var dist: WordDist = _

  override def set_field(field: String, value: String) {
    if (field == "counts") {
      table.num_word_count_documents_by_split(this.split) += 1
      table.num_documents_with_word_counts += 1
      // Set the distribution on the document.  But, if we are evaluating on
      // the dev set, skip the test set and vice versa to save memory,
      // and don't use the eval set's distributions in computing global
      // smoothing values and such, to avoid contaminating the results.
      val is_training_set = (this.split == "training")
      val is_eval_set = (this.split == table.driver.params.eval_set)
      if (is_training_set || is_eval_set) {
        table.word_dist_factory.initialize_distribution(this, value,
          is_training_set)
      }
    } else super.set_field(field, value)
  }
  
  // FIXME: Write get_field() to convert counts the other way, for output
}

