///////////////////////////////////////////////////////////////////////////////
//  DistDocument.scala
//
//  Copyright (C) 2010, 2011, 2012 Ben Wing, The University of Texas at Austin
//  Copyright (C) 2011, 2012 Stephen Roller, The University of Texas at Austin
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

package opennlp.textgrounder.gridlocate

import collection.mutable
import util.matching.Regex
import util.control.Breaks._

import java.io._

import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.textdbutil._
import opennlp.textgrounder.util.distances._
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.ioutil._
import opennlp.textgrounder.util.osutil.output_resource_usage
import opennlp.textgrounder.util.printutil.{errprint, warning}
import opennlp.textgrounder.util.Serializer
import opennlp.textgrounder.util.textutil.capfirst

import opennlp.textgrounder.worddist.{WordDist,WordDistFactory}

import GridLocateDriver.Debug._

/////////////////////////////////////////////////////////////////////////////
//                          DistDocument tables                            //
/////////////////////////////////////////////////////////////////////////////

/**
 * A simple class holding properties referring to extra operations that
 * may be needed during document loading, depending on the particular
 * strategies and/or type of cell grids.  All extra operations start
 * out set to false.  If anyone requests extra, we do it.
 */
class DocumentLoadingProperties {
  var need_training_docs_in_memory_during_testing: Boolean = false
  var need_two_passes_over_training_docs: Boolean = false
  var need_dist_during_first_pass_over_training_docs: Boolean = false
  var need_pass_over_eval_docs_during_training: Boolean = false
  var need_dist_during_pass_over_eval_docs_during_training: Boolean = false
}
  
//////////////////////  DistDocument table

/**
 * Class maintaining tables listing all documents and mapping between
 * names, ID's and documents.
 */
abstract class DistDocumentTable[
  TCoord : Serializer,
  TDoc <: DistDocument[TCoord],
  TGrid <: CellGrid[TCoord,TDoc,_]
](
  /* SCALABUG!!! Declaring TDoc <: DistDocument[TCoord] isn't sufficient
     for Scala to believe that null is an OK value for TDoc, even though
     DistDocument is a reference type and hence TDoc must be reference.
   */
  val driver: GridLocateDriver,
  val word_dist_factory: WordDistFactory
) {
  /**
   * Properties indicating whether we need to do more than simply do a
   * single pass through training and eval documents.  Set by individual
   * strategies or cell grid types.
   */
  val loading_props = new DocumentLoadingProperties

  /**
   * List of documents in each split.
   */
  val documents_by_split = bufmap[String, TDoc]()

  // Example of using TaskCounterWrapper directly for non-split values.
  // val num_documents = new driver.TaskCounterWrapper("num_documents") 

  /** # of records seen in each split. */
  val num_records_by_split =
    driver.countermap("num_records_by_split")
  /** # of records skipped in each split due to errors */
  val num_error_skipped_records_by_split =
    driver.countermap("num_error_skipped_records_by_split")
  /** # of records skipped in each split, due to issues other than errors
    * (e.g. for Wikipedia documents, not being in the Main namespace).  */
  val num_non_error_skipped_records_by_split =
    driver.countermap("num_non_error_skipped_records_by_split")
  /** # of documents seen in each split.  This does not include skipped
    * records (see above).  */
  val num_documents_by_split =
    driver.countermap("num_documents_by_split")
  /** # of documents seen in each split skipped because lacking coordinates.
    * Note that although most callers skip documents without coordinates,
    * there are at least some cases where callers request to include such
    * documents.  */
  val num_documents_skipped_because_lacking_coordinates_by_split =
    driver.countermap("num_documents_skipped_because_lacking_coordinates_by_split")
  /** # of documents seen in each split skipped because lacking coordinates,
    * but which otherwise would have been recorded.  Note that although most
    * callers skip documents without coordinates, there are at least some
    * cases where callers request to include such documents.  In addition,
    * some callers do not ask for documents to be recorded (this happens
    * particularly with eval-set documents). */
  val num_would_be_recorded_documents_skipped_because_lacking_coordinates_by_split =
    driver.countermap("num_would_be_recorded_documents_skipped_because_lacking_coordinates_by_split")
  /** # of recorded documents seen in each split (i.e. those added to the
    * cell grid).  Non-recorded documents are generally those in the eval set.
    */
  val num_recorded_documents_by_split =
    driver.countermap("num_recorded_documents_by_split")
  /** # of documents in each split with coordinates. */
  val num_documents_with_coordinates_by_split =
    driver.countermap("num_documents_with_coordinates_by_split")
  /** # of recorded documents in each split with coordinates.  Non-recorded
    * documents are generally those in the eval set. */
  val num_recorded_documents_with_coordinates_by_split =
    driver.countermap("num_recorded_documents_with_coordinates_by_split")
  /** # of word tokens for documents seen in each split.  This does not
    * include skipped records (see above). */
  val word_tokens_of_documents_by_split =
    driver.countermap("word_tokens_of_documents_by_split")
  /** # of word tokens for documents seen in each split skipped because
    * lacking coordinates (see above). */
  val word_tokens_of_documents_skipped_because_lacking_coordinates_by_split =
    driver.countermap("word_tokens_of_documents_skipped_because_lacking_coordinates_by_split")
  /** # of word tokens for documents seen in each split skipped because
    * lacking coordinates, but which otherwise would have been recorded
    * (see above).  Non-recorded documents are generally those in the
    * eval set. */
  val word_tokens_of_would_be_recorded_documents_skipped_because_lacking_coordinates_by_split =
    driver.countermap("word_tokens_of_would_be_recorded_documents_skipped_because_lacking_coordinates_by_split")
  /** # of word tokens for recorded documents seen in each split (i.e.
    * those added to the cell grid).  Non-recorded documents are generally
    * those in the eval set. */
  val word_tokens_of_recorded_documents_by_split =
    driver.countermap("word_tokens_of_recorded_documents_by_split")
  /** # of word tokens for documents in each split with coordinates. */
  val word_tokens_of_documents_with_coordinates_by_split =
    driver.countermap("word_tokens_of_documents_with_coordinates_by_split")
  /** # of word tokens for recorded documents in each split with coordinates.
    * Non-recorded documents are generally those in the eval set. */
  val word_tokens_of_recorded_documents_with_coordinates_by_split =
    driver.countermap("word_tokens_of_recorded_documents_with_coordinates_by_split")

  def create_document(schema: Schema): TDoc

  /**
   * Implementation of `create_and_init_document`.  Subclasses should
   * override this if needed.  External callers should call
   * `create_and_init_document`, not this.  Note also that the
   * parameter `record_in_table` has a different meaning here -- it only
   * refers to recording in subsidiary tables, subclasses, etc.  The
   * wrapping function `create_and_init_document` takes care of recording
   * in the main table.
   */
  protected def imp_create_and_init_document(schema: Schema,
      fieldvals: Seq[String], record_in_table: Boolean) = {
    val doc = create_document(schema)
    if (doc != null)
      doc.set_fields(fieldvals)
    doc
  }

  /**
   * Create, initialize and return a document with the given fieldvals,
   * loaded from a corpus with the given schema.  Return value may be
   * null, meaning that the given record was skipped (e.g. due to erroneous
   * field values or for some other reason -- e.g. Wikipedia records not
   * in the Main namespace are skipped).
   *
   * @param schema Schema of the corpus from which the record was loaded
   * @param fieldvals Field values, taken from the record
   * @param record_in_table If true, record the document in the table and
   *   in any subsidiary tables, subclasses, etc.  This does not record
   *   the document in the cell grid; the caller needs to do that if
   *   needed.
   * @param must_have_coord If true, the document must have a coordinate;
   *   if not, it will be skipped, and null will be returned.
   */
  def create_and_init_document(schema: Schema, fieldvals: Seq[String],
      record_in_table: Boolean, must_have_coord: Boolean = true) = {
    val split = schema.get_field_or_else(fieldvals, "split", "unknown")
    if (record_in_table)
      num_records_by_split(split) += 1
    val doc = try {
      imp_create_and_init_document(schema, fieldvals, record_in_table)
    } catch {
      case e:Exception => {
        num_error_skipped_records_by_split(split) += 1
        throw e
      }
    }
    if (doc == null) {
      num_non_error_skipped_records_by_split(split) += 1
      doc 
    } else {
      assert(doc.split == split)
      assert(doc.dist != null)
      val double_tokens = doc.dist.model.num_tokens
      val tokens = double_tokens.toInt
      // Partial counts should not occur in training documents.
      assert(double_tokens == tokens)
      if (record_in_table) {
        num_documents_by_split(split) += 1
        word_tokens_of_documents_by_split(split) += tokens
      }
      if (!doc.has_coord && must_have_coord) {
        errprint("Document %s skipped because it has no coordinate", doc)
        num_documents_skipped_because_lacking_coordinates_by_split(split) += 1
        word_tokens_of_documents_skipped_because_lacking_coordinates_by_split(split) += tokens
        if (record_in_table) {
          num_would_be_recorded_documents_skipped_because_lacking_coordinates_by_split(split) += 1
          word_tokens_of_would_be_recorded_documents_skipped_because_lacking_coordinates_by_split(split) += tokens
        }
        // SCALABUG, same bug with null and a generic type inheriting from
        // a reference type
        null.asInstanceOf[TDoc]
      }
      if (doc.has_coord) {
        num_documents_with_coordinates_by_split(split) += 1
        word_tokens_of_documents_with_coordinates_by_split(split) += tokens
      }
      if (record_in_table) {
        // documents_by_split(split) += doc
        num_recorded_documents_by_split(split) += 1
        word_tokens_of_recorded_documents_by_split(split) += tokens
      }
      if (doc.has_coord && record_in_table) {
        num_recorded_documents_with_coordinates_by_split(split) += 1
        (word_tokens_of_recorded_documents_with_coordinates_by_split(split)
          += tokens)
      }
      doc
    }
  }

  /**
   * A file processor that reads corpora containing document metadata,
   * creates a DistDocument for each document described, and adds it to
   * this document table.
   *
   * @param suffix Suffix specifying the type of document file wanted
   *   (e.g. "counts" or "document-metadata"
   * @param cell_grid Cell grid to add newly created DistDocuments to
   */
  class DistDocumentTableFileProcessor(
    suffix: String, cell_grid: TGrid,
    task: ExperimentMeteredTask
  ) extends DistDocumentFileProcessor(suffix, driver) {
    def handle_document(fieldvals: Seq[String]) = {
      val doc = create_and_init_document(schema, fieldvals, true)
      if (doc != null) {
        assert(doc.dist != null)
        cell_grid.add_document_to_cell(doc)
        (true, true)
      }
      else (false, true)
    }

    def process_lines(lines: Iterator[String],
        filehand: FileHandler, file: String,
        compression: String, realname: String) = {
      // Stop if we've reached the maximum
      var should_stop = false
      breakable {
        for (line <- lines) {
          if (!parse_row(line))
            should_stop = true
          if (task.item_processed())
            should_stop = true
          if ((driver.params.num_training_docs > 0 &&
            task.num_processed >= driver.params.num_training_docs)) {
            errprint("")
            errprint("Stopping because limit of %s documents reached",
              driver.params.num_training_docs)
            should_stop = true
          }
          val sleep_at = debugval("sleep-at-docs")
          if (sleep_at != "") {
            if (task.num_processed == sleep_at.toInt) {
              errprint("Reached %d documents, sleeping ...")
              Thread.sleep(5000)
            }
          }
          if (should_stop)
            break
        }
      }
      (!should_stop, ())
    }
  }

  /**
   * Read the training documents from the given corpus.  Documents listed in
   * the document file(s) are created, listed in this table,
   * and added to the cell grid corresponding to the table.
   *
   * @param filehand The FileHandler for working with the file.
   * @param dir Directory containing the corpus.
   * @param suffix Suffix specifying the type of document file wanted
   *   (e.g. "counts" or "document-metadata"
   * @param cell_grid Cell grid into which the documents are added.
   */
  def read_training_documents(filehand: FileHandler, dir: String,
      suffix: String, cell_grid: TGrid) {

    for (pass <- 1 to cell_grid.num_training_passes) {
      cell_grid.begin_training_pass(pass)
      val task =
        new ExperimentMeteredTask(driver, "document", "reading pass " + pass,
              maxtime = driver.params.max_time_per_stage)
      val training_distproc =
        new DistDocumentTableFileProcessor("training-" + suffix, cell_grid, task)
      training_distproc.read_schema_from_textdb(filehand, dir)
      training_distproc.process_files(filehand, Seq(dir))
      task.finish()
      output_resource_usage()
    }
  }

  def clear_training_document_distributions() {
    for (doc <- documents_by_split("training"))
      doc.dist = null
  }

  def finish_document_loading() {
    // Compute overall distribution values (e.g. back-off statistics).
    errprint("Finishing global dist...")
    word_dist_factory.finish_global_distribution()

    // Now compute per-document values dependent on the overall distribution
    // statistics just computed.
    errprint("Finishing document dists...")
    for ((split, table) <- documents_by_split) {
      for (doc <- table) {
        if (doc.dist != null)
          doc.dist.finish_after_global()
      }
    }

    // Now output statistics on number of documents seen, etc.
    errprint("")
    errprint("-------------------------------------------------------------------------")
    errprint("Document/record/word token statistics:")

    var total_num_records = 0L
    var total_num_error_skipped_records = 0L
    var total_num_non_error_skipped_records = 0L
    var total_num_documents = 0L
    var total_num_documents_skipped_because_lacking_coordinates = 0L
    var total_num_would_be_recorded_documents_skipped_because_lacking_coordinates = 0L
    var total_num_recorded_documents = 0L
    var total_num_documents_with_coordinates = 0L
    var total_num_recorded_documents_with_coordinates = 0L
    var total_word_tokens_of_documents = 0L
    var total_word_tokens_of_documents_skipped_because_lacking_coordinates = 0L
    var total_word_tokens_of_would_be_recorded_documents_skipped_because_lacking_coordinates = 0L
    var total_word_tokens_of_recorded_documents = 0L
    var total_word_tokens_of_documents_with_coordinates = 0L
    var total_word_tokens_of_recorded_documents_with_coordinates = 0L
    for (split <- num_records_by_split.keys) {
      errprint("For split '%s':", split)

      val num_records = num_records_by_split(split).value
      errprint("  %s records seen", num_records)
      total_num_records += num_records

      val num_error_skipped_records =
        num_error_skipped_records_by_split(split).value
      errprint("  %s records skipped due to error seen",
        num_error_skipped_records)
      total_num_error_skipped_records += num_error_skipped_records

      val num_non_error_skipped_records =
        num_non_error_skipped_records_by_split(split).value
      errprint("  %s records skipped due to other than error seen",
        num_non_error_skipped_records)
      total_num_non_error_skipped_records += num_non_error_skipped_records

      def print_line(documents: String, num_documents: Long,
          num_tokens: Long) {
        errprint("  %s %s, %s total tokens, %.2f tokens/document",
          num_documents, documents, num_tokens,
          // Avoid division by zero
          num_tokens.toDouble / (num_documents + 1e-100))
      }

      val num_documents = num_documents_by_split(split).value
      val word_tokens_of_documents =
        word_tokens_of_documents_by_split(split).value
      print_line("documents seen", num_documents, word_tokens_of_documents)
      total_num_documents += num_documents
      total_word_tokens_of_documents += word_tokens_of_documents

      val num_recorded_documents =
        num_recorded_documents_by_split(split).value
      val word_tokens_of_recorded_documents =
        word_tokens_of_recorded_documents_by_split(split).value
      print_line("documents recorded", num_recorded_documents,
        word_tokens_of_recorded_documents)
      total_num_recorded_documents += num_recorded_documents
      total_word_tokens_of_recorded_documents +=
        word_tokens_of_recorded_documents

      val num_documents_skipped_because_lacking_coordinates =
        num_documents_skipped_because_lacking_coordinates_by_split(split).value
      val word_tokens_of_documents_skipped_because_lacking_coordinates =
        word_tokens_of_documents_skipped_because_lacking_coordinates_by_split(
          split).value
      print_line("documents skipped because lacking coordinates",
        num_documents_skipped_because_lacking_coordinates,
        word_tokens_of_documents_skipped_because_lacking_coordinates)
      total_num_documents_skipped_because_lacking_coordinates +=
        num_documents_skipped_because_lacking_coordinates
      total_word_tokens_of_documents_skipped_because_lacking_coordinates +=
        word_tokens_of_documents_skipped_because_lacking_coordinates

      val num_would_be_recorded_documents_skipped_because_lacking_coordinates =
        num_would_be_recorded_documents_skipped_because_lacking_coordinates_by_split(
        split).value
      val word_tokens_of_would_be_recorded_documents_skipped_because_lacking_coordinates =
        word_tokens_of_would_be_recorded_documents_skipped_because_lacking_coordinates_by_split(
          split).value
      print_line("would-be-recorded documents skipped because lacking coordinates",
        num_would_be_recorded_documents_skipped_because_lacking_coordinates,
        word_tokens_of_would_be_recorded_documents_skipped_because_lacking_coordinates)
      total_num_would_be_recorded_documents_skipped_because_lacking_coordinates +=
        num_would_be_recorded_documents_skipped_because_lacking_coordinates
      total_word_tokens_of_would_be_recorded_documents_skipped_because_lacking_coordinates +=
        word_tokens_of_would_be_recorded_documents_skipped_because_lacking_coordinates

      val num_documents_with_coordinates =
        num_documents_with_coordinates_by_split(split).value
      val word_tokens_of_documents_with_coordinates =
        word_tokens_of_documents_with_coordinates_by_split(split).value
      print_line("documents having coordinates seen",
        num_documents_with_coordinates,
        word_tokens_of_documents_with_coordinates)
      total_num_documents_with_coordinates += num_documents_with_coordinates
      total_word_tokens_of_documents_with_coordinates +=
        word_tokens_of_documents_with_coordinates

      val num_recorded_documents_with_coordinates =
        num_recorded_documents_with_coordinates_by_split(split).value
      val word_tokens_of_recorded_documents_with_coordinates =
        word_tokens_of_recorded_documents_with_coordinates_by_split(split).value
      print_line("documents having coordinates recorded",
        num_recorded_documents_with_coordinates,
        word_tokens_of_recorded_documents_with_coordinates)
      total_num_recorded_documents_with_coordinates +=
        num_recorded_documents_with_coordinates
      total_word_tokens_of_recorded_documents_with_coordinates +=
        word_tokens_of_recorded_documents_with_coordinates
    }

    errprint("Total: %s records, %s skipped records (%s from error)",
      total_num_records,
      (total_num_error_skipped_records + total_num_non_error_skipped_records),
      total_num_error_skipped_records)
    errprint("Total: %s documents with %s total word tokens",
      total_num_documents, total_word_tokens_of_documents)
    errprint("Total: %s documents skipped because lacking coordinates,\n       with %s total word tokens",
      total_num_documents_skipped_because_lacking_coordinates,
      total_word_tokens_of_documents_skipped_because_lacking_coordinates)
    errprint("Total: %s would-be-recorded documents skipped because lacking coordinates,\n       with %s total word tokens",
      total_num_would_be_recorded_documents_skipped_because_lacking_coordinates,
      total_word_tokens_of_would_be_recorded_documents_skipped_because_lacking_coordinates)
    errprint("Total: %s recorded documents with %s total word tokens",
      total_num_recorded_documents, total_word_tokens_of_recorded_documents)
    errprint("Total: %s documents having coordinates with %s total word tokens",
      total_num_documents_with_coordinates,
      total_word_tokens_of_documents_with_coordinates)
    errprint("Total: %s recorded documents having coordinates with %s total word tokens",
      total_num_recorded_documents_with_coordinates,
      total_word_tokens_of_recorded_documents_with_coordinates)
  }
}

/////////////////////////////////////////////////////////////////////////////
//                             DistDocuments                               //
/////////////////////////////////////////////////////////////////////////////

/**
 * An exception thrown to indicate an error during document creation
 * (typically due to a bad field value).
 */
case class DocumentValidationException(
  message: String,
  cause: Option[Throwable] = None
) extends Exception(message) {
  if (cause != None)
    initCause(cause.get)

  /**
   * Alternate constructor.
   *
   * @param message  exception message
   */
  def this(msg: String) = this(msg, None)

  /**
   * Alternate constructor.
   *
   * @param message  exception message
   * @param cause    wrapped, or nested, exception
   */
  def this(msg: String, cause: Throwable) = this(msg, Some(cause))
}

/**
 * A document with an associated coordinate placing it in a grid, with a word
 * distribution describing the text of the document.  For a "coordinate"
 * referring to a location on the Earth, documents can come from Wikipedia
 * articles, individual tweets, Twitter feeds (all tweets from a user), book
 * chapters from travel stories, etc.  For a "coordinate" referring to a
 * point in time, documents might be biographical entries in an encyclopedia,
 * snippets of text surrounding a date from an arbitrary web source, etc.
 *
 * The fields available depend on the source of the document (e.g. Wikipedia,
 * Twitter, etc.).
 *
 * Defined general fields:
 *
 * corpus: Corpus of the document (stored as a fixed field in the schema).
 * corpus-type: Corpus type of the corpus ('wikipedia', 'twitter', stored
 *   as a fixed field in the schema.
 * title: Title of document.  The combination of (corpus, title) needs to
 *   uniquely identify a document.
 * coord: Coordinates of document.
 * split: Evaluation split of document ("training", "dev", "test"), usually
 *   stored as a fixed field in the schema.
 */
abstract class DistDocument[TCoord : Serializer](
  val schema: Schema,
  val table: DistDocumentTable[TCoord,_,_]
) {

  import DistDocumentConverters._

  /**
   * Title of the document -- something that uniquely identifies it,
   * at least within all documents with a given corpus name. (The combination
   * of corpus and title uniquely identifies the document.) Title can be
   * taken from an actual title (e.g. in Wikipedia) or ID of some sort
   * (e.g. a tweet ID), or simply taken from an assigned serial number.
   * The underlying representation can be arbitrary -- `title` is just a
   * function to produce a string representation.
   *
   * FIXME: The corpus name is stored in the schema, but we don't currently
   * make any attempt to verify that corpus names are unique or implement
   * any operations involving corpus names -- much less verify that titles
   * are in fact unique for a given corpus name.  There's in general no way
   * to look up a document by title -- perhaps this is good since such
   * lookup adds a big hash table and entails storing the documents to
   * begin with, both of which things we want to avoid when possible.
   */
  def title: String
  /**
   * True if this document has a coordinate associated with it.
   */
  def has_coord: Boolean
  /**
   * Return the coordinate of a document with a coordinate.  Results are
   * undefined if the document has no coordinate (e.g. it might throw an
   * error or return a default value such as `null`).
   */
  def coord: TCoord
  /**
   * Return the evaluation split ("training", "dev" or "test") of the document.
   * This was created before corpora were sub-divided by the value of this
   * field, and hence it could be a property of the document.  It's now a
   * fixed value in the schema, but the field remains.
   */
  def split = schema.get_fixed_field("split", error_if_missing = true)
  /**
   * If this document has an incoming-link value associated with it (i.e.
   * number of links pointing to it in some sort of link structure), return
   * Some(NUM-LINKS); else return None.
   *
   * FIXME: This is used to establish a prior for the Naive Bayes strategy,
   * and is computed on a cell level, which is why we have it here; but it
   * seems too specific and tied to Wikipedia.  Also, perhaps we want to
   * split it into has_incoming_links and incoming_links (without the Option[]
   * wrapping).  We also need some comment of "corpus type" and a way to
   * request whether a given corpus type has incoming links marked on it.
   */
  def incoming_links: Option[Int] = None

  /**
   * Object containing word distribution of this document.
   */
  var dist: WordDist = _

  /**
   * Set the fields of the document to the given values.
   *
   * @param fieldvals A list of items, of the same length and in the same
   *   order as the corresponding schema.
   *
   * Note that we don't include the field values as a constructor parameter
   * and set them during construction, because we run into bootstrapping
   * problems.  In particular, if subclasses declare and initialize
   * additional fields, then those fields get initialized *after* the
   * constructor runs, in which case the values determined from the field
   * values get *overwritten* with their default values.  Subtle and bad.
   * So instead we make it so that the call to `set_fields` has to happen
   * *after* construction.
   */
  def set_fields(fieldvals: Seq[String]) {
    for ((field, value) <- (schema.fieldnames zip fieldvals)) {
      if (debug("rethrow"))
        set_field(field, value)
      else {
        try { set_field(field, value) }
        catch {
          case e@_ => {
            val msg = ("Bad value %s for field '%s': %s" format
                       (value, field, e.toString))
            if (debug("stack-trace") || debug("stacktrace"))
              e.printStackTrace
            throw new DocumentValidationException(msg, e)
          }
        }
      }
    }
  }

  def get_fields(fields: Seq[String]) = {
    for (field <- fields;
         value = get_field(field);
         if value != null)
      yield value
  }

  def set_field(field: String, value: String) {
    field match {
      case "counts" => {
        // Set the distribution on the document.  But don't use the eval
        // set's distributions in computing global smoothing values and such,
        // to avoid contaminating the results (training on your eval set).
        // In addition, if this isn't the training or eval set, we shouldn't
        // be loading at all.
        val is_training_set = (this.split == "training")
        val is_eval_set = (this.split == table.driver.params.eval_set)
        assert (is_training_set || is_eval_set)
        table.word_dist_factory.constructor.initialize_distribution(this,
          value, is_training_set)
        dist.finish_before_global()
      }
      case _ => () // Just eat the other parameters
    }
  }

  def get_field(field: String) = {
    field match {
      case "title" => title
      case "coord" => if (has_coord) put_x(coord) else null
      case _ => null
    }
  }

  // def __repr__ = "DistDocument(%s)" format toString.encode("utf-8")

  def shortstr = "%s" format title

  override def toString = {
    val coordstr = if (has_coord) " at %s".format(coord) else ""
    val corpus_name = schema.get_fixed_field("corpus-name")
    val corpusstr = if (corpus_name != null) "%s/".format(corpus_name) else ""
    "%s%s%s".format(corpusstr, title, coordstr)
  }

  def struct: scala.xml.Elem

  def distance_to_coord(coord2: TCoord): Double

  /**
   * Output a distance with attached units
   */
  def output_distance(dist: Double): String
}


/////////////////////////////////////////////////////////////////////////////
//                           Conversion functions                          //
/////////////////////////////////////////////////////////////////////////////

object DistDocumentConverters {
  def yesno_to_boolean(foo: String)  = {
    foo match {
      case "yes" => true
      case "no" => false
      case _ => {
        warning("Expected yes or no, saw '%s'", foo)
        false
      }
    }
  }
  def boolean_to_yesno(foo: Boolean) = if (foo) "yes" else "no"
  
  def get_int_or_none(foo: String) =
    if (foo == "") None else Option[Int](foo.toInt)
  def put_int_or_none(foo: Option[Int]) = {
    foo match {
      case None => ""
      case Some(x) => x.toString
    }
  }

  /**
   * Convert an object of type `T` into a serialized (string) form, for
   * storage purposes in a text file.  Note that the construction
   * `T : Serializer` means essentially "T must have a Serializer".
   * More technically, it adds an extra implicit parameter list with a
   * single parameter of type Serializer[T].  When the compiler sees a
   * call to put_x[X] for some type X, it looks in the lexical environment
   * to see if there is an object in scope of type Serializer[X] that is
   * marked `implicit`, and if so, it gives the implicit parameter
   * the value of that object; otherwise, you get a compile error.  The
   * function can then retrieve the implicit parameter's value using the
   * construction `implicitly[Serializer[T]]`.  The `T : Serializer`
   * construction is technically known as a *context bound*.
   */
  def put_x[T : Serializer](foo: T) =
    implicitly[Serializer[T]].serialize(foo)
  /**
   * Convert the serialized form of the value of an object of type `T`
   * back into that type.  Throw an error if an invalid string was seen.
   * See `put_x` for a description of the `Serializer` type and the *context
   * bound* (denoted by a colon) that ties it to `T`.
   *
   * @see put_x
   */
  def get_x[T : Serializer](foo: String) =
    implicitly[Serializer[T]].deserialize(foo)

  /**
   * Convert an object of type `Option[T]` into a serialized (string) form.
   * See `put_x` for more information.  The only difference between that
   * function is that if the value is None, a blank string is written out;
   * else, for a value Some(x), where `x` is a value of type `T`, `x` is
   * written out using `put_x`.
   *
   * @see put_x
   */
  def put_x_or_none[T : Serializer](foo: Option[T]) = {
    foo match {
      case None => ""
      case Some(x) => put_x[T](x)
    }
  }
  /**
   * Convert a blank string into None, or a valid string that converts into
   * type T into Some(value), where value is of type T.  Throw an error if
   * a non-blank, invalid string was seen.
   *
   * @see get_x
   * @see put_x
   */
  def get_x_or_none[T : Serializer](foo: String) =
    if (foo == "") None
    else Option[T](get_x[T](foo))

  /**
   * Convert an object of type `T` into a serialized (string) form.
   * If the object has the value `null`, write out a blank string.
   * Note that T must be a reference type (i.e. not a primitive
   * type such as Int or Double), so that `null` is a valid value.
   * 
   * @see put_x
   * @see put_x
   */
  def put_x_or_null[T >: Null : Serializer](foo: T) = {
    if (foo == null) ""
    else put_x[T](foo)
  }
  /**
   * Convert a blank string into null, or a valid string that converts into
   * type T into that value.  Throw an error if a non-blank, invalid string
   * was seen.  Note that T must be a reference type (i.e. not a primitive
   * type such as Int or Double), so that `null` is a valid value.
   *
   * @see get_x
   * @see put_x
   */
  def get_x_or_null[T >: Null : Serializer](foo: String) =
    if (foo == "") null
    else get_x[T](foo)
}

/////////////////////////////////////////////////////////////////////////////
//                       DistDocument File Processors                      //
/////////////////////////////////////////////////////////////////////////////

/**
 * A file processor that reads document files from a corpora.
 *
 * @param suffix Suffix used for selecting the particular corpus from a
 *  directory
 * @param dstats ExperimentDriverStats used for recording counters and such.
 *   Pass in null to not record counters.
 */
abstract class DistDocumentFileProcessor(
  suffix: String,
  val dstats: ExperimentDriverStats
) extends BasicTextDBProcessor[Unit](suffix) {

  /******** Counters to track what's going on ********/

  var shortfile: String = _

  def get_shortfile = shortfile

  def filename_to_counter_name(filehand: FileHandler, file: String) = {
    var (_, base) = filehand.split_filename(file)
    breakable {
      while (true) {
        val newbase = """\.[a-z0-9]*$""".r.replaceAllIn(base, "")
        if (newbase == base) break
        base = newbase
      }
    }
    """[^a-zA-Z0-9]""".r.replaceAllIn(base, "_") 
  }

  def get_file_counter_name(counter: String) =
    "byfile." + get_shortfile + "." + counter

  def increment_counter(counter: String, value: Long = 1) {
    if (dstats != null) {
      val file_counter = get_file_counter_name(counter)
      dstats.increment_task_counter(file_counter, value)
      dstats.increment_task_counter(counter, value)
      dstats.increment_local_counter(file_counter, value)
      dstats.increment_local_counter(counter, value)
    }
  }

  def increment_document_counter(counter: String) {
    increment_counter(counter)
    increment_counter("documents.total")
  }

  /******** Main code ********/

  /**
   * Handle (e.g. create and record) a document.
   *
   * @param fieldvals Field values of the document as read from the
   *   document file.
   * @return Tuple `(processed, keep_going)` where `processed` indicates
   *   whether the document was processed to completion (rather than skipped)
   *   and `keep_going` indicates whether processing of further documents
   *   should continue or stop.  Note that the value of `processed` does
   *   *NOT* indicate whether there is an error in the field values.  In
   *   that case, the error should be caught and rethrown as a
   *   DocumentValidationException, listing the field and value as well
   *   as the error.
   */
  def handle_document(fieldvals: Seq[String]): (Boolean, Boolean)

  override def handle_bad_row(line: String, fieldvals: Seq[String]) {
    increment_document_counter("documents.bad")
    super.handle_bad_row(line, fieldvals)
  }

  def process_row(fieldvals: Seq[String]): (Boolean, Boolean) = {
    val (processed, keep_going) =
      try { handle_document(fieldvals) }
      catch {
        case e:DocumentValidationException => {
          warning("Line %s: %s", num_processed + 1, e.message)
          return (false, true)
        }
      }
    if (processed)
      increment_document_counter("documents.processed")
    else {
      errprint("Skipped document %s",
        schema.get_field_or_else(fieldvals, "title", "unknown title??"))
      increment_document_counter("documents.skipped")
    }
    return (true, keep_going)
  }

  override def begin_process_file(filehand: FileHandler, file: String) {
    shortfile = filename_to_counter_name(filehand, file)
    super.begin_process_file(filehand, file)
  }

  override def end_process_file(filehand: FileHandler, file: String) {
    def note(counter: String, english: String) {
      if (dstats != null) {
        val file_counter = get_file_counter_name(counter)
        val value = dstats.get_task_counter(file_counter)
        errprint("Number of %s for file %s: %s", english, file,
          value)
      }
    }

    if (debug("per-document")) {
      note("documents.processed", "documents processed")
      note("documents.skipped", "documents skipped")
      note("documents.bad", "bad documents")
      note("documents.total", "total documents")
    }
    super.end_process_file(filehand, file)
  }
}

/**
 * A writer class for writing DistDocuments out to a corpus.
 *
 * @param schema schema describing the fields in the document files
 * @param suffix suffix used for identifying the particular corpus in a
 *  directory
 */
class DistDocumentWriter[TCoord : Serializer](
  schema: Schema,
  suffix: String
) extends TextDBWriter(schema, suffix) {
  def output_document(outstream: PrintStream, doc: DistDocument[TCoord]) {
    schema.output_row(outstream, doc.get_fields(schema.fieldnames))
  }
}
