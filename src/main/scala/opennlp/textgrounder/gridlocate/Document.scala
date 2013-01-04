///////////////////////////////////////////////////////////////////////////////
//  GeoDoc.scala
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

package opennlp.textgrounder
package gridlocate

import scala.util.matching.Regex
import scala.util.control.Breaks._

import java.io._

import util.collection._
import util.textdb._
import util.distances._
import util.experiment._
import util.io._
import util.print.{errprint, warning, internal_error}
import util.Serializer
import util.text.capfirst

import worddist.{WordDist,WordDistFactory}

import GridLocateDriver.Debug._


/////////////////////////////////////////////////////////////////////////////
//                            Document loading                             //
/////////////////////////////////////////////////////////////////////////////


/**
 * Description of the status of attempting to read a document from some
 * external format in file `file` handled by `filehand`. `doc` is the document
 * (which will generally only be present when `status` == "processed");
 * `status` is "bad", "skipped" or "processed"; `reason` is a string
 * indicating the reason why something is bad or skipped; and `docdesc`
 * is a description of the document, useful especially for bad or skipped
 * documents, where it generally describes what the document looked like
 * at the point it was skipped or discovered bad.
 */
case class DocStatus[TDoc](
  filehand: FileHandler,
  file: String,
  lineno: Long,
  maybedoc: Option[TDoc],
  status: String,
  reason: String,
  docdesc: String
) {
  require((status == "processed") == (maybedoc != None))
  def map_result[NewDoc](f: TDoc => (Option[NewDoc], String, String, String)) = {
    maybedoc match {
      case Some(doc) => {
        val (doc2, status2, reason2, docdesc2) = f(doc)
        DocStatus(filehand, file, lineno, doc2, status2, reason2, docdesc2)
      }
      case None =>
        DocStatus[NewDoc](filehand, file, lineno, None, status, reason, docdesc)
    }
  }
}

/******** Counters to track what's going on ********/
class DocCounterTracker[T](
  shortfile: String,
  driver: ExperimentDriverStats
) {
  def get_file_counter_name(counter: String) =
    "byfile." + shortfile + "." + counter

  def increment_counter(counter: String, value: Long = 1) {
    if (driver != null) {
      val file_counter = get_file_counter_name(counter)
      driver.increment_task_counter(file_counter, value)
      driver.increment_task_counter(counter, value)
      driver.increment_local_counter(file_counter, value)
      driver.increment_local_counter(counter, value)
    }
  }

  def increment_document_counter(counter: String) {
    increment_counter(counter)
    increment_counter("documents.total")
  }

  def record_status(status: DocStatus[T]) {
    status.status match {
      case "bad" => increment_document_counter("documents.bad")
      case "skipped" => increment_document_counter("documents.skipped")
      case "processed" => increment_document_counter("documents.processed")
    }
  }

  def print_status(status: DocStatus[T]) {
    status.status match {
      case "skipped" =>
        errprint("Skipped document %s because %s", status.docdesc, status.reason)
      case "bad" =>
        errprint("Unable to load document %s because %s", status.docdesc,
          status.reason)
      case _ => {}
    }
  }

  def handle_status(status: DocStatus[T]): Option[T] = {
    record_status(status)
    print_status(status)
    status.maybedoc
  }

  def note_document_counters(file: String) {
    def note(counter: String, english: String) {
      val file_counter = get_file_counter_name(counter)
      val value = driver.get_task_counter(file_counter)
      errprint("Number of %s for file %s: %s", english, file,
        value)
    }

    note("documents.processed", "documents processed")
    note("documents.skipped", "documents skipped")
    note("documents.bad", "bad documents")
    note("documents.total", "total documents")
  }
}

class DocCounterTrackerFactory[T](driver: ExperimentDriverStats) {
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

  def create_tracker(shortname: String) =
    new DocCounterTracker[T](shortname, driver)

  def process_statuses(statuses: Iterator[DocStatus[T]]) = {
    val byfile = new GroupByIterator(statuses,
      (stat: DocStatus[T]) => (stat.filehand, stat.file))
    (for (((filehand, file), file_statuses) <- byfile) yield {
      val tracker = create_tracker(filename_to_counter_name(filehand, file))
      val results = file_statuses.flatMap(tracker.handle_status(_))
      if (debug("per-document")) {
        val output_stats =
          new SideEffectIterator { tracker.note_document_counters(file) }
        results ++ output_stats
      } else
        results
    }).flatten
  }
}

case class RawDocument(schema: Schema, fields: IndexedSeq[String])


/////////////////////////////////////////////////////////////////////////////
//                              Document factories                         //
/////////////////////////////////////////////////////////////////////////////

/**
 * Factory for creating documents and maintaining statistics and certain
 * other info about them.
 */
abstract class GeoDocFactory[Co : Serializer](
  val driver: GridLocateDriver[Co],
  val word_dist_factory: WordDistFactory
) {
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

  /**
   * Implementation of `create_and_init_document`.  Subclasses should
   * override this if needed.  External callers should call
   * `create_and_init_document`, not this.  Note also that the
   * parameter `record_in_subfactory` has a different meaning here -- it only
   * refers to recording in subsidiary factories, subclasses, etc.  The
   * wrapping function `create_and_init_document` takes care of recording
   * in the main factory.
   */
  protected def imp_create_and_init_document(schema: Schema,
      fieldvals: IndexedSeq[String], dist: WordDist, record_in_factory: Boolean
  ): Option[GeoDoc[Co]]

  /**
   * Create, initialize and return a document with the given fieldvals,
   * loaded from a corpus with the given schema.  Return value may be
   * None, meaning that the given record was skipped (e.g. due to erroneous
   * field values or for some other reason -- e.g. Wikipedia records not
   * in the Main namespace are skipped).
   *
   * @param schema Schema of the corpus from which the record was loaded
   * @param fieldvals Field values, taken from the record
   * @param record_in_factory If true, record the document in the factory and
   *   in any subsidiary factores, subclasses, etc.  This does not record
   *   the document in the cell grid; the caller needs to do that if
   *   needed.
   */
  def create_and_init_document(schema: Schema, fieldvals: IndexedSeq[String],
      record_in_factory: Boolean) = {
    val split = schema.get_field_or_else(fieldvals, "split", "unknown")
    if (record_in_factory)
      num_records_by_split(split) += 1
    val counts = schema.get_field(fieldvals, driver.word_count_field)

    def catch_doc_validation[T](body: => T) = {
      if (debug("rethrow"))
        body
      else {
        try {
          body
        } catch {
          case e:Exception => {
            num_error_skipped_records_by_split(split) += 1
            if (debug("stack-trace") || debug("stacktrace"))
              e.printStackTrace
            throw new DocValidationException(
              "Bad value for field: %s" format e, e)
          }
        }
      }
    }

    val dist = catch_doc_validation {
      word_dist_factory.constructor.create_distribution(counts)
    }
    val maybedoc = catch_doc_validation {
      imp_create_and_init_document(schema, fieldvals, dist, record_in_factory)
    }
    maybedoc match {
      case None => {
        num_non_error_skipped_records_by_split(split) += 1
        None
      }
      case Some(doc) => {
        assert(doc.split == split)
        val double_tokens = doc.dist.model.num_tokens
        val tokens = double_tokens.toInt
        // Partial counts should not occur in training documents.
        assert(double_tokens == tokens)
        if (record_in_factory) {
          num_documents_by_split(split) += 1
          word_tokens_of_documents_by_split(split) += tokens
        }
        if (!doc.has_coord) {
          errprint("Document %s skipped because it has no coordinate", doc)
          num_documents_skipped_because_lacking_coordinates_by_split(split) += 1
          word_tokens_of_documents_skipped_because_lacking_coordinates_by_split(split) += tokens
          if (record_in_factory) {
            num_would_be_recorded_documents_skipped_because_lacking_coordinates_by_split(split) += 1
            word_tokens_of_would_be_recorded_documents_skipped_because_lacking_coordinates_by_split(split) += tokens
          }
          None
        } else {
          num_documents_with_coordinates_by_split(split) += 1
          word_tokens_of_documents_with_coordinates_by_split(split) += tokens
          if (record_in_factory) {
            num_recorded_documents_by_split(split) += 1
            word_tokens_of_recorded_documents_by_split(split) += tokens
            num_recorded_documents_with_coordinates_by_split(split) += 1
            (word_tokens_of_recorded_documents_with_coordinates_by_split(split)
              += tokens)
          }
          maybedoc
        }
      }
    }
  }

  /**
   * Convert a raw document (describing the fields of the document) to
   * an actual document object.
   *
   * @param schema Schema for textdb, indicating names of fields, etc.
   * @param record_in_factory Whether to record the document in the document
   *  factory.
   * @return a DocStatus describing the document.
   */
  def raw_document_to_document_status(rawdoc: DocStatus[RawDocument],
      record_in_factory: Boolean, note_globally: Boolean
    ): DocStatus[GeoDoc[Co]] = {
    rawdoc map_result { rd =>
      try {
        create_and_init_document(rd.schema, rd.fields, record_in_factory) match {
          case None => {
            (None, "skipped", "unable to create document",
              rd.schema.get_field_or_else(rd.fields, "title",
                "unknown title??"))
          }
          case Some(doc) => {
            if (note_globally) {
              // Don't use the eval set's distributions in computing
              // global smoothing values and such, to avoid contaminating
              // the results (training on your eval set). In addition, if
              // this isn't the training or eval set, we shouldn't be
              // loading at all.
              //
              // Add the distribution to the global stats before
              // eliminating infrequent words through
              // `finish_before_global`.
              doc.word_dist_factory.note_dist_globally(doc.dist)
            }
            doc.dist.finish_before_global()
            (Some(doc), "processed", "", "")
          }
        }
      } catch {
        case e:DocValidationException => {
          warning("Line %s: %s", rawdoc.lineno, e.message)
          (None, "bad", "error validating document field", "")
        }
      }
    }
  }

  /**
   * Convert a line from a textdb database into a document, returning a
   * DocStatus describing the document.
   *
   * @param schema Schema for textdb, indicating names of fields, etc.
   * @param record_in_factory Whether to record the document in the document
   *  factory.
   */
  def line_to_document_status(filehand: FileHandler, file: String,
      line: String, lineno: Long, schema: Schema, record_in_factory: Boolean,
      note_globally: Boolean
    ): DocStatus[GeoDoc[Co]] = {
    raw_document_to_document_status(
      GeoDocFactory.line_to_raw_document(filehand, file, line, lineno, schema),
      record_in_factory, note_globally)
  }

  /**
   * Convert raw documents into document statuses.
   *
   * @param filehand The FileHandler for the directory of the corpus.
   * @param dir Directory containing the corpus.
   * @param suffix Suffix specifying a particular corpus if many exist in
   *   the same dir (e.g. "-dev")
   * @param record_in_factory Whether to record documents in any subfactories.
   *   (FIXME: This should be an add-on to the iterator.)
   * @param note_globally Whether to add each document's words to the global
   *   (e.g. back-off) distribution statistics.  Normally false, but may be
   *   true during bootstrapping of those statistics.
   * @param finish_globally Whether to compute statistics of the documents'
   *   distributions that depend on global (e.g. back-off) distribution
   *   statistics.  Normally true, but may be false during bootstrapping of
   *   those statistics.
   * @return Iterator over document statuses.
   */
  def raw_documents_to_document_statuses(
    rawdocs: Iterator[DocStatus[RawDocument]],
    record_in_subfactory: Boolean = false,
    note_globally: Boolean = false,
    finish_globally: Boolean = true
  ) = {
    val docstats =
      rawdocs map { rawdoc =>
        raw_document_to_document_status(rawdoc, record_in_subfactory,
          note_globally)
      }
    if (!finish_globally)
      docstats
    else
      docstats.map { stat =>
        stat.maybedoc.foreach { doc => doc.dist.finish_after_global() }
        stat
      }
  }

  def document_statuses_to_documents(
    docstats: Iterator[DocStatus[GeoDoc[Co]]]
  ) = {
    new DocCounterTrackerFactory[GeoDoc[Co]](driver).
      process_statuses(docstats)
  }

  def raw_documents_to_documents(
    rawdocs: Iterator[DocStatus[RawDocument]],
    record_in_subfactory: Boolean = false,
    note_globally: Boolean = false,
    finish_globally: Boolean = true
  ) = {
    val docstats = raw_documents_to_document_statuses(rawdocs,
      record_in_subfactory, note_globally, finish_globally)
    document_statuses_to_documents(docstats)
  }

  /**
   * Read the documents from a textdb corpus.
   *
   * @param filehand The FileHandler for the directory of the corpus.
   * @param dir Directory containing the corpus.
   * @param suffix Suffix specifying a particular corpus if many exist in
   *   the same dir (e.g. "-dev")
   * @param record_in_factory Whether to record documents in any subfactories.
   *   (FIXME: This should be an add-on to the iterator.)
   * @param note_globally Whether to add each document's words to the global
   *   (e.g. back-off) distribution statistics.  Normally false, but may be
   *   true during bootstrapping of those statistics.
   * @param finish_globally Whether to compute statistics of the documents'
   *   distributions that depend on global (e.g. back-off) distribution
   *   statistics.  Normally true, but may be false during bootstrapping of
   *   those statistics.
   * @return Iterator over document statuses.
   */
  def read_document_statuses_from_textdb(filehand: FileHandler, dir: String,
      suffix: String = "", record_in_subfactory: Boolean = false,
      note_globally: Boolean = false,
      finish_globally: Boolean = true) = {
    val rawdocs =
      GeoDocFactory.read_raw_documents_from_textdb(filehand, dir, suffix)
    raw_documents_to_document_statuses(rawdocs, record_in_subfactory,
      note_globally, finish_globally)
  }

  def read_documents_from_textdb(filehand: FileHandler, dir: String,
      suffix: String = "", record_in_subfactory: Boolean = false,
      note_globally: Boolean = false,
      finish_globally: Boolean = true) = {
    val docstats = read_document_statuses_from_textdb(filehand, dir, suffix,
      record_in_subfactory, note_globally, finish_globally)
    document_statuses_to_documents(docstats)
  }

  def finish_document_loading() {
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

object GeoDocFactory {
  /**
   * Convert a line from a textdb database into a raw document status,
   * listing the fields in the document.
   *
   * @param schema Schema for textdb, indicating names of fields, etc.
   */
  def line_to_raw_document(filehand: FileHandler, file: String, line: String,
      lineno: Long, schema: Schema): DocStatus[RawDocument] = {
    line_to_fields(line, lineno, schema) match {
      case Some(fields) =>
        DocStatus(filehand, file, lineno, Some(RawDocument(schema, fields)),
          "processed", "", "")
      case None =>
        DocStatus(filehand, file, lineno, None, "bad",
          "badly formatted database row", "")
    }
  }

  /**
   * Read the raw documents from a textdb corpus.
   *
   * @param filehand The FileHandler for the directory of the corpus.
   * @param dir Directory containing the corpus.
   * @param suffix Suffix specifying a particular corpus if many exist in
   *   the same dir (e.g. "-dev")
   * @return Iterator over document statuses.
   */
  def read_raw_documents_from_textdb(filehand: FileHandler, dir: String,
      suffix: String = ""): Iterator[DocStatus[RawDocument]] = {
    val (schema, files) =
      TextDB.get_textdb_files(filehand, dir, suffix)
    files.flatMap { file =>
      filehand.openr(file).zipWithIndex.map {
        case (line, idx) =>
          line_to_raw_document(filehand, file, line, idx + 1, schema)
      }
    }
  }
}

/////////////////////////////////////////////////////////////////////////////
//                                GeoDocs                                  //
/////////////////////////////////////////////////////////////////////////////

/**
 * An exception thrown to indicate an error during document creation
 * (typically due to a bad field value).
 */
case class DocValidationException(
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
 *
 * @param schema
 * @param word_dist_factory
 * @param dist Object containing word distribution of this document.
 */
abstract class GeoDoc[Co : Serializer](
  val schema: Schema,
  val word_dist_factory: WordDistFactory,
  val dist: WordDist
) {

  import util.Serializer._

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
  def coord: Co
  /**
   * Return the evaluation split ("training", "dev" or "test") of the document.
   * This was created before corpora were sub-divided by the value of this
   * field, and hence it could be a property of the document.  It's now a
   * fixed value in the schema, but the field remains.
   */
  def split = schema.get_fixed_field("split")
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

  def get_fields(fields: Iterable[String]) = fields map get_field

  /**
   * Return the value of the given field.
   */
  def get_field(field: String) = {
    field match {
      case "title" => title
      case "coord" => if (has_coord) put_x(coord) else ""
      case _ => internal_error("Unrecognized field name %s" format field)
    }
  }

  // def __repr__ = "GeoDoc(%s)" format toString.encode("utf-8")

  def shortstr = "%s" format title

  override def toString = {
    val coordstr = if (has_coord) " at %s".format(coord) else ""
    val corpus_name = schema.get_fixed_field_or_else("corpus-name", "unknown")
    val corpusstr = if (corpus_name != null) "%s/".format(corpus_name) else ""
    "%s%s%s".format(corpusstr, title, coordstr)
  }

  def xmldesc: scala.xml.Elem

  def distance_to_coord(coord2: Co): Double

  /**
   * Output a distance with attached units
   */
  def output_distance(dist: Double): String
}


/////////////////////////////////////////////////////////////////////////////
//                               GeoDocWriter                              //
/////////////////////////////////////////////////////////////////////////////

/**
 * A writer class for writing GeoDocs out to a corpus.
 *
 * @param schema schema describing the fields in the document files
 * @param suffix suffix used for identifying the particular corpus in a
 *  directory
 */
class GeoDocWriter[Co : Serializer](
  schema: Schema,
  suffix: String = ""
) extends TextDBWriter(schema, suffix) {
  def output_document(outstream: PrintStream, doc: GeoDoc[Co]) {
    outstream.println(schema.make_row(doc.get_fields(schema.fieldnames)))
  }
}
