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

package opennlp.textgrounder.geolocate

import util.control.Breaks._

import java.io._

import opennlp.textgrounder.util.distances._
import opennlp.textgrounder.util.experiment.ExperimentDriverStats
import opennlp.textgrounder.util.ioutil._
import opennlp.textgrounder.util.printutil.{errprint, warning}
import opennlp.textgrounder.util.Serializer

import GeolocateDriver.Debug._

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                               //
/////////////////////////////////////////////////////////////////////////////

case class DocumentValidationException(
  message: String
) extends Exception(message) { }

/**
 * A file processor that reads document files from a corpora.
 *
 * @param schema list of fields in the document files, as determined from
 *   a schema file
 * @param dstats ExperimentDriverStats used for 
 */
abstract class GeoDocumentFileProcessor(
  suffix: String,
  val dstats: ExperimentDriverStats
) extends DocumentCorpusFileProcessor(suffix) {

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
    val file_counter = get_file_counter_name(counter)
    dstats.increment_task_counter(file_counter, value)
    dstats.increment_task_counter(counter, value)
    dstats.increment_local_counter(file_counter, value)
    dstats.increment_local_counter(counter, value)
  }

  def increment_document_counter(counter: String) {
    increment_counter(counter)
    increment_counter("documents.total")
  }

  /******** Main code ********/

  /**
   * Handle (e.g. create and record) a document based on the given `fieldvals`.
   * Return true if a document was actually processed to completion (e.g.
   * created and recorded), false if skipped.  If an error occurs due to
   * invalid values, it should be caught and re-thrown as a
   * DocumentValidationException, listing the field and value as well as the
   * error.
   */
  def handle_document(fieldvals: Seq[String]): Boolean

  override def handle_bad_row(line: String, fieldvals: Seq[String]) {
    increment_document_counter("documents.bad")
    super.handle_bad_row(line, fieldvals)
  }

  def process_row(fieldvals: Seq[String]): Boolean = {
    // FIXME! This is ugly as hell.  This Wikipedia-specific stuff should
    // either be moved elsewhere, or more likely, we should filter the
    // document-data file when we generate it, to remove stuff not in the
    // Main namespace.  Furthermore, the same logic here is duplicated in
    // would_add_document_to_list() in DistDocumentTable, which also
    // contains a special case for redirect documents.  It really seems that
    // we should create a WikipediaDocument trait to encapsulate this stuff.
    val params = (schema zip fieldvals).toMap
    if ((params contains "namespace") &&
        params("namespace") != "Main") {
      errprint("Skipped document %s, namespace %s is not Main",
        params.getOrElse("title", "unknown title??"), params("namespace"))
      increment_document_counter("documents.skipped")
      return true
    } else {
      val was_accepted =
        try { handle_document(fieldvals) }
        catch {
          case e@DocumentValidationException(msg) => {
            warning("Line %s: %s", num_processed + 1, msg)
            return false
          }
        }
      if (was_accepted)
        increment_document_counter("documents.accepted")
      else {
        errprint("Skipped document %s",
          params.getOrElse("title", "unknown title??"))
        increment_document_counter("documents.skipped")
      }
      return true
    }
  }

  override def begin_process_file(filehand: FileHandler, file: String) {
    shortfile = filename_to_counter_name(filehand, file)
    super.begin_process_file(filehand, file)
  }

  override def end_process_file(filehand: FileHandler, file: String) {
    def note(counter: String, english: String) {
      val file_counter = get_file_counter_name(counter)
      val value = dstats.get_task_counter(file_counter)
      errprint("Number of %s for file %s: %s", english, file,
        value)
    }

    note("documents.accepted", "documents accepted")
    note("documents.skipped", "documents skipped")
    note("documents.bad", "bad documents")
    note("documents.total", "total documents")
    super.end_process_file(filehand, file)
  }
}

class GeoDocumentWriter[CoordType : Serializer](
  schema: Seq[String]
) extends FieldTextWriter(schema) {
  def output_document(outstream: PrintStream, doc: GeoDocument[CoordType]) {
    output_row(outstream, doc.get_fields(schema))
  }
}

/** A document for the purpose of geolocation.  The fields available
 * depend on the source of the document (e.g. Wikipedia, etc.).
 *
 * Defined general fields:
 *
 * corpus: Corpus of the document.
 * title: Title of document.  The combination of (corpus, title) needs to
 *   uniquely identify a document.
 * id: ID of document, as an int.
 * coord: Coordinates of document.
 * split: Split of document ("training", "dev", "test")
 * 
 * Defined fields for Wikipedia (FIXME, create a Wikipedia subclass for these):
 *
 * incoming_links: Number of incoming links, or None if unknown.
 * redir: If this is a redirect, document title that it redirects to; else
 *          an empty string.
 *
 * Other Wikipedia params (we pay attention to "namespace" if present):
 *
 * namespace: Namespace of document (e.g. "Main", "Wikipedia", "File")
 * is_list_of: Whether document title is "List of *"
 * is_disambig: Whether document is a disambiguation page.
 * is_list: Whether document is a list of any type ("List of *", disambig,
 *          or in Category or Book namespaces)
 */
abstract class GeoDocument[CoordType : Serializer](
  val schema: Seq[String],
  fieldvals: Seq[String]
) {
  // Fixed fields
  var corpus = ""
  var title = ""
  var id = ""
  var optcoord: Option[CoordType] = None
  def coord = optcoord.get
  var split = ""

  // Wikipedia-specific fields that we reference commonly (FIXME, make a
  // Wikipedia-specific subclass)
  var incoming_links: Option[Int] = None
  var redir = ""

  import GeoDocument._, GeoDocumentConverters._

  val params =
    (for ((name, v) <- (schema zip fieldvals)) yield {
      val handled =
        try { handle_parameter(name, v) }
        catch {
          case e@_ => {
            val msg =
              "Bad value %s for field '%s': %s" format (v, name, e.toString)
            throw DocumentValidationException(msg)
          }
        }
        if (handled) Nil else List(name -> v)
    }).flatten.toMap

  def handle_parameter(name: String, v: String) = {
    name match {
      case "corpus" => corpus = v; true
      case "title" => title = v; true
      case "id" => id = v; true
      case "split" => split = v; true
      case "coord" => optcoord = get_x_or_blank[CoordType](v); true
      // Wikipedia-specific:
      case "redir" => redir = v; true
      case "incoming_links" => incoming_links = get_int_or_blank(v); true
      case _ => false
    }
  }

  def get_fields(fields: Iterable[String]) = {
    for (field <- fields) yield {
      field match {
        case "corpus" => corpus
        case "title" => title
        case "id" => id
        case "split" => split
        case "coord" => put_x_or_blank[CoordType](optcoord)
          // Wikipedia-specific:
        case "redir" => redir
        case "incoming_links" => put_int_or_blank(incoming_links)
        case _ => params(field)
      }
    }
  }

  override def toString() = {
    val coordstr = if (optcoord != None) " at %s".format(coord) else ""
    val redirstr =
      if (redir.length > 0) ", redirect to %s".format(redir) else ""
    var corpusstr = if (corpus.length > 0) "%s/".format(corpus) else ""
    "%s%s(%s)%s%s, params=%s".format(corpusstr, title, id, coordstr,
      redirstr, params)
  }

  def adjusted_incoming_links = adjust_incoming_links(incoming_links)
}

object GeoDocument {
  val document_metadata_suffix = "document-metadata"
  val unigram_counts_suffix = "unigram-counts"
  val bigram_counts_suffix = "bigram-counts"
  val text_suffix = "text"

  /**
   * Encode a word for placement inside a "counts" field.  Colons and spaces
   * are used for separation.  There should be no spaces because words are
   * tokenized on spaces, but we have to escape colons.  We do this using
   * URL-style-encoding, replacing : by %3A; hence we also have to escape %
   * signs. (We could equally well use HTML-style encoding; then we'd have to
   * escape &amp; instead of :.) Note that regardless of whether we use
   * URL-style or HTML-style encoding, we probably want to do the encoding
   * ourselves rather than use a predefined encoder.  We could in fact use
   * the presupplied URL encoder, but it would encode all sorts of stuff,
   * which is unnecessary and would make the raw files harder to read.
   * In the case of HTML-style encoding, : isn't even escaped, so that
   * wouldn't work at all.
   */
  def encode_word_for_counts_field(word: String) = {
    assert(!(word contains ' '))
    word.replace("%", "%25").replace(":", "%3A")
  }

  /**
   * Decode a word encoded using `encode_word_for_counts_field`.
   */
  def decode_word_for_counts_field(word: String) = {
    word.replace("%3A", ":").replace("%3a", ":").replace("%25", "%")
  }

  // val fixed_fields = Seq("corpus", "title", "id", "split", "coord")
  // val wikipedia_fields = Seq("incoming_links", "redir")

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

/************************ Conversion functions ************************/
object GeoDocumentConverters {
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
  
  def get_int_or_blank(foo: String) =
    if (foo == "") None else Option[Int](foo.toInt)
  def put_int_or_blank(foo: Option[Int]) = {
    foo match {
      case None => ""
      case Some(x) => x.toString
    }
  }

  /**
   * Convert a blank string into None, or a valid string that converts into
   * type T into Some(value), where value is of type T.  Throw an error if
   * a non-blank, invalid string was seen.  Note that the construction
   * `T : Serializer` means essentially "T must have a Serializer".
   * More technically, it adds an extra implicit parameter list with a
   * single parameter of type Serializer[T].  When the compiler sees a
   * call to get_x_or_blank[X] for some type X, it looks in the lexical
   * environment to see if there is an object in scope of type Serializer[X]
   * that is marked `implicit`, and if so, it gives the implicit parameter
   * the value of that object; otherwise, you get a compile error.  The
   * function can then retrieve the implicit parameter's value using the
   * construction `implicitly[Serializer[T]]`.  The `T : Serializer`
   * construction is technically known as a *context bound*.
   */
  def get_x_or_blank[T : Serializer](foo: String) =
    if (foo == "") None
    else Option[T](implicitly[Serializer[T]].deserialize(foo))
  def put_x_or_blank[T : Serializer](foo: Option[T]) = {
    foo match {
      case None => ""
      case Some(x) => implicitly[Serializer[T]].serialize(x)
    }
  }
}
