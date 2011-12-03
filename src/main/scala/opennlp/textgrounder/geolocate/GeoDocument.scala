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

import WordDist.memoizer._
import GeolocateDriver.Debug._

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                               //
/////////////////////////////////////////////////////////////////////////////

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
 * A file processor that reads document files from a corpora.
 *
 * @param schema list of fields in the document files, as determined from
 *   a schema file
 * @param dstats ExperimentDriverStats used for 
 */
abstract class GeoDocumentFileProcessor(
  suffix: String,
  val dstats: ExperimentDriverStats
) extends CorpusFileProcessor(suffix) {

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
    val was_accepted =
      try { handle_document(fieldvals) }
      catch {
        case e:DocumentValidationException => {
          warning("Line %s: %s", num_processed + 1, e.message)
          if (debug("stack-trace") || debug("stacktrace"))
            e.printStackTrace
          return false
        }
      }
    if (was_accepted)
      increment_document_counter("documents.accepted")
    else {
      errprint("Skipped document %s",
        get_field_value_or_else(schema, fieldvals, "title", "unknown title??"))
      increment_document_counter("documents.skipped")
    }
    return true
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
 * coord: Coordinates of document.
 * split: Split of document ("training", "dev", "test")
 *
 * FIXME: It's unclear it makes sense to split GeoDocument from DistDocument.
 */
abstract class GeoDocument[CoordType : Serializer](
  val schema: Seq[String]
) {
  var corpusind = blank_memoized_string
  def corpus = unmemoize_string(corpusind)
  def title: String
  def has_coord: Boolean
  def coord: CoordType
  var splitind = blank_memoized_string
  def split = unmemoize_string(splitind)
  def incoming_links: Option[Int] = None

  import GeoDocument._, GeoDocumentConverters._

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
    for ((field, value) <- (schema zip fieldvals)) {
      if (debug("rethrow"))
        set_field(field, value)
      else {
        try { set_field(field, value) }
        catch {
          case e@_ => {
            val msg = ("Bad value %s for field '%s': %s" format
                       (value, field, e.toString))
            throw new DocumentValidationException(msg, e)
          }
        }
      }
    }
  }

  def get_fields(fields: Iterable[String]) = {
    for (field <- fields;
         value = get_field(field);
         if value != null)
      yield value
  }

  def set_field(field: String, value: String) {
    field match {
      case "corpus" => corpusind = memoize_string(value)
      case "split" => splitind = memoize_string(value)
      case _ => () // Just eat the extra parameters
    }
  }

  def get_field(field: String) = {
    field match {
      case "corpus" => unmemoize_string(corpusind)
      case "split" => unmemoize_string(splitind)
      case "title" => title
      case "coord" => if (has_coord) put_x(coord) else null
      case _ => null
    }
  }

  // def __repr__ = "DistDocument(%s)" format toString.encode("utf-8")

  def shortstr = "%s" format title

  override def toString = {
    val coordstr = if (has_coord) " at %s".format(coord) else ""
    var corpusstr = if (corpus.length > 0) "%s/".format(corpus) else ""
    "%s%s%s".format(corpusstr, title, coordstr)
  }

  def struct: scala.xml.Elem

  def distance_to_coord(coord2: CoordType): Double
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
