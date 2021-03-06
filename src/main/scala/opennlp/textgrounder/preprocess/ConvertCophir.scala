///////////////////////////////////////////////////////////////////////////////
//  ConvertCophir.scala
//
//  Copyright (C) 2012-2014 Ben Wing, The University of Texas at Austin
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
package preprocess

import com.nicta.scoobi.Scoobi._

import util.argparser._
import util.collection._
import util.hadoop.HadoopFileHandler
import util.io.FileHandler
import util.print._
import util.textdb._

/*
Comment Feb 21, 2013 by Ben:

To implement user frequency rather than term frequency, we need to be
able to exclude different instances of terms used by the same user in a given
grid cell. Because this depends on the size and layout of grid cells, it might
make the most sense to do this when creating the language model rather than
during pre-processing. We have to do something like this:

1. Rather than using a count map to count term instances in a given grid cell,
   we use a set to record (term, user) pairs.
2. Then at the end, we iterate through the set, creating a count map of the
   term instances seen.

What about for KD trees? Since the KD-tree cells are created based on the
distribution of documents, rather than terms, this won't affect things, other
than the fact that we can't do the creation of the language-model
distributions until we've computed the KD tree grid.

Conceivably we could run out of memory creatng the distributions, since we
have to record term-user pairs rather than just terms, especially if there
are a lot of users and each has relatively few documents. In such a case we'd
have to run a Scoobi process. The above steps could be implemented as follows:

1. For each term token in the corpus, generate a (term, user, cell) triple.
2. Group by the triple and combine, throwing out duplicates.
3. Group by (term, cell) and combine, counting instances seen.

This requires that we have the grid cell structure available, which requires
some work in the case of KD trees.

One potential way of reducing memory is to memoize both terms and users
down to 32-bit integers (might not be necessary for users, which may already
have a 32-bit ID) and then combine them into a 64-bit long rather than
using a tuple or similar object to record the pair, which adds a lot of
overhead.
*/

class ConvertCophirParams(ap: ArgParser) extends ScoobiProcessFilesParams(ap) {
  var corpus_name = ap.option[String]("corpus-name",
    help="""Name of output corpus; for identification purposes.
    Default to name taken from input directory.""")
  var preserve_case = ap.flag("preserve-case",
    help="""Don't lowercase words.  This preserves the difference
    between e.g. the name "Mark" and the word "mark".""")
  var allow_missing_coord = ap.flag("allow-missing-coord",
    help="""Allow for records with missing coordinates; otherwise, they will be
skipped.""")
  var no_bulk_filter = ap.flag("no-bulk-filter",
    help="""Don't apply bulk filtering to remove multiple photos from the
same user with the same tags.""")
  var max_ngram = ap.option[Int]("max-ngram", "max-n-gram", "ngram", "n-gram",
    default = 1,
    must = be_>(0),
    help="""Largest size of n-grams to create.  Default 1, i.e. language model
    only contains unigrams.""")
}

/**
 * Data for a particular CoPHiR image.
 *
 * @param rawtags Concatenated raw tags, for bulk filtering
 * @param owner_id ID of photo owner, for training/dev/test splitting
 * @param photo_id ID of photo
 * @param props List of pairs of properties (including the raw tags and ID's)
 */
case class CophirImage(
  rawtags: String,
  owner_id: Long,
  photo_id: Long,
  props: Iterable[(String, String)]
)

trait CophirImplicits {
  implicit val cophirImageWire =
    mkCaseWireFormat(CophirImage.apply _, CophirImage.unapply _)
}

/**
 * A generic action in the ConvertCophir app.
 */
trait ConvertCophirAction extends
    ScoobiProcessFilesAction with CophirImplicits {
  val progname = "ConvertCophir"
}

/**
 * Given the filename of the XML file and the raw contents of the file,
 * parse as XML and optionally return a list of key-value pairs.  If file
 * can't be parsed, return None.
 */
class ParseXml(opts: ConvertCophirParams) extends ConvertCophirAction {

  val operation_category = "ParseXml"

  /**
   * Convert a word to lowercase.
   */
  def normalize_word(orig_word: String) = {
    if (opts.preserve_case)
      orig_word
    else
      orig_word.toLowerCase
  }

  /**
   * Break up a piece of text into tokens and separate into ngrams.
   */
  def break_text_into_ngrams(text: String):
      Iterable[Iterable[String]] = {
    val words = text.split("\\s+").toIterable
    val normwords = words.map(normalize_word)

    // Then, generate all possible ngrams up to a specified maximum length,
    // where each ngram is a sequence of words.  `sliding` overlays a sliding
    // window of a given size on a sequence to generate successive
    // subsequences -- exactly what we want to generate ngrams.  So we
    // generate all 1-grams, then all 2-grams, etc. up to the maximum size,
    // and then concatenate the separate lists together (that's what `flatMap`
    // does).
    (1 to opts.max_ngram).flatMap(normwords.sliding(_))
  }

  /**
   * Tokenize a sequence of words into ngrams and count them, emitting
   * the word-count pairs encoded into a string.
   */
  def emit_ngrams(text: Iterable[String]): String = {
    val ngrams =
      text.flatMap(break_text_into_ngrams(_)).toSeq.
        map(encode_ngram_for_map_field)
    shallow_encode_count_map(list_to_item_count_map(ngrams).toSeq)
  }

  /**
   * Extract text from the given tags, at any level of depth.
   */
  def extract_tags(node: xml.NodeSeq, tags: Iterable[String],
      prefix: String = "") = {
    tags map { tag =>
      val value = (node \\ tag).text
      ((prefix + tag, Encoder.string(value)))
    }
  }

  /**
   * Extract text from the given properties of the given tag(s) (e.g. in
   * &lt;foo bar="baz"&gt;, "foo" is a tag and "bar" is a property of the tag,
   * whose text is "baz".  Does not recurse down the tree.  Internally, adds an
   * @ to the property names to indicate to the Scala XML routines that they are
   * properties, not tags.
   */
  def extract_props(node: xml.NodeSeq, props: Iterable[String],
      prefix: String = "") = {
    props map { prop =>
      val value = (node \ ("@" + prop)).text
      ((prefix + prop, Encoder.string(value)))
    }
  }

  def safe_toLong(str: String) = {
    try {
      Some(str.toLong)
    } catch {
      case e: NumberFormatException =>
        problem("Unable to parse number", "'%s': %s" format (str, e))
    }
  }

  /**
   * Convert the contents of an XML file from the CoPHiR corpus into an
   * object describing the image.  If file can't be parsed or other error,
   * return None.
   *
   * The CophirImage object returned contains a key-value list of properties,
   * which will be converted into a textdb corpus.  It also contains fields
   * for some individual properties, for use in implementing the
   * corpus-generation algorithm in O'Hare and Murdock (2012).
   */
  def apply(filename: String, rawxml: String): Option[CophirImage] = {
    val maybedom = try {
      Some(xml.XML.loadString(rawxml))
    } catch {
      case _: Exception =>
        problem("Unable to parse XML filename", filename)
    }

    maybedom flatMap { dom =>
      val photo_idstr = (dom \\ "MediaUri").text
      val owner_idstr = (dom \\ "owner" \ "@nsid").text
      if (photo_idstr == "")
        problem("Can't find photo ID in XML", "file %s" format filename)
      else if (owner_idstr == "")
        problem("Can't find owner ID in XML", "file %s" format filename)
      else {
        val maybe_photo_id = safe_toLong(photo_idstr)
        val maybe_owner_id =
          safe_toLong(owner_idstr.replaceAll("@N[0-9]*", ""))
        val location = dom \\ "location"
        val lat = (location \ "@latitude").text
        val long = (location \ "@longitude").text
        val coord =
          if (lat.length > 0 && long.length > 0)
            "%s,%s" format (lat, long)
          else
            ""
        if (maybe_photo_id == None || maybe_owner_id == None ||
            (coord.length == 0 && !opts.allow_missing_coord)) None
        else {
          val photo_id = maybe_photo_id.get
          val owner_id = maybe_owner_id.get
          val idprops =
            List(("photo-id", Encoder.long(photo_id)),
                 ("owner-id", Encoder.long(owner_id)))
          val photoprops = extract_props(dom \\ "photo",
            List("dateuploaded"), "photo-")
          val dateprops = extract_props(dom \\ "dates",
            List("posted", "taken", "lastupdate"), "dates-")
          val ownerprops = extract_props(dom \\ "owner",
            List("username", "realname", "location"), "owner-")
          val coordprop = List(("coord", Encoder.string(coord)))
          val locprops1 = extract_props(location, List("accuracy"), "location-")
          val locprops2 = extract_tags(location,
            List("neighbourhood", "locality", "county", "region", "country"),
            "location-")
          val otherprops = extract_tags(dom, List("url", "title", "description"))

          val rawtags = (dom \\ "tag") flatMap { tag =>
            // filter machine-added tags (geotags, etc.)
            (tag \ "@machine_tag").text match {
              case "1" => None
              case _ => Some((tag \ "@raw").text)
            }
          } map { _.trim } filter { _.length > 0 } // Filter out blank tags
          val rawtags_str = Encoder.string_seq(rawtags)
          // Filter photos without user-added tags (needs to be done after
          // filtering machine-added tags)
          if (rawtags_str.size == 0) None
          else {
            val tagprops =
              List(("rawtags", rawtags_str),
                   ("unigram-counts", emit_ngrams(rawtags)))
            val filenameprops = List(("orig-filename", Encoder.string(filename)))
            Some(CophirImage(rawtags_str, owner_id, photo_id,
              idprops ++ photoprops ++ dateprops ++ ownerprops ++ coordprop ++
              locprops1 ++ locprops2 ++ otherprops ++ filenameprops ++ tagprops))
          }
        }
      }
    }
  }

  /**
   * Get list of fields for a given textdb row, converted from an XML file.
   * We feed a fake XML file into the parser.  This depends on the fact
   * that all fields are always returned (and will be blank if the data
   * can't be found).
   */
  def row_fields = {
    apply("foo.xml",
      """<root><MediaUri>0</MediaUri><location latitude="50" longitude="50"/><owner nsid="0@N00"/><tags><tag id="666" raw="cat" machine_tag="0">cat</tag></tags></root>""").get match {
      case CophirImage(_, _, _, props) => props.map(_._1)
    }
  }
}

class ConvertCophirDriver(opts: ConvertCophirParams)
    extends ConvertCophirAction {

  val operation_category = "Driver"

  /**
   * Compute name of corpus, derived either from explicitly specified
   * --corpus-name or from the non-directory component of the input file,
   * minus certain extensions and with * replaced by _.
   */
  def compute_corpus_name(filehand: FileHandler) = {
    if (opts.corpus_name != null) opts.corpus_name
    else {
      val (_, tail) = filehand.split_filename(opts.input)
      val exts_to_remove = Seq(".tgz.seq", ".tar.gz.seq", ".seq")
      // Find the first matching suffix, if any, and strip it.  Don't
      // use fold() because we don't want to strip multiple suffixes in
      // general.
      val stripped_tail =
        exts_to_remove find (tail.endsWith(_)) match {
          case Some(str) => tail.stripSuffix(str)
          case None => tail
        }
      stripped_tail.replace("*", "_")
    }
  }

  /**
   * Create a schema for the data to be output.
   */
  def create_schema = {
    val fields = new ParseXml(opts).row_fields
    val fixed_fields = Map(
        "corpus-name" -> opts.corpus_name,
        "generating-app" -> progname,
        "textdb-type" -> "textgrounder-corpus",
        "corpus-type" -> "cophir") ++
      opts.non_default_params_string.toMap
    new Schema(fields, fixed_fields)
  }
}

/**
 * Convert the CoPHiR corpus into a textdb corpus;
 * split into training/dev/test and apply bulk-upload filtering and such,
 * according to the algorithm described in O'Hare and Murdock (2012).
 */
object ConvertCophir
    extends ScoobiProcessFilesApp[ConvertCophirParams]
       with ConvertCophirAction {
  def create_params(ap: ArgParser) = new ConvertCophirParams(ap)

  def bulk_upload_combine(x: CophirImage, y: CophirImage) =
    if (x.photo_id < y.photo_id) x else y

  def run() {
    ////// Initialize
    val opts = init_scoobi_app()
    val filehand = new HadoopFileHandler(configuration)
    val ccd = new ConvertCophirDriver(opts)
    opts.corpus_name = ccd.compute_corpus_name(filehand)
    val schema = ccd.create_schema

    ////// Read in sequence files, convert entries to textdb lines
    val parse_xml = new ParseXml(opts)
    // Get list of pairs of (filename, rawxml)
    val rawxmlfiles: DList[(String, String)] =
      // FIXME: Scoobi should allow Array[Byte] directly.
      convertFromSequenceFile[String, Seq[Byte]](args(0)) map {
        case (fname, bytes) => (fname, new String(bytes.toArray, "UTF-8"))
      }
    // Parse XML into a property list, with property values
    // encoded for textdb fields
    val id_props_lines =
      rawxmlfiles flatMap { case (fname, rawxml) => parse_xml(fname, rawxml) }
    // Convert property list into textdb line
    val outlines =
      if (opts.no_bulk_filter) {
        id_props_lines.map { im =>
          (im.owner_id % 100, schema.make_line(im.props map (_._2)))
        }
      } else {
        id_props_lines.
          groupBy { im => (im.rawtags, im.owner_id) }.
          combine(bulk_upload_combine).
          map { case (_, im) =>
            (im.owner_id % 100, schema.make_line(im.props map (_._2)))
          }
      }
      
    val training =
      outlines.filter { case (modid, row) => modid < 80 }.
               map    { case (modid, row) => row }
    val dev =
      outlines.filter { case (modid, row) => modid >= 80 && modid < 90 }.
               map    { case (modid, row) => row }
    val dev_srcdir = opts.output + "-dev"
    val test =
      outlines.filter { case (modid, row) => modid >= 90 }.
               map    { case (modid, row) => row }
    val test_srcdir = opts.output + "-test"

    // output data files
    persist((
      TextOutput.toTextFile(training, opts.output),
      TextOutput.toTextFile(dev, dev_srcdir),
      TextOutput.toTextFile(test, test_srcdir)))

    // move/rename data files
    rename_output_files(filehand, opts.output,
      opts.corpus_name, "-training")
    move_output_files(filehand, dev_srcdir, opts.output,
      opts.corpus_name, "-dev")
    move_output_files(filehand, test_srcdir, opts.output,
      opts.corpus_name, "-test")

    // output schema files
    val base = "%s/%s" format (opts.output, opts.corpus_name)
    for (split <- List("training", "dev", "test")) {
    schema.clone_with_changes(Map("split"->split)).
      output_constructed_schema_file(filehand, base + "-" + split)
    }
  }
}
