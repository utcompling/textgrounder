package opennlp.textgrounder
package preprocess

import com.nicta.scoobi.Scoobi._

import util.argparser._
import util.collection._
import util.hadoop.HadoopFileHandler
import util.io.FileHandler
import util.print._
import util.textdb._

class ConvertCophirParams(ap: ArgParser) extends ScoobiProcessFilesParams(ap) {
  var corpus_name = ap.option[String]("corpus-name",
    help="""Name of output corpus; for identification purposes.
    Default to name taken from input directory.""")
  var split = ap.option[String]("split", default = "training",
    help="""Split (training, dev, test) to place data in.  Default %default.""")
  var preserve_case = ap.flag("preserve-case",
    help="""Don't lowercase words.  This preserves the difference
    between e.g. the name "Mark" and the word "mark".""")
  var max_ngram = ap.option[Int]("max-ngram", "max-n-gram", "ngram", "n-gram",
    default = 1,
    help="""Largest size of n-grams to create.  Default 1, i.e. distribution
    only contains unigrams.""")
}

/**
 * A generic action in the ConvertCophir app.
 */
trait ConvertCophirAction extends ScoobiProcessFilesAction {
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
        map(encode_ngram_for_count_map_field)
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

  /**
   * Given the filename of the XML file and the raw contents of the file,
   * parse as XML and optionally return a list of key-value pairs.  If file
   * can't be parsed, return None.
   */
  def apply(filename: String, rawxml: String):
      Option[Iterable[(String, String)]] = {
    val maybedom = try {
      Some(xml.XML.loadString(rawxml))
    } catch {
      case _ => {
        warning("Unable to parse XML filename: %s", filename)
        None
      }
    }

    maybedom map { dom =>
      val id = extract_tags(dom, List("MediaUri"))
      val photoprops = extract_props(dom \\ "photo",
        List("dateuploaded"), "photo-")
      val dateprops = extract_props(dom \\ "dates",
        List("posted", "taken", "lastupdate"), "dates-")
      val owner = extract_props(dom \\ "owner",
        List("nsid", "username", "realname", "location"), "owner-")
      val location = dom \\ "location"
      val lat = (location \ "@latitude").text
      val long = (location \ "@longitude").text
      val coord =
        if (lat.length > 0 && long.length > 0)
          "%s,%s" format (lat, long)
        else
          ""
      val coordprop = List(("coord", Encoder.string(coord)))
      val locprops1 = extract_props(location, List("accuracy"), "location-")
      val locprops2 = extract_tags(location,
        List("neighbourhood", "locality", "county", "region", "country"),
        "location-")
      val other = extract_tags(dom, List("url", "title", "description"))

      val rawtags = (dom \\ "tag") flatMap { tag =>
        (tag \ "@machine_tag").text match {
          case "1" => None
          case _ => Some((tag \ "@raw").text)
        }
      }
      val tagprops =
        List(("rawtags", Encoder.seq_string(rawtags)),
             ("tag-counts", emit_ngrams(rawtags)))
      val filenameprops = List(("orig-filename", Encoder.string(filename)))
      id ++ photoprops ++ dateprops ++ owner ++ coordprop ++
        locprops1 ++ locprops2 ++ other ++ filenameprops ++ tagprops
    }
  }

  /**
   * Get list of fields for a given textdb row, converted from an XML file.
   * We feed a fake XML file into the parser.  This depends on the fact
   * that all fields are always returned (and will be blank if the data
   * can't be found).
   */
  def row_fields = {
    val fake_xml_fields = apply("foo.xml", "<foo />")
    fake_xml_fields.get.map(_._1)
  }
}

class ConvertCophirDriver(opts: ConvertCophirParams)
    extends ConvertCophirAction {

  val operation_category = "Driver"

  def corpus_suffix = "cophir"

  /**
   * Compute name of corpus, derived either from explicitly specified
   * --corpus-name or from the non-directory component of the input file,
   * minus certain extensions and with * replaced by _.
   */
  def compute_corpus_name(filehand: FileHandler) = {
    if (opts.corpus_name != null) opts.corpus_name
    else {
      val (_, last_component) = filehand.split_filename(opts.input)
      val exts_to_remove = Seq(".tgz.seq", ".tar.gz.seq", ".seq")
      // Find the first matching suffix, if any, and strip it.  Don't
      // use fold() because we don't want to strip multiple suffixes in
      // general.
      val stripped_component =
        exts_to_remove find (last_component.endsWith(_)) match {
          case Some(str) => last_component.stripSuffix(str)
          case None => last_component
        }
      stripped_component.replace("*", "_")
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
        "corpus-type" -> "cophir") ++
      opts.non_default_params_string.toMap ++
      Map(
        "split" -> opts.split
      )
    new Schema(fields, fixed_fields)
  }
}

object ConvertCophir
    extends ScoobiProcessFilesApp[ConvertCophirParams]
       with ConvertCophirAction {
  def create_params(ap: ArgParser) = new ConvertCophirParams(ap)

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
    val props_lines =
      rawxmlfiles flatMap { case (fname, rawxml) => parse_xml(fname, rawxml) }
    // Convert property list into textdb line
    val outlines = props_lines map (prop => prop map (_._2) mkString "\t")
   
    /////// Output textdb corpus
    dlist_output_textdb(schema, outlines, filehand, opts.output,
      opts.corpus_name, ccd.corpus_suffix)
  }
}
