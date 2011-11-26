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

package opennlp.textgrounder.preprocess

import util.matching.Regex
import util.control.Breaks._
import collection.mutable

import java.io.{InputStream, PrintStream}

import opennlp.textgrounder.geolocate.IdentityMemoizer._
import opennlp.textgrounder.geolocate.GeoDocument

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.collectionutil.DynamicArray
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.ioutil._
import opennlp.textgrounder.util.MeteredTask
import opennlp.textgrounder.util.printutil.{errprint, warning}

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////

class MMCParameters(ap: ArgParser) extends
    ArgParserParameters(ap) {
  val output_dir =
    ap.option[String]("o", "output-dir",
      metavar = "DIR",
      help = """Directory to store output files in.""")
  val input_dir =
    ap.option[String]("i", "input-dir",
      metavar = "FILE",
      help = """Directory containing the input corpus, in old format.""")
  val counts_file =
    ap.option[String]("counts-file",
      metavar = "FILE",
      help = """File containing the word counts, in old format.""")
  var output_file_prefix =
    ap.option[String]("output-file-prefix",
      metavar = "FILE",
      help = """Prefix to add to files in the output corpus dir.""")
}

/**
 * A simple reader for word-count files in the old multi-line count format.
 * This is stripped of debugging code, code to handle case-merging, stopwords
 * and other modifications of the distributions, etc.  All it does is
 * read the distributions and pass them to `handle_document`.
 */

trait SimpleUnigramWordDistReader {
  val initial_dynarr_size = 1000
  val keys_dynarr =
    new DynamicArray[Word](initial_alloc = initial_dynarr_size)
  val values_dynarr =
    new DynamicArray[Int](initial_alloc = initial_dynarr_size)

  def handle_document(title: String, keys: Array[Word], values: Array[Int],
      num_words: Int): Boolean

  def read_word_counts(filehand: FileHandler, filename: String) {
    errprint("Reading word counts from %s...", filename)
    errprint("")

    var title: String = null

    // Written this way because there's another line after the for loop,
    // corresponding to the else clause of the Python for loop
    breakable {
      for (line <- filehand.openr(filename)) {
        if (line.startsWith("Article title: ")) {
          if (title != null) {
            if (!handle_document(title, keys_dynarr.array, values_dynarr.array,
                keys_dynarr.length))
              break
          }
          // Extract title and set it
          val titlere = "Article title: (.*)$".r
          line match {
            case titlere(ti) => title = ti
            case _ => assert(false)
          }
          keys_dynarr.clear()
          values_dynarr.clear()
        } else if (line.startsWith("Article coordinates) ") ||
          line.startsWith("Article ID: ")) {
        } else {
          val linere = "(.*) = ([0-9]+)$".r
          line match {
            case linere(word, count) => {
              // errprint("Saw1 %s,%s", word, count)
              keys_dynarr += memoize_word(word)
              values_dynarr += count.toInt
            }
            case _ =>
              warning("Strange line, can't parse: title=%s: line=%s",
                title, line)
          }
        }
      }
     if (!handle_document(title, keys_dynarr.array, values_dynarr.array,
         keys_dynarr.length))
       break
    }
  }
}

/**
 * A simple factory for reading in the unigram distributions in the old
 * format.
 */
class MMCUnigramWordDistHandler(
  schema: Seq[String],
  document_params: mutable.Map[String, Map[String, String]],
  filehand: FileHandler,
  output_dir: String,
  output_file_prefix: String
) extends SimpleUnigramWordDistReader {
  val writer = new FieldTextWriter(schema ++ Seq("counts"))
  val full_prefix = "%s/%s" format (output_dir, output_file_prefix)
  val outstream = filehand.openw("%s-counts.txt" format full_prefix,
    compression = "bzip2")
 
  def output_schema_file() {
    val schema_outstream = filehand.openw("%s-schema.txt" format full_prefix)
    writer.output_schema(schema_outstream)
    schema_outstream.close()
  }

  def minimal_url_encode(word: String) =
    word.replace("%", "%25").replace(":", "%3A")

  def handle_document(title: String, keys: Array[Word], values: Array[Int],
      num_words: Int) = {
    errprint("Handling document: %s", title)
    val params = document_params.getOrElse(title, null)
    if (params == null)
      warning("Strange, can't find document %s in document file", title)
    else {
      val counts =
        (for (i <- 0 until num_words) yield {
          // errprint("Saw2 %s,%s", keys(i), values(i))
          ("%s:%s" format
            (minimal_url_encode(unmemoize_word(keys(i))), values(i)))}).
          mkString(" ")
      val new_params = params + ("counts" -> counts)
      writer.output_row(outstream, new_params)
    }
    true
  }
}

/**
 * A simple field-text file processor that just records the documents read in,
 * by title.
 *
 * @param schema fields of the document-data files, as determined from
 *   a schema file
 * @param filter Regular expression used to select document-data files in
 *   a directory
 */
class MMCDocumentFileProcessor(
  schema: Seq[String], filter: Regex
) extends FieldTextFileProcessor(schema) {
  val document_params = mutable.Map[String, Map[String, String]]()

  def process_row(fieldvals: Map[String, String]): Boolean = {
    document_params(fieldvals("title")) = fieldvals
    true
  }

  def process_lines(lines: Iterator[String],
      filehand: FileHandler, file: String,
      compression: String, realname: String) = {
    val task = new MeteredTask("document", "reading")
    for (line <- lines)
      parse_row(line)
    true
  }

  override def filter_dir_files(filehand: FileHandler, dir: String,
      files: Iterable[String]) = {
    for (file <- files if filter.findFirstMatchIn(file) != None) yield file
  }
}

class MMCDriver extends ArgParserExperimentDriver {
  type ParamType = MMCParameters
  type RunReturnType = Unit
  
  val filehand = new LocalFileHandler
  
  def usage() {
    sys.error("""Usage: MMC [-o OUTDIR | --outfile OUTDIR] [--output-stats] INFILE ...

Convert input files in the Infochimps Twitter corpus into files in the
format expected by TextGrounder.  If --output-stats is given,
output statistics to stderr rather than converting text.  Otherwise,
store results in OUTDIR, which must not exist already.  
""")
  }

  def handle_parameters() {
    need(params.output_dir, "output-dir")
    need(params.input_dir, "input-dir")
    need(params.counts_file, "counts-file")
    if (params.output_file_prefix == null) {
      val schema_file = GeoDocument.find_schema_file(filehand,
        params.input_dir)
      var (_, base) = filehand.split_filename(schema_file)
      params.output_file_prefix = base.replaceAll("-[^-]*$", "")
      errprint("Settings new output-file prefix to '%s'",
        params.output_file_prefix)
    }
  }

  def setup_for_run() { }

  def run_after_setup() {
    if (!filehand.make_directories(params.output_dir))
      param_error("Output dir %s must not already exist" format
        params.output_dir)

    val schema =
      GeoDocument.read_schema_from_corpus(filehand, params.input_dir)

    val fileproc =
      new MMCDocumentFileProcessor(schema, GeoDocument.document_metadata_regex)
    fileproc.process_files(filehand, Seq(params.input_dir))

    val counts_handler =
      new MMCUnigramWordDistHandler(schema, fileproc.document_params,
        filehand, params.output_dir, params.output_file_prefix)
    counts_handler.output_schema_file()
    counts_handler.read_word_counts(filehand, params.counts_file)
  }
}

object MergeMetadataAndOldCounts extends
    ExperimentDriverApp("Convert Twitter Infochimps") {
  type DriverType = MMCDriver
  def create_param_object(ap: ArgParser) = new ParamType(ap)
  def create_driver() = new DriverType
}
