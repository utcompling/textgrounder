///////////////////////////////////////////////////////////////////////////////
//  MergeMetadataAndOldCounts.scala
//
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

import opennlp.textgrounder.worddist.IdentityMemoizer._
import opennlp.textgrounder.gridlocate.DistDocument

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.collectionutil.DynamicArray
import opennlp.textgrounder.util.corpusutil._
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

trait SimpleUnigramWordDistConstructor {
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
              keys_dynarr += memoize_string(word)
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
  schema: Schema,
  document_fieldvals: mutable.Map[String, Seq[String]],
  filehand: FileHandler,
  output_dir: String,
  output_file_prefix: String
) extends SimpleUnigramWordDistConstructor {
  val new_schema = new Schema(schema.fieldnames ++ Seq("counts"),
    schema.fixed_values)
  val writer = new CorpusWriter(new_schema, "unigram-counts")
  writer.output_schema_file(filehand, output_dir, output_file_prefix)
  val outstream = writer.open_document_file(filehand, output_dir,
    output_file_prefix, compression = "bzip2")
 
  def handle_document(title: String, keys: Array[Word], values: Array[Int],
      num_words: Int) = {
    errprint("Handling document: %s", title)
    val params = document_fieldvals.getOrElse(title, null)
    if (params == null)
      warning("Strange, can't find document %s in document file", title)
    else {
      val counts =
        (for (i <- 0 until num_words) yield {
          // errprint("Saw2 %s,%s", keys(i), values(i))
          ("%s:%s" format
            (encode_string_for_count_map_field(unmemoize_string(keys(i))),
              values(i)))
          }).
          mkString(" ")
      val new_params = params ++ Seq(counts)
      writer.schema.output_row(outstream, new_params)
    }
    true
  }

  def finish() {
    outstream.close()
  }
}

/**
 * A simple field-text file processor that just records the documents read in,
 * by title.
 *
 * @param suffix Suffix used to select document metadata files in a directory
 */
class MMCDocumentFileProcessor(
  suffix: String
) extends BasicCorpusFieldFileProcessor[Unit](suffix) {
  val document_fieldvals = mutable.Map[String, Seq[String]]()

  def process_row(fieldvals: Seq[String]) = {
    val params = (schema.fieldnames zip fieldvals).toMap
    document_fieldvals(params("title")) = fieldvals
    (true, true)
  }

  def process_lines(lines: Iterator[String],
      filehand: FileHandler, file: String,
      compression: String, realname: String) = {
    val task = new MeteredTask("document", "reading")
    for (line <- lines) {
      task.item_processed()
      parse_row(line)
    }
    task.finish()
    (true, ())
  }
}

class MMCDriver extends ArgParserExperimentDriver {
  type TParam = MMCParameters
  type TRunRes = Unit
  
  val filehand = new LocalFileHandler
  
  def usage() {
    sys.error("""Usage: MergeMetadataAndOldCounts [-o OUTDIR | --outfile OUTDIR] [--output-stats] INFILE ...

Merge document-metadata files and old-style counts files into a new-style
counts file also containing the metadata.
""")
  }

  def handle_parameters() {
    need(params.output_dir, "output-dir")
    need(params.input_dir, "input-dir")
    need(params.counts_file, "counts-file")
  }

  def setup_for_run() { }

  def run_after_setup() {
    if (!filehand.make_directories(params.output_dir))
      param_error("Output dir %s must not already exist" format
        params.output_dir)

    val fileproc =
      new MMCDocumentFileProcessor(document_metadata_suffix)
    fileproc.read_schema_from_corpus(filehand, params.input_dir)

    if (params.output_file_prefix == null) {
      var (_, base) = filehand.split_filename(fileproc.schema_file)
      params.output_file_prefix = base.replaceAll("-[^-]*$", "")
      params.output_file_prefix =
        params.output_file_prefix.stripSuffix("-document-metadata")
      errprint("Setting new output-file prefix to '%s'",
        params.output_file_prefix)
    }

    fileproc.process_files(filehand, Seq(params.input_dir))

    val counts_handler =
      new MMCUnigramWordDistHandler(fileproc.schema,
        fileproc.document_fieldvals, filehand, params.output_dir,
        params.output_file_prefix)
    counts_handler.read_word_counts(filehand, params.counts_file)
    counts_handler.finish()
  }
}

object MergeMetadataAndOldCounts extends
    ExperimentDriverApp("Merge document metadata files and old counts file") {
  type TDriver = MMCDriver
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver() = new TDriver
}
