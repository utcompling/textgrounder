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

package opennlp.textgrounder
package preprocess

import scala.util.matching.Regex
import scala.util.control.Breaks._
import collection.mutable

import java.io.{InputStream, PrintStream}

import gridlocate.GridDoc

import util.argparser._
import util.collection.DynamicArray
import util.textdb._
import util.experiment._
import util.io._
import util.print.{errprint, warning}

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////

class MMCParameters(val parser: ArgParser) extends
    ArgParserParameters {
  val output_dir =
    parser.option[String]("o", "output-dir",
      metavar = "DIR",
      help = """Directory to store output files in.""")
  val input_dir =
    parser.option[String]("i", "input-dir",
      metavar = "FILE",
      help = """Directory containing the input corpus, in old format.""")
  val counts_file =
    parser.option[String]("counts-file",
      metavar = "FILE",
      help = """File containing the word counts, in old format.""")
  var output_file_prefix =
    parser.option[String]("output-file-prefix",
      metavar = "FILE",
      help = """Prefix to add to files in the output corpus dir.""")
}

/**
 * A simple reader for word-count files in the old multi-line count format.
 * This is stripped of debugging code, code to handle case-merging, stopwords
 * and other modifications of the language models, etc.  All it does is
 * read the language models and pass them to `handle_document`.
 */

trait SimpleUnigramLangModelBuilder {
  val initial_dynarr_size = 1000
  val keys_dynarr =
    new DynamicArray[String](initial_alloc = initial_dynarr_size)
  val values_dynarr =
    new DynamicArray[Int](initial_alloc = initial_dynarr_size)

  def handle_document(title: String, keys: Array[String], values: Array[Int],
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
              keys_dynarr += word
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
 * A simple factory for reading in the unigram language models in the old
 * format.
 */
class MMCUnigramLangModelHandler(
  schema: Schema,
  document_fieldvals: Map[String, Seq[String]],
  filehand: FileHandler,
  output_dir: String,
  output_file_prefix: String
) extends SimpleUnigramLangModelBuilder {
  val new_schema = new Schema(schema.fieldnames ++ Seq("unigram-counts"),
    schema.fixed_values)
  val writer = new TextDBWriter(new_schema)
  writer.output_schema_file(filehand, output_dir + "/" + output_file_prefix)
  val outstream = writer.open_data_file(filehand,
    output_dir + "/" + output_file_prefix, compression = "bzip2")
 
  def handle_document(title: String, keys: Array[String], values: Array[Int],
      num_words: Int) = {
    errprint("Handling document: %s", title)
    val params = document_fieldvals.getOrElse(title, null)
    if (params == null)
      warning("Strange, can't find document %s in document file", title)
    else {
      val counts =
        (for (i <- 0 until num_words) yield {
          // errprint("Saw2 %s,%s", keys(i), values(i))
          ("%s:%s" format (encode_string_for_map_field(keys(i)),
            values(i)))
        }). mkString(" ")
      val new_params = params ++ Seq(counts)
      outstream.println(writer.schema.make_line(new_params))
    }
    true
  }

  def finish() {
    outstream.close()
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

  def run() {
    if (!filehand.make_directories(params.output_dir))
      param_error("Output dir %s must not already exist" format
        params.output_dir)

    val (schema, field_iter) =
      TextDB.read_textdb_data(filehand, params.input_dir)

    if (params.output_file_prefix == null) {
      var (_, prefix, _, _) =
        Schema.split_schema_file(filehand, schema.filename).get
      errprint("Setting new output-file prefix to '%s'", prefix)
      params.output_file_prefix = prefix
    }

    val document_fieldvals =
      (for (fieldvals <- field_iter.flatten) yield {
        val params = (schema.fieldnames zip fieldvals).toMap
        (params("title"), fieldvals)
      }).toMap

    val counts_handler =
      new MMCUnigramLangModelHandler(schema,
        document_fieldvals, filehand, params.output_dir,
        params.output_file_prefix)
    counts_handler.read_word_counts(filehand, params.counts_file)
    counts_handler.finish()
  }
}

object MergeMetadataAndOldCounts extends
    ExperimentDriverApp("MergeMetadataAndOldCounts") {
  type TDriver = MMCDriver
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver = new TDriver
}
