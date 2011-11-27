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

import opennlp.textgrounder.geolocate.GeoDocument

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.ioutil._
import opennlp.textgrounder.util.MeteredTask
import opennlp.textgrounder.util.printutil.{errprint}

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////

class FrobCorpusParameters(ap: ArgParser) extends
    ProcessFilesParameters(ap) {
  val add_field =
    ap.multiOption[String]("a", "add-field",
      help = """Field to add, of the form FIELD=VAL, e.g.
'corpus=twitter-geotext-output-5-docthresh'.  Fields are added at the
beginning.""")
  val remove_field =
    ap.multiOption[String]("r", "remove-field",
      metavar = "FIELD",
      help = """Field to remove.""")
  val input_dir =
    ap.option[String]("i", "input-dir",
      metavar = "DIR",
      help = """Directory containing input corpus.""")
  val suffix =
    ap.option[String]("s", "suffix",
      default = "unigram-counts",
      metavar = "DIR",
      help = """Suffix used to select the appropriate files to operate on.
Default '%default'.""")
}

/**
 * A field-text file processor that outputs fields as the came in,
 * possibly modified in various ways.
 *
 * @param schema fields of the document metadata files, as determined from
 *   a schema file
 * @param params Parameters retrieved from the command-line arguments
 */
class FrobCorpusDocumentFileProcessor(
  schema: Seq[String], filehand: FileHandler, schema_file: String,
  params: FrobCorpusParameters
) extends FieldTextFileProcessor(schema) {
   var writer: FieldTextWriter = _
   var cur_outstream: PrintStream = _

  def output_schema_file() {
    val fake_fieldvals = Seq.fill(schema.length)("foo")
    val (new_schema, _) = frob_row(fake_fieldvals).unzip
    val (_, schema_base) = filehand.split_filename(schema_file)
    val new_schema_file = filehand.join_filename(params.output_dir, schema_base)
    val schema_outstream = filehand.openw(new_schema_file)
    writer = new FieldTextWriter(new_schema)
    writer.output_schema(schema_outstream)
    schema_outstream.close()
  }

  def frob_row(fieldvals: Seq[String]) = {
    val zipped_vals = (schema zip fieldvals)
    val new_fields =
      for (addfield <- params.add_field) yield {
        val Array(field, value) = addfield.split("=", 2)
        (field -> value)
      }
    val docparams = mutable.LinkedHashMap[String, String]()
    docparams ++= new_fields
    docparams ++= zipped_vals
    for (field <- params.remove_field)
      docparams -= field
    docparams.toSeq
  }

  def process_row(fieldvals: Seq[String]): Boolean = {
    val (_, new_fieldvals) = frob_row(fieldvals).unzip
    writer.output_row(cur_outstream, new_fieldvals)
    true
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
    true
  }

  override def filter_dir_files(filehand: FileHandler, dir: String,
      files: Iterable[String]) = {
    val filter = GeoDocument.make_document_file_suffix_regex(params.suffix)
    for (file <- files if filter.findFirstMatchIn(file) != None) yield file
  }

  override def begin_process_lines(lines: Iterator[String],
      filehand: FileHandler, file: String,
      compression: String, realname: String) {
    val (_, base) = filehand.split_filename(realname)
    val new_file = filehand.join_filename(params.output_dir, base)
    cur_outstream = filehand.openw(new_file, compression = compression)
    super.begin_process_lines(lines, filehand, file, compression, realname)
  }

  override def end_process_file(filehand: FileHandler, file: String) {
    cur_outstream.close()
    cur_outstream = null
  }
}

class FrobCorpusDriver extends ProcessFilesDriver {
  type ParamType = FrobCorpusParameters
  
  def usage() {
    sys.error("""Usage: MergeMetadataAndOldCounts [-o OUTDIR | --outfile OUTDIR] [--output-stats] INFILE ...

Merge document-metadata files and old-style counts files into a new-style
counts file also containing the metadata.
""")
  }

  override def handle_parameters() {
    need(params.input_dir, "input-dir")
    super.handle_parameters()
  }

  override def run_after_setup() {
    super.run_after_setup()

    val schema_file =
      GeoDocument.find_schema_file(filehand, params.input_dir, params.suffix)
    val schema =
      FieldTextFileProcessor.read_schema_file(filehand, schema_file)
    val fileproc =
      new FrobCorpusDocumentFileProcessor(schema, filehand, schema_file,
        params)
    fileproc.output_schema_file()
    fileproc.process_files(filehand, Seq(params.input_dir))
  }
}

object FrobCorpus extends
    ExperimentDriverApp("Frob a corpus, adding or removing fields") {
  type DriverType = FrobCorpusDriver
  def create_param_object(ap: ArgParser) = new ParamType(ap)
  def create_driver() = new DriverType
}
