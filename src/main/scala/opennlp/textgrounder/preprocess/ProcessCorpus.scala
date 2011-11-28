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

import java.io.PrintStream

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.ioutil._
import opennlp.textgrounder.util.MeteredTask

import opennlp.textgrounder.geolocate.GeoDocument

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////

class ProcessCorpusParameters(ap: ArgParser) extends
    ProcessFilesParameters(ap) {
  val input_dir =
    ap.option[String]("i", "input-dir",
      metavar = "DIR",
      help = """Directory containing input corpus.""")
}

/**
 * A file processor for reading in a corpus, processing it in some ways, and
 * writing a modified version.
 *
 * @param suffix file suffix used in reading schema and document files
 * @param new_suffix file suffix used when writing modified schema and
 *   document files
 * @param output_filehand file handler of output directory
 * @param output_dir output directory in which to store modified corpus
 */
abstract class ProcessCorpusFileProcessor(
  suffix: String, new_suffix: String,
  output_filehand: FileHandler, output_dir: String
) extends DocumentCorpusFileProcessor(suffix) {
   var writer: FieldTextWriter = _
   var cur_outstream: PrintStream = _

  def compute_new_schema() = {
    val fake_fieldvals = Seq.fill(schema.length)("foo")
    val (new_schema, _) = frob_row(fake_fieldvals).unzip
    new_schema
  }

  def construct_modified_output_file(orig_file: String, file_ending: String) = {
    val (_, base) = schema_file_filehand.split_filename(orig_file)
    val prefix = base.stripSuffix(suffix + file_ending)
    val new_base = prefix + new_suffix + file_ending
    val new_file = output_filehand.join_filename(output_dir, new_base)
    new_file
  }

  def output_schema_file() {
    val new_schema = compute_new_schema()
    val new_schema_file = construct_modified_output_file(schema_file,
      "-schema.txt")
    val schema_outstream = output_filehand.openw(new_schema_file)
    writer = new FieldTextWriter(new_schema)
    writer.output_schema(schema_outstream)
    schema_outstream.close()
  }

  def frob_row(fieldvals: Seq[String]): Seq[(String, String)]

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

  override def begin_process_lines(lines: Iterator[String],
      filehand: FileHandler, file: String,
      compression: String, realname: String) {
    val new_file = construct_modified_output_file(realname, ".txt")
    cur_outstream = output_filehand.openw(new_file, compression = compression)
    super.begin_process_lines(lines, filehand, file, compression, realname)
  }

  override def end_process_file(filehand: FileHandler, file: String) {
    cur_outstream.close()
    cur_outstream = null
  }
}

abstract class ProcessCorpusDriver extends ProcessFilesDriver {
  override type ParamType <: ProcessCorpusParameters
  
  override def handle_parameters() {
    need(params.input_dir, "input-dir")
    super.handle_parameters()
  }

  override def run_after_setup() {
    super.run_after_setup()

    val fileproc = create_file_processor(get_input_corpus_suffix)
    fileproc.read_schema_from_corpus(filehand, params.input_dir)
    fileproc.output_schema_file()
    fileproc.process_files(filehand, Seq(params.input_dir))
  }

  def get_input_corpus_suffix: String
  def create_file_processor(input_suffix: String): ProcessCorpusFileProcessor
}

