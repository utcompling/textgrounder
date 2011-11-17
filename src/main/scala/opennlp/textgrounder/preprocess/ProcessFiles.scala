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

import java.io.InputStream
import java.util.zip.GZIPInputStream

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.ioutil._

/*
Common code for doing basic file-processing operations.
*/

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////

/**
 * Class retrieving command-line arguments or storing programmatic
 * configuration parameters.
 *
 * @param parser If specified, should be a parser for retrieving the
 *   value of command-line arguments from the command line.  Provided
 *   that the parser has been created and initialized by creating a
 *   previous instance of this same class with the same parser (a
 *   "shadow field" class), the variables below will be initialized with
 *   the values given by the user on the command line.  Otherwise, they
 *   will be initialized with the default values for the parameters.
 *   Because they are vars, they can be freely set to other values.
 *
 */
class ProcessFilesParameters(parser: ArgParser) extends
    ArgParserParameters(parser) {
  protected val ap = parser

  var output_dir =
    ap.option[String]("o", "output-dir",
      metavar = "DIR",
      help = """Directory to store output files in.""")

  var files =
    ap.multiPositional[String]("infile",
      help = """File(s) to process for input.""")
}

abstract class ProcessFilesDriver extends ArgParserExperimentDriver {
  override type ParamType <: ProcessFilesParameters
  type RunReturnType = Unit

  val filehand = new LocalFileHandler
  
  def usage()

  def handle_parameters() {
    need(params.output_dir, "output-dir")
    if (!filehand.make_directories(params.output_dir))
      param_error("Output dir %s must not already exist" format
        params.output_dir)
  }

  def setup_for_run() { }

  def process_one_file(lines: Iterator[String], outname: String)

  def iterate_input_files(process: (Iterator[String], String) => Unit) {
    for (file <- params.files) {
      errprint("Processing %s..." format file)
      val (lines, _, realname) = filehand.openr_with_compression_info(file)
      var (_, outname) = filehand.split_filename(realname)
      process(lines, outname)
    }
  }

  def run_after_setup() {
    iterate_input_files(process_one_file _)
  }
}
