///////////////////////////////////////////////////////////////////////////////
//  ProcessFiles.scala
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

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.ioutil._

/*
   Common code for doing basic file-processing operations.

   FIXME: It's unclear there's enough code to justify factoring it out
   like this.
*/

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////

/**
 * Class for defining and retrieving command-line arguments.  Consistent
 * with "field-style" access to an ArgParser, this class needs to be
 * instantiated twice with the same ArgParser object, before and after parsing
 * the command line.  The first instance defines the allowed arguments in the
 * ArgParser, while the second one retrieves the values stored into the
 * ArgParser as a result of parsing.
 *
 * @param ap ArgParser object.
 */
class ProcessFilesParameters(ap: ArgParser) extends
    ArgParserParameters(ap) {
  val output_dir =
    ap.option[String]("o", "output-dir",
      metavar = "DIR",
      help = """Directory to store output files in.  It must not already
exist, and will be created (including any parent directories).""")
}

abstract class ProcessFilesDriver extends HadoopableArgParserExperimentDriver {
  override type TParam <: ProcessFilesParameters
  type TRunRes = Unit

  def handle_parameters() {
    need(params.output_dir, "output-dir")
  }

  def setup_for_run() { }

  def run_after_setup() {
    if (!get_file_handler.make_directories(params.output_dir))
      param_error("Output dir %s must not already exist" format
        params.output_dir)
  }
}
