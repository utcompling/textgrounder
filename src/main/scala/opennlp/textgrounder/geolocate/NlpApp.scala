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

import tgutil._
import OptParse._

/**
 A general main application class for use in an NLP (natural language
processing) application.  The program is assumed to have various arguments
specified on the command line, and additionally to compute some additional
"parameters" based on those arguments, the environment, etc.  Both the
arguments and parameters are output at the beginning of execution so that
the researcher can see exactly which parameters this particular experiment
was run with.
 */

/* SCALABUG: If this param isn't declared with a 'val', we get an error
   below on the line creating OptionParser when trying to access progname,
   saying "no such field". */
abstract class NlpApp(val progname: String) extends App {
  // Things that must be implemented

  type ArgClass

  def create_arg_class(): ArgClass

  def handle_arguments(args: Seq[String])
  def implement_main(args: Seq[String])

  // Things that may be overridden
  def output_internal_parameters() {}

  def output_parameters() {
    errprint("Parameter values:")
    for ((name, value) <- optparser.argNameValues) {
      errprint("%30s: %s", name, value)
      //errprint("%30s: %s", name, op.getType(name))
    }
    errprint("")
  }

  /**
   * An instance of OptionParser, for parsing options
   */
  val optparser = new OptionParser(progname)

  var argholder: ArgClass = _

  def main() = {
    set_stdout_stderr_utf_8()
    errprint("Beginning operation at %s" format curtimehuman())
    errprint("Arguments: %s" format (args mkString " "))
    val shadow_fields = create_arg_class()
    optparser.parse(args)
    argholder = create_arg_class()
    handle_arguments(args)
    output_parameters()
    output_internal_parameters()
    val retval = implement_main(args)
    errprint("Ending operation at %s" format curtimehuman())
    retval
  }
}

