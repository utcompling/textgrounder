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
import argparser._

/**
 A general main application class for use in an application that performs
experiments (currently used mostly for NLP -- i.e. natural language
processing -- experiments, but not limited to this).  The program is assumed
to have various parameters controlling its operation, some of which come
from command-line arguments, some from constants hard-coded into the source
code, some from environment variables or configuration files, some computed
from other parameters, etc.  We divide them into two types: command-line
(those coming from the command line) and ancillary (from any other source).
Both types of parameters are output at the beginning of execution so that
the researcher can see exactly which parameters this particular experiment
was run with.
 */

/* SCALABUG: If this param isn't declared with a 'val', we get an error
   below on the line creating ArgParser when trying to access progname,
   saying "no such field". */
abstract class ExperimentApp(val progname: String) extends App {
  // Things that must be implemented

  /**
   * Class holding the declarations and received values of the command-line
   * arguments.  Needs to have an ArgParser object passed in to it, typically
   * as a constructor parameter.
   */
  type ArgType

  /**
   * Function to create an ArgType, passing in the value of `the_argparser`.
   */
  def create_arg_class(): ArgType

  def handle_arguments(args: Seq[String])
  def implement_main(args: Seq[String])

  // Things that may be overridden
  def output_ancillary_parameters() {}

  def output_command_line_parameters() {
    errprint("Parameter values:")
    for ((name, value) <- the_argparser.argNameValues) {
      errprint("%30s: %s", name, value)
      //errprint("%30s: %s", name, op.getType(name))
    }
    errprint("")
  }

  /**
   * An instance of ArgParser, for parsing options
   */
  val the_argparser = new ArgParser(progname)

  var argholder: ArgType = _

  def main() = {
    set_stdout_stderr_utf_8()
    errprint("Beginning operation at %s" format curtimehuman())
    errprint("Arguments: %s" format (args mkString " "))
    val shadow_fields = create_arg_class()
    the_argparser.parse(args)
    argholder = create_arg_class()
    handle_arguments(args)
    output_command_line_parameters()
    output_ancillary_parameters()
    val retval = implement_main(args)
    errprint("Ending operation at %s" format curtimehuman())
    retval
  }
}

