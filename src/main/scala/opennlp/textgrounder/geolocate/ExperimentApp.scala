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
 * A general main application class for use in an application that performs
 * experiments (currently used mostly for experiments in NLP -- i.e. natural
 * language processing -- but not limited to this).  The program is assumed
 * to have various parameters controlling its operation, some of which come
 * from command-line arguments, some from constants hard-coded into the source
 * code, some from environment variables or configuration files, some computed
 * from other parameters, etc.  We divide them into two types: command-line
 * (those coming from the command line) and ancillary (from any other source).
 * Both types of parameters are output at the beginning of execution so that
 * the researcher can see exactly which parameters this particular experiment
 * was run with.
 *
 * The general operation is as follows:
 *
 * (1) The command line is passed in, and we parse it.  Command-line parsing
 *     uses the ArgParser class.  We use "field-style" access to the
 *     arguments retrieved from the command line; this means that there is
 *     a separate class taking the ArgParser as a construction parameter,
 *     and containing vars, one per argument, each initialized using a
 *     method call on the ArgParser that sets up all the features of that
 *     particular argument.
 * (2) Application verifies the parsed arguments passed in and initializes
 *     its parameters based on the parsed arguments and possibly other
 *     sources.
 * (3) Application is run.
 *
 * A particular application customizes things as follows:
 *
 * (1) Consistent with "field-style" access, it creates a class that will
 *     hold the user-specified values of command-line arguments, and also
 *     initializes the ArgParser with the list of allowable arguments,
 *     types, default values, etc. `ArgType` is the type of this class,
 *     and `create_arg_class` must be implemented to create an instance
 *     of this class.
 * (2) `initialize_parameters` must be implemented to handle validation
 *     of the command-line arguments and retrieval of any other parameters.
 * (3) `run_program` must, of course, be implemented, to provide the
 *     actual behavior of the application.
 */

/* SCALABUG: If this param isn't declared with a 'val', we get an error
   below on the line creating ArgParser when trying to access progname,
   saying "no such field". */
abstract class ExperimentApp(val progname: String) {
  val beginning_time = curtimesecs()

  // Things that must be implemented

  /**
   * Class holding the declarations and received values of the command-line
   * arguments.  Needs to have an ArgParser object passed in to it, typically
   * as a constructor parameter.
   */
  type ArgType

  /**
   * Function to create an ArgType, passing in the value of `arg_parser`.
   */
  def create_arg_class(ap: ArgParser): ArgType

  /**
   * Function to initialize and verify internal parameters from command-line
   * arguments and other sources.
   */
  def initialize_parameters()

  /**
   * Function to run the actual app, after parameters have been set.
   * @return Exit code of program (0 for successful completion, > 0 for
   *  an error
   */
  def run_program(): Int

  // Things that may be overridden

  /**
   * Output the values of "ancillary" parameters (see above)
   */
  def output_ancillary_parameters() {}

  /**
   * Output the values of "command-line" parameters (see above)
   */
  def output_command_line_parameters() {
    errprint("Parameter values:")
    for (name <- arg_parser.argNames) {
      errprint("%30s: %s", name, arg_parser(name))
      //errprint("%30s: %s", name, arg_parser.getType(name))
    }
    errprint("")
  }

  /**
   * An instance of ArgParser, for parsing options
   */
  val arg_parser = new ArgParser(progname)

  /**
   * A class for holding the values retrieved from the command-line
   * options.  Note that the values themselves are also stored in the
   * ArgParser object; this class provides easy access through the
   * "field-style" paradigm enabled by ArgParser.
   */
  var arg_holder: ArgType = _

  /**
   * Code to implement main entrance point.  We move this to a separate
   * function because a subclass might want to wrap the program entrance/
   * exit (e.g. a Hadoop driver).
   *
   * @param args Command-line arguments, as specified by user
   * @return Exit code, typically 0 for successful completion,
   *   positive for errorThis needs to be called explicitly from the
   */
  def implement_main(args: Array[String]) = {
    set_stdout_stderr_utf_8()
    errprint("Beginning operation at %s" format humandate_full(beginning_time))
    errprint("Arguments: %s" format (args mkString " "))
    val shadow_fields = create_arg_class(arg_parser)
    arg_parser.parse(args)
    arg_holder = create_arg_class(arg_parser)
    initialize_parameters()
    output_command_line_parameters()
    output_ancillary_parameters()
    val retval = run_program()
    val ending_time = curtimesecs()
    errprint("Ending operation at %s" format humandate_full(ending_time))
    errprint("Program running time: %s",
      format_minutes_seconds(ending_time - beginning_time))
    retval
  }

  /**
   * Actual entrance point from the JVM.  Does nothing but call
   * `implement_main`, then call `System.exit()` with the returned exit code.
   * @see #implement_main
   */
  def main(args: Array[String]) {
    val retval = implement_main(args)
    System.exit(retval)
  }
}

