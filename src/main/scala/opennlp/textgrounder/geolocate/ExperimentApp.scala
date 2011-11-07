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

/**
 * A general experiment driver class for programmatic access to a program
 * that runs experiments.
 */

abstract class ExperimentDriver {
  type ArgType
  type RunReturnType
  var params: ArgType = _

  protected var argerror = default_error_handler _

  /**
   * Default error handler for signalling an argument error.
   */
  def default_error_handler(string: String) {
    throw new IllegalArgumentException(string)
  }

  /**
   * Change the error handler.  Return the old handler.
   */
  def set_error_handler(handler: String => Unit) = {
    /* FUCK ME TO HELL.  Want to make this either a function to override
       or a class parameter, but both get awkward because Java type erasure
       means there's no easy way for a generic class to create a new object
       of the generic type. */
    val old_handler = argerror
    argerror = handler
    old_handler
  }

  protected def argument_needed(arg: String, arg_english: String = null) {
    val marg_english =
      if (arg_english == null)
        arg.replace("-", " ")
      else
        arg_english
    argerror("Must specify %s using --%s" format
      (marg_english, arg.replace("_", "-")))
  }

  def need_seq(value: Seq[String], arg: String, arg_english: String = null) {
    if (value.length == 0)
      argument_needed(arg, arg_english)
  }

  def need(value: String, arg: String, arg_english: String = null) {
    if (value == null || value.length == 0)
      argument_needed(arg, arg_english)
  }

  def run(args: ArgType) = {
    this.params = args
    handle_parameters(this.params)
    setup_for_run()
    run_after_setup()
  }

  /********************************************************************/
  /*                 Function to override below this line             */
  /********************************************************************/

  /**
   * Output the values of some internal parameters.  Only needed
   * for debugging.
   */
  def output_ancillary_parameters() {}

  /**
   * Verify and canonicalize the given command-line arguments.  Retrieve
   * any other parameters from the environment.  NOTE: Currently, some of the
   * fields in this structure will be changed (canonicalized).  See above.
   * If options are illegal, an error will be signaled.
   *
   * @param options Object holding options to set
   */

  def handle_parameters(Args: ArgType)

  /**
   * Do any setup before actually implementing the experiment.  This
   * typically means loading files and creating any needed structures.
   */

  def setup_for_run()

  /**
   * Actually run the experiment.  We have separated out the run process
   * into the above three steps because we might want to replace one
   * of the components in a sub-implementation of an experiment. (For example,
   * if we implement a Hadoop version of an experiment, typically we need
   * to replace the run_after_setup component but leave the others.)
   */

  def run_after_setup(): RunReturnType
}

/**
 * A general implementation of the ExperimentApp class that uses an
 * ExperimentDriver to do the actual work, so that both command-line and
 * programmatic access to the experiment-running program is possible.
 *
 * Most concrete implementations will only need to implement DriverType,
 * ArgType, create_driver and create_arg_class.
 */

abstract class ExperimentDriverApp(appname: String) extends
    ExperimentApp(appname) {
  type DriverType <: ExperimentDriver
  
  val driver = create_driver()
  type ArgType = driver.ArgType

  def create_driver(): DriverType

  def arg_error(str: String) {
    arg_parser.error(str)
  }

  override def output_ancillary_parameters() {
    driver.output_ancillary_parameters()
  }

  def initialize_parameters() {
    driver.set_error_handler(arg_error _)
    driver.handle_parameters(arg_holder.asInstanceOf[driver.ArgType])
  }

  def run_program() = {
    driver.run(arg_holder)
    0
  }
}

