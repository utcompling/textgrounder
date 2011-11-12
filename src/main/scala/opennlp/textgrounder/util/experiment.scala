//////////////////////////////////////////////////////////////////////////////
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

package opennlp.textgrounder.util

import scala.collection.mutable

import argparser._
import collectionutil._
import ioutil._
import osutil._
import textutil._

package object experiment {
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
   * A mix-in that adds to a driver the ability to record statistics
   * (specifically, counters that can be incremented and queried) about
   * the experiment run.  These counters may be tracked globally across
   * a set of separate tasks, e.g. in Hadoop where multiple separate tasks
   * may be run in parallel of different machines to completely a global
   * job.  Counters can be tracked both globally and per-task.
   */
  abstract trait ExperimentDriverStats {
    /** Set of counters under the given group, using fully-qualified names. */
    protected val counters_by_group = setmap[String,String]()
    /** Set of counter groups under the given group, using fully-qualified
     * names. */
    protected val counter_groups_by_group = setmap[String,String]()
    /** Set of all counters seen. */
    protected val counters_by_name = mutable.Set[String]()

    protected def local_to_full_name(name: String) =
      if (name == "")
        local_counter_group
      else
        local_counter_group + "." + name

    /**
     * Note that a counter of the given name exists.  It's not necessary to
     * do this before calling `increment_counter` or `get_counter` because
     * unseen counters default to 0, but doing so ensures that the counter
     * is seen when we enumerate counters even if it has never been incremented.
     */
    def note_counter(name: String) {
      if (!(counters_by_name contains name)) {
        counters_by_name += name
        note_counter_by_group(name, true)
      }
    }

    protected def split_counter(name: String) = {
      val lastsep = name.lastIndexOf('.')
      if (lastsep < 0)
        ("", name)
      else
        (name.slice(0, lastsep), name.slice(lastsep + 1, name.length))
    }

    /**
     * Note each part of the name in the next-higher group.  For example,
     * for a name "io.sort.spill.percent", we note the counter "percent" in
     * group "io.sort.spill", but also the group "spill" in the group "io.sort",
     * the group "sort" in the group "io", and the group "io" in the group "".
     *
     * @param name Name of the counter
     * @param is_counter Whether this is an actual counter, or a group.
     */
    private def note_counter_by_group(name: String, is_counter: Boolean) {
      val (group, _) = split_counter(name)
      if (is_counter)
        counters_by_group(group) += name
      else
        counter_groups_by_group(group) += name
      if (group != "")
        note_counter_by_group(group, false)
    }

    /**
     * Enumerate all the counters in the given group, for counters
     * which have been either incremented or noted (using `note_counter`).
     *
     * @param group Group to list counters under.  Can be set to the
     *   empty string ("") to list from the top level.
     * @param recursive If true, search recursively under the group;
     *   otherwise, only list those at the level immediately under the group.
     * @param fully_qualified If true (the default), all counters returned
     *   are fully-qualified, i.e. including the entire counter name.
     *   Otherwise, only the portion after `group` is returned.
     * @returns An iterator over counters
     */
    def list_counters(group: String, recursive: Boolean,
        fully_qualified: Boolean = true) = {
      val groups = Iterable(group)
      val subgroups =
        if (!recursive)
          Iterable[String]()
        else
          list_counter_groups(group, true)
      val fq = (groups.view ++ subgroups) flatMap (counters_by_group(_))
      if (fully_qualified) fq
      else fq map (_.stripPrefix(group + "."))
    }

    def list_local_counters(group: String, recursive: Boolean,
        fully_qualified: Boolean = true) =
      (list_counters(local_to_full_name(group), recursive, fully_qualified) map
        (_.stripPrefix(local_counter_group + ".")))

    /**
     * Enumerate all the counter groups in the given group, for counters
     * which have been either incremented or noted (using `note_counter`).
     *
     * @param group Group to list counter groups under.  Can be set to the
     *   empty string ("") to list from the top level.
     * @param recursive If true, search recursively under the group;
     *   otherwise, only list those at the level immediately under the group.
     * @param fully_qualified If true (the default), all counter groups returned
     *   are fully-qualified, i.e. including the entire counter name.
     *   Otherwise, only the portion after `group` is returned.
     * @returns An iterator over counter groups
     */
    def list_counter_groups(group: String, recursive: Boolean,
        fully_qualified: Boolean = true): Iterable[String] = {
      val groups = counter_groups_by_group(group).view
      val fq =
        if (!recursive)
          groups
        else
          groups ++ (groups flatMap (list_counter_groups(_, true)))
      if (fully_qualified) fq
      else fq map (_.stripPrefix(group + "."))
    }

    def list_local_counter_groups(group: String, recursive: Boolean,
        fully_qualified: Boolean = true) =
      (list_counter_groups(local_to_full_name(group), recursive,
        fully_qualified) map (_.stripPrefix(local_counter_group + ".")))

    /**
     * Increment the given local counter by 1.  Local counters are those
     * specific to this application rather than counters set by the overall
     * framework. (The difference is that local counters are placed in their
     * own group, as specified by `local_counter_group`.)
     */
    def increment_local_counter(name: String) {
      increment_local_counter(name, 1)
    }

    /**
     * Increment the given local counter by the given value.  Local counters
     * are those specific to this application rather than counters set by the
     * overall framework. (The difference is that local counters are placed in
     * their own group, as specified by `local_counter_group`.)
     */
    def increment_local_counter(name: String, byvalue: Long) {
      increment_counter(local_to_full_name(name), byvalue)
    }

    /**
     * Increment the given fully-named counter by 1.  Global counters are those
     * provided by the overall framework rather than specific to this
     * application; hence there is rarely cause for changing them.
     */
    def increment_counter(name: String) {
      increment_counter(name, 1)
    }

    /**
     * Increment the given fully-named counter by the given value.
     *
     * @see increment_local_counter
     */
    def increment_counter(name: String, byvalue: Long) {
      note_counter(name)
      do_increment_counter(name, byvalue)
    }

    /**
     * Return the value of the given local counter.
     *
     * See `increment_local_counter` for a discussion of local counters,
     * and `get_counter` for a discussion of caveats in a multi-process
     * environment.
     */
    def get_local_counter(name: String) =
      get_counter(local_to_full_name(name))

    /**
     * Return the value of the given fully-named counter.  Note: When operating
     * in a multi-threaded or multi-process environment, it cannot be guaranteed
     * that this value is up-to-date.  This is especially the case e.g. when
     * operating in Hadoop, where counters are maintained globally across
     * tasks which are running on different machines on the network.
     *
     * @see get_local_counter
     */
    def get_counter(name: String) = do_get_counter(name)

    /******************* Override/implement below this line **************/

    /**
     * Group that local counters are placed in.
     */
    val local_counter_group: String 

    /**
     * Return ID of current task.  This is used for Hadoop or similar, to handle
     * operations that may be run multiple times per task in an overall job.
     * In a standalone environment, this should always return the same value.
     */
    def get_task_id: Int

    /**
     * Underlying implementation to increment the given counter by the
     * given value.
     */
    protected def do_increment_counter(name: String, byvalue: Long)

    /**
     * Underlying implementation to return the value of the given counter.
     */
    protected def do_get_counter(name: String): Long
  }

  abstract class ExperimentParameters(val parser: ArgParser) {
  }

  /**
   * A general experiment driver class for programmatic access to a program
   * that runs experiments.
   *
   * Basic operation:
   *
   * 1. Create an instance of the appropriate subclass of GeolocateParameters
   * (e.g. GeolocateDocumentParameters for document geolocation) and populate
   * it with the appropriate parameters.  Don't pass in any ArgParser instance,
   * as is the default; that way, the parameters will get initialized to their
   * default values, and you only have to change the ones you want to be
   * non-default.
   * 2. Call set_parameters(), passing in the instance you just created.
   * 3. Call run().  The return value contains some evaluation results.
   *
   * NOTE: Currently, the GeolocateParameters-subclass instance is recorded
   * directly inside of this singleton object, without copying, and some of the
   * fields are changed to more canonical values.  If this is a problem, let me
   * know and I'll fix it.
   *
   */

  abstract class ExperimentDriver {
    type ArgType <: ExperimentParameters
    type RunReturnType
    var params: ArgType = _

    /**
     * Signal an argument error.
     */
    def argerror(string: String) {
      // throw new IllegalArgumentException(string)
      params.parser.error(string)
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
     * If options are illegal, an error will be signaled.-   *
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

    override def output_ancillary_parameters() {
      driver.output_ancillary_parameters()
    }

    def initialize_parameters() {
      driver.handle_parameters(arg_holder)
    }

    def run_program() = {
      driver.run(arg_holder)
      0
    }
  }

}
