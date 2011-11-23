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
import osutil._
import printutil.{errprint, set_stdout_stderr_utf_8}
import textutil._

package object experiment {
  /**
   * A general experiment driver class for programmatic access to a program
   * that runs experiments.
   *
   * Basic operation:
   *
   * 1. Create an instance of a class of type ParamType (which is determined
   *    by the particular driver implementation) and populate it with the
   *    appropriate parameters.
   * 2. Call `run()`, passing in the parameter object created in the previous
   *    step.  The return value (of type RunReturnType, again determined by
   *    the particular implementation) contains the results.
   *
   * NOTE: Some driver implementations may change the values of some of the
   * parameters recorded in the parameter object (particularly to
   * canonicalize them).
   *
   * Note that `run()` is actually a convenience method that does three steps:
   *
   * 1. `set_parameters`, which notes the parameters passed in and verifies
   *    that their values are good.
   * 2. `setup_for_run`, which does any internal setup necessary for
   *    running the experiment (e.g. reading files, creating internal
   *    structures).
   * 3. `run_after_setup`, which executes the experiment.
   *
   * These three steps have been split out because some applications may need
   * to access each step separately, or override some but not others.  For
   * example, to add Hadoop support to an application, `run_after_setup`
   * might need to be replaced with a different implementation based on the
   * MapReduce framework, while the other steps might stay more or less the
   * same.
   */

  abstract class ExperimentDriver {
    type ParamType
    type RunReturnType
    var params: ParamType = _

    /**
     * Signal a parameter error.
     */
    def param_error(string: String) {
      throw new IllegalArgumentException(string)
    }

    protected def param_needed(param: String, param_english: String = null) {
      val mparam_english =
        if (param_english == null)
          param.replace("-", " ")
        else
          param_english
      param_error("Must specify %s using --%s" format
        (mparam_english, param.replace("_", "-")))
    }

    protected def need_seq(value: Seq[String], param: String,
        param_english: String = null) {
      if (value.length == 0)
        param_needed(param, param_english)
    }

    protected def need(value: String, param: String,
        param_english: String = null) {
      if (value == null || value.length == 0)
        param_needed(param, param_english)
    }

    def set_parameters(params: ParamType) {
      this.params = params
      handle_parameters()
    }

    def run(params: ParamType) = {
      set_parameters(params)
      setup_for_run()
      run_after_setup()
    }

    /********************************************************************/
    /*                 Function to override below this line             */
    /********************************************************************/

    /**
     * Verify and canonicalize the parameters passed in.  Retrieve any other
     * parameters from the environment.  NOTE: Currently, some of the
     * fields in this structure will be changed (canonicalized).  See above.
     * If parameter values are illegal, an error will be signaled.
     */

    protected def handle_parameters()

    /**
     * Do any setup before actually implementing the experiment.  This
     * may mean, for example, loading files and creating any needed
     * structures.
     */

    def setup_for_run()

    /**
     * Actually run the experiment.  We have separated out the run process
     * into three steps because we might want to replace one of the
     * components in a sub-implementation of an experiment. (For example,
     * if we implement a Hadoop version of an experiment, typically we need
     * to replace the run_after_setup component but leave the others.)
     */

    def run_after_setup(): RunReturnType
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
   * NOTE: Although in common parlance the terms "argument" and "parameter"
   * are often synonymous, we make a clear distinction between the two.
   * We speak of "parameters" in general when referring to general settings
   * that control the operation of a program, and "command-line arguments"
   * when referring specifically to parameter setting controlled by arguments
   * specified in the command-line invocation of a program.  The latter can
   * be either "options" (specified as e.g. '--outfile myfile.txt') or
   * "positional arguments" (e.g. the arguments 'somefile.txt' and 'myfile.txt'
   * in the command 'cp somefile.txt myfile.txt').  Although command-line
   * arguments are one way of specifying parameters, parameters could also
   * come from environment variables, from a file containing program settings,
   * from the arguments to a function if the program is invoked through a
   * function call, etc.
   *
   * The general operation of this class is as follows:
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
   *     types, default values, etc. `ParamType` is the type of this class,
   *     and `create_param_object` must be implemented to create an instance
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
    type ParamType

    /**
     * Function to create an ParamType, passing in the value of `arg_parser`.
     */
    def create_param_object(ap: ArgParser): ParamType

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
     * A class for holding the parameters retrieved from the command-line
     * arguments and elsewhere ("ancillary parameters"; see above).  Note
     * that the parameters that originate in command-line arguments are
     * also stored in the ArgParser object; this class provides easy access
     * to those parameters through the preferred "field-style" paradigm
     * of the ArgParser, and also holds the ancillary parameters (if any).
     */
    var params: ParamType = _

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
      val shadow_fields = create_param_object(arg_parser)
      arg_parser.parse(args)
      params = create_param_object(arg_parser)
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
   * An object holding parameters retrieved from command-line arguments
   * using the `argparser` module, following the preferred style of that
   * module.
   *
   * @param parser the ArgParser object that does the actual parsing of
   *   command-line arguments.
   */

  abstract class ArgParserParameters(val parser: ArgParser) {
  }

  /**
   * A version of ExperimentDriver where the parameters come from command-line
   * arguments and parsing of these arguments is done using an `ArgParser`
   * object (from the `argparser` module).  The object holding the parameters
   * must be a subclass of `ArgParserParameters`, which holds a reference to
   * the `ArgParser` object that does the parsing.  Parameter errors are
   * redirected to the `ArgParser` error handler.  It's also assumed that,
   * in the general case, some of the parameters (so-called "ancillary
   * parameters") may come from sources other than command-line arguments;
   * in such a case, the function `output_ancillary_parameters` can be
   * overridden to output the values of these ancillary parameters.
   */

  abstract class ArgParserExperimentDriver extends ExperimentDriver {
    override type ParamType <: ArgParserParameters
    
    override def param_error(string: String) {
      params.parser.error(string)
    }

    /**
     * Output the values of some internal parameters.  Only needed
     * for debugging.
     */
    def output_ancillary_parameters() {}
  }

  /**
   * A general implementation of the ExperimentApp class that uses an
   * ArgParserExperimentDriver to do the actual work, so that both
   * command-line and programmatic access to the experiment-running
   * program is possible.
   *
   * Most concrete implementations will only need to implement `DriverType`,
   * `create_driver` and `create_param_object`. (The latter two functions will
   * be largely boilerplate, and are only needed at all because of type
   * erasure in Java.)
   *
   * FIXME: It's not clear that the separation between ExperimentApp and
   * ExperimentDriverApp is really worthwhile.  If not, merge the two.
   */

  abstract class ExperimentDriverApp(appname: String) extends
      ExperimentApp(appname) {
    type DriverType <: ArgParserExperimentDriver
    
    val driver = create_driver()
    type ParamType = driver.ParamType

    def create_driver(): DriverType

    override def output_ancillary_parameters() {
      driver.output_ancillary_parameters()
    }

    def initialize_parameters() {
      driver.set_parameters(params)
    }

    def run_program() = {
      driver.setup_for_run()
      driver.run_after_setup()
      0
    }
  }
}
