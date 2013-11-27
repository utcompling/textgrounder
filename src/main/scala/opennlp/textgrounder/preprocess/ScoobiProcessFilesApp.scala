//  ScoobiProcessFilesApp.scala
//
//  Copyright (C) 2012 Stephen Roller, The University of Texas at Austin
//  Copyright (C) 2012-2013 Ben Wing, The University of Texas at Austin
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

/**
 * This file provides support for a Scoobi application that processes files.
 */

package opennlp.textgrounder
package preprocess

import java.io._

import org.apache.commons.logging.LogFactory
import org.apache.log4j.{Level=>JLevel,_}
import org.apache.hadoop.fs.{FileSystem => HFileSystem, Path, FileStatus}

import com.nicta.scoobi.Scoobi._
// import com.nicta.scoobi.testing.HadoopLogFactory
import com.nicta.scoobi.application.HadoopLogFactory

import util.argparser._
import util.collection._
import util.io.FileHandler
import util.os._
import util.print._
import util.textdb._

class ScoobiProcessFilesParams(val ap: ArgParser) {
  var debug = ap.flag("debug",
    help="""Output debug info about data processing.""")
  var debug_file = ap.option[String]("debug-file",
    help="""File to write debug info to, instead of stderr.""")
  var input = ap.positional[String]("INPUT",
    help = "Source directory to read files from.")
  var output = ap.positional[String]("OUTPUT",
    help = "Destination directory to place files in.")

  /**
   * Check usage of command-line parameters and use `ap.usageError` to
   * signal that an error occurred.
   */
  def check_usage() {}

  def non_default_params = {
    for (name <- ap.argNames if (ap.specified(name))) yield
      (name, ap(name))
  }
  def non_default_params_string = {
    non_default_params.map {
      case (param,x) => (param, "%s" format x) }
  }
}

/**
 * An "action" -- simply used to encapsulate code to perform arbitrary
 * operations.  Encapsulating them like this allows information (e.g.
 * command-line options) to be made available to the routines without
 * using global variables, which won't work when the routines to be
 * executed are done in a Hadoop task rather than on the client (which
 * runs on the user's own machine, typically the Hadoop job node).
 * You need to set the values of `progname` and `operation_category`,
 * which appear in log messages (output using `warning`) and in
 * counters (incremented using `bump_counter`).
 */
trait ScoobiProcessFilesAction {

  def progname: String
  val operation_category: String
  def full_operation_category = progname + "." + operation_category

  var lineno = 0
  lazy val logger = LogFactory.getLog(full_operation_category)

  def warning(line: String, fmt: String, args: Any*) {
    logger.warn("Line %s: %s: %s" format
      (lineno, fmt format (args: _*), line))
  }

  def bump_counter(counter: String, amount: Long = 1) {
    // HACK!! Fix this by fixing Scoobi.
    try {
      incrCounter(full_operation_category, counter, amount)
    } catch {
      case e:IllegalStateException => ()
    }
  }

  /**
   * Report a non-fatal problem during operation, typically due to bad data
   * of some sort.  Logs a warning and increments a counter indicating
   * how many times this type of problem occurred. (Counters are automatically
   * added up across all mappers and reducers, and output by Scoobi at the
   * end of operations.)
   *
   * @param message Message describing the problem, without including the
   *   particular data that triggered the problem.  This is used as the name
   *   of the counter associated with the problem.
   * @param tag Optional string describing the particular data that triggered
   *   the problem.  The warning logged will be of the form `message: tag` if
   *   the tag is non-blank; otherwise it will just be `message`. (This, of
   *   course, is in additional to any prefix output by the logging mechanism
   *   itself.)
   * @return `None` (useful in the common case when a function returns
   *   `Some(x)` on success, `None` on failure)
   */
  def problem(message: String, tag: String = "") = {
    if (tag == "")
      logger.warn(message)
    else
      logger.warn("%s: %s" format (message, tag))
    bump_counter(message)
    None
  }

  /**
   * A class used internally by `error_wrap`.
   */
  private class ErrorWrapper {
    // errprint("Created an ErrorWrapper")
    var lineno = 0
  }

  /**
   * This is used to keep track of the line number.  Theoretically we should
   * be able to key off of `fun` itself but this doesn't actually work,
   * because a new object is created each time to hold the environment of
   * the function.  However, using the class of the function works well,
   * because internally each anonymous function is implemented by defining
   * a new class that underlyingly implements the function.
   */
  private val wrapper_map =
    defaultmap[Class[_], ErrorWrapper](new ErrorWrapper, setkey = true)

  /**
   * Wrapper function used to catch errors when doing line-oriented
   * (or record-oriented) processing.  It is passed a value (typically,
   * the line or record to be processed), a function to process the
   * value, and a default value to be returned upon error.  If an error
   * occurs during execution of the function, we log the line number,
   * the value that triggered the error, and the error itself (including
   * stack trace), and return the default.
   *
   * We need to create internal state in order to track the line number.
   * This function is able to handle multiple overlapping or nested
   * invocations of `error_wrap`, keyed (approximately) on the particular
   * function invoked.
   *
   * @param value Value to process
   * @param fun Function to use to process the value
   * @param default Default value to be returned during error
   */
  def error_wrap[T, U](value: T, default: => U)(fun: T => U) = {
    // errprint("error_wrap called with fun %s", fun)
    // errprint("class is %s", fun.getClass)
    val wrapper = wrapper_map(fun.getClass)
    // errprint("got wrapper %s", wrapper)
    try {
      wrapper.lineno += 1
      fun(value)
    } catch {
      case e: Exception => {
        logger.warn("Line %s: %s: %s\n%s" format
          (wrapper.lineno, e, value, stack_trace_as_string(e)))
        default
      }
    }
  }
}

abstract class ScoobiProcessFilesApp[ParamType <: ScoobiProcessFilesParams]
    extends ScoobiApp with ScoobiProcessFilesAction {

  // This is necessary to turn off the LibJars mechanism, which is somewhat
  // buggy and interferes with assemblies.
  override lazy val upload = false
  def create_params(ap: ArgParser): ParamType
  val operation_category = "MainApp"
  def output_command_line_parameters(arg_parser: ArgParser) {
    // Output using errprint() rather than logger() so that the results
    // stand out more.
    errprint("")
    errprint("Non-default parameter values:")
    for (name <- arg_parser.argNames) {
      if (arg_parser.specified(name))
        errprint("%30s: %s" format (name, arg_parser(name)))
    }
    errprint("")
    errprint("Parameter values:")
    for (name <- arg_parser.argNames) {
      errprint("%30s: %s" format (name, arg_parser(name)))
      //errprint("%30s: %s" format (name, arg_parser.getType(name)))
    }
    errprint("")
  }

  def init_scoobi_app() = {
    initialize_osutil()
    val ap = new ArgParser(progname)
    // This first call is necessary, even though it doesn't appear to do
    // anything.  In particular, this ensures that all arguments have been
    // defined on `ap` prior to parsing.
    create_params(ap)
    // Here and below, output using errprint() rather than logger() so that
    // the basic steps stand out more -- when accompanied by typical logger
    // prefixes, they easily disappear.
    errprint("Parsing args: %s" format (args mkString " "))
    ap.parse(args)
    val Opts = create_params(ap)
    Opts.check_usage()
    enableCounterLogging()
    if (Opts.debug) {
      HadoopLogFactory.setQuiet(false)
      HadoopLogFactory.setLogLevel(HadoopLogFactory.TRACE)
      LogManager.getRootLogger().setLevel(JLevel.DEBUG.asInstanceOf[JLevel])
    }
    if (Opts.debug_file != null)
      set_errout_file(Opts.debug_file)
    output_command_line_parameters(ap)
    Opts
  }

  def finish_scoobi_app(Opts: ParamType) {
    errprint("All done with everything.")
    errprint("")
    output_resource_usage()
  }

  /**
   * Given a Hadoop-style path specification (specifying a single directory,
   * a single file, or a glob), return a list of all files specified.
   */
  def files_of_path_spec(spec: String) = {
    /**
     * Expand a file status possibly referring to a directory to a list of
     * the files within. FIXME: Should this be recursive?
     */
    def expand_dirs(status: FileStatus) = {
      if (status.isDir)
        configuration.fs.listStatus(status.getPath)
      else
        Array(status)
    }

    configuration.fs.globStatus(new Path(spec)).
      flatMap(expand_dirs).
      map(_.getPath.toString)
  }

  /**
   * Output a textdb corpus given a schema and a DList of lines of text to
   * output.
   *
   * @param schema Schema object for corpus.
   * @param lines Lines of text, formatted properly for a textdb
   *   (tab-separated, etc.).
   * @param filehand File handler of output directory.
   * @param outdir Directory to output corpus in.
   * @param prefix Prefix to use for files in directory.
   */
  def dlist_output_textdb(schema: Schema, lines: DList[String],
      filehand: FileHandler, outdir: String, prefix: String) {
    // output data file
    persist(TextOutput.toTextFile(lines, outdir))
    rename_output_files(filehand, outdir, prefix)

    // output schema file
    val schema_filename =
      schema.output_constructed_schema_file(filehand, outdir + "/" + prefix)
    logger.info("Schema file output to %s" format schema_filename)
  }

  /**
   * Output a textdb corpus given a schema and an Iterable of lines of text to
   * output.
   *
   * @param schema Schema object for corpus.
   * @param lines Lines of text, formatted properly for a textdb
   *   (tab-separated, etc.).
   * @param filehand File handler of output directory.
   * @param outdir Directory to output corpus in.
   * @param prefix Prefix to use for files in directory.
   */
  def local_output_textdb(schema: Schema, lines: Iterable[String],
      filehand: FileHandler, outdir: String, prefix: String) {
    // output data file
    filehand.make_directories(outdir)
    val base = outdir + "/" + prefix
    val outfile = TextDB.construct_data_file(base)
    val outstr = filehand.openw(outfile)
    lines.map(outstr.println(_))
    outstr.close()

    // output schema file
    val schema_filename =
      schema.output_constructed_schema_file(filehand, base)
    logger.info("Schema file output to %s" format schema_filename)
  }

  /**
   * The files persisted by Scoobi have a name that we can't control
   * very much.  Move them from one directory to another and rename them
   * appropriately for a textdb corpus.
   *
   * @param srcdir Directory containing persisted files.
   * @param destdir Directory to contain results.
   * @param prefix Prefix to use for files in directory.
   * @param suffix Suffix to use for files in directory.
   */ 
  def move_output_files(filehand: FileHandler, srcdir: String, destdir: String,
      prefix: String, suffix: String = "") {
    errprint("Moving/renaming output files ...")
    val globpat = "%s/*-r-*" format srcdir
    val fs = configuration.fs
    for (file <- fs.globStatus(new Path(globpat))) {
      val path = file.getPath
      val orig_basename = path.getName
      val base = "%s/%s-%s%s" format (destdir, prefix, orig_basename, suffix)
      val newname = TextDB.construct_data_file(base)
      errprint("Moving %s to %s" format (path, newname))
      fs.rename(path, new Path(newname))
    }
  }

  /**
   * Similar to `move_output_files` but just rename them, keeping them in the
   * same directory.
   *
   * @param dir Directory containing persisted files.
   * @param prefix Prefix to use for files in directory.
   * @param suffix Suffix to use for files in directory.
   */ 
  def rename_output_files(filehand: FileHandler, dir: String,
      prefix: String, suffix: String = "") =
    move_output_files(filehand, dir, dir, prefix, suffix)
}

