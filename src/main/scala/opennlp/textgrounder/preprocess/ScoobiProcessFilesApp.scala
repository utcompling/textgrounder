//  GroupTwitterPull.scala
//
//  Copyright (C) 2012 Stephen Roller, The University of Texas at Austin
//  Copyright (C) 2012 Ben Wing, The University of Texas at Austin
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

import collection.JavaConversions._
import util.control.Breaks._

import java.io._
import java.lang.Double.isNaN
import java.text.{SimpleDateFormat, ParseException}

import net.liftweb
import org.apache.commons.logging
import logging.LogFactory
import org.apache.hadoop.fs.{FileSystem=>HFileSystem,_}
import org.apache.log4j.{Level=>JLevel,_}
// import com.codahale.jerkson

import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.testing.HadoopLogFactory
// import com.nicta.scoobi.application.HadoopLogFactory

import opennlp.textgrounder.util.Twokenize
import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.ioutil._
import opennlp.textgrounder.util.osutil._
import opennlp.textgrounder.util.printutil._
import opennlp.textgrounder.gridlocate.DistDocument._

class ScoobiProcessFilesParams(ap: ArgParser) {
  var debug = ap.flag("debug",
    help="""Output debug info about tweet processing/acceptance.""")
  var debug_file = ap.option[String]("debug-file",
    help="""File to write debug info to, instead of stderr.""")
//     var use_jerkson = ap.flag("use-jerkson",
//       help="""Use Jerkson instead of Lift to parse JSON.""")
  var input = ap.positional[String]("INPUT",
    help = "Source directory to read files from.")
  var output = ap.positional[String]("OUTPUT",
    help = "Destination directory to place files in.")
}

abstract class ScoobiProcessFilesShared {

  def progname: String
  val operation_category: String
  def full_operation_category = progname + "." + operation_category

  var lineno = 0
  lazy val logger = LogFactory.getLog(full_operation_category)

  def warning(line: String, fmt: String, args: Any*) {
    logger.warn("Line %d: %s: %s" format
      (lineno, fmt format (args: _*), line))
  }

  def bump_counter(counter: String) {
    incrCounter(full_operation_category, counter)
  }
}

abstract class ScoobiProcessFilesApp[ParamType <: ScoobiProcessFilesParams] extends ScoobiApp {

  def create_params(ap: ArgParser): ParamType
  def progname: String
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
}

