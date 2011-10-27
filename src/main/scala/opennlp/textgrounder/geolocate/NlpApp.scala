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

abstract class NlpApp extends App {
  // Things that must be implemented
  /** An instance of OptionParser, for parsing options */
  val the_op: OptionParser
  /**
   * An object containing def fields, one per command-line option.  See
   * the comments in OptParse.scala.
   */
  val the_opts: AnyRef
  /**
   * Whether to allow fields other than option def fields in `the_opts`.
   * If this is false, the presence of such fields will trigger an error.
   * If true, the fields will be allowed, but NO ZERO-ARGUMENT FUNCTIONS
   * CAN EXIST, because they will be called during parsing, and if they
   * have side effects, bad things may happen.
   */
  val allow_other_fields_in_obj: Boolean
  def handle_arguments(op: OptionParser, args: Seq[String])
  def implement_main(op: OptionParser, args: Seq[String])

  // Things that may be overridden
  def output_parameters() {}

  def output_options(op_par: OptionParser = null) {
    val op = if (op_par != null) op_par else the_op
    errprint("Parameter values:")
    for ((name, value) <- op.argNameValues) {
      errprint("%30s: %s", name, value)
      //errprint("%30s: %s", name, op.getType(name))
    }
    errprint("")
  }

  def main() = {
    set_stdout_stderr_utf_8()
    errprint("Beginning operation at %s" format curtimehuman())
    errprint("Arguments: %s" format (args mkString " "))
    the_op.parse(args, the_opts,
      allow_other_fields_in_obj = allow_other_fields_in_obj)
    handle_arguments(the_op, args)
    output_options(the_op)
    output_parameters()
    val retval = implement_main(the_op, args)
    errprint("Ending operation at %s" format curtimehuman())
    retval
  }
}

