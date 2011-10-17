package opennlp.textgrounder.geolocate

import NlpUtil._
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
  val the_opts: AnyRef
  val the_op: OptionParser
  def handle_arguments(op: OptionParser, args: Seq[String])
  def implement_main(op: OptionParser, args: Seq[String])

  // Things that may be overridden
  def output_parameters() {}

  def output_options(op_par: OptionParser = null) {
    val op = if (op_par != null) op_par else the_op
    errprint("Parameter values:")
    for ((name, opt) <- op.get_argmap)
      errprint("%30s: %s", name, opt.value)
    errprint("")
  }

  def need(arg: String, arg_english: String = null) {
    the_op.need(arg, arg_english)
  }

  def main() = {
    set_stdout_stderr_utf_8()
    errprint("Beginning operation at %s" format curtimehuman())
    errprint("Arguments: %s" format (args mkString " "))
    the_op.parse(the_opts, args)
    handle_arguments(the_op, args)
    output_options(the_op)
    output_parameters()
    val retval = implement_main(the_op, args)
    errprint("Ending operation at %s" format curtimehuman())
    retval
  }
}

