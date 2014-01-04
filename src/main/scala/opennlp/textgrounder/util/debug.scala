package opennlp.textgrounder.util

import collection._

protected class DebugPackage {
  // Debug params.  Different params indicate different info to output.
  // Specified using --debug.  Multiple params are separated by spaces,
  // commas or semicolons.  Params can be boolean, if given alone, or
  // valueful, if given as PARAM=VALUE.  Certain params are list-valued;
  // multiple values are specified by separating values by a colon.
  val debug = booleanmap[String]()
  val debugval = stringmap[String]()

  def parse_debug_spec(debugspec: String) {
    val params = """[,;\s]+""".r.split(debugspec)
    // Allow params with values, and allow lists of values to be given
    // by repeating the param
    for (f <- params) {
      if (f contains '=') {
        val Array(param, value) = f.split("=", 2)
        debugval(param) = value
      } else
        debug(f) = true
    }
  }

  /**
   * Retrieve an integer-valued debug param.
   */
  def debugint(param: String, default: Int) = {
    val v = debugval(param)
    if (v != "") v.toInt else default
  }

  /**
   * Retrieve a double-valued debug param.
   */
  def debugdouble(param: String, default: Double) = {
    val v = debugval(param)
    if (v != "") v.toDouble else default
  }

  /**
   * Retrieve a list-valued debug param.
   */
  def debuglist(param: String) = {
    val value = debugval(param)
    if (value == "")
      Seq[String]()
    else
     ":".split(value).toSeq
  }
}

package object debug extends DebugPackage { }
