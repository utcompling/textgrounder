package opennlp.textgrounder.util

import collection._

class DebugSettings {
  // Debug params.  Different params indicate different info to output.
  // Specified using --debug.  Multiple params are separated by spaces,
  // colons or semicolons.  Params can be boolean, if given alone, or
  // valueful, if given as PARAM=VALUE.  Certain params are list-valued;
  // multiple values are specified by including the parameter multiple
  // times, or by separating values by a comma.
  val debug = booleanmap[String]()
  val debugval = stringmap[String]()
  val debuglist = bufmap[String, String]()

  var list_debug_params = Set[String]()

  // Register a list-valued debug param.
  def register_list_debug_param(param: String) {
    list_debug_params += param
  }

  def parse_debug_spec(debugspec: String) {
    val params = """[,;\s]+""".r.split(debugspec)
    // Allow params with values, and allow lists of values to be given
    // by repeating the param
    for (f <- params) {
      if (f contains '=') {
        val Array(param, value) = f.split("=", 2)
        if (list_debug_params contains param) {
          val values = ":".split(value)
          debuglist(param) ++= values
        } else
          debugval(param) = value
      } else
        debug(f) = true
    }
  }
}

package object debug extends DebugSettings { }
