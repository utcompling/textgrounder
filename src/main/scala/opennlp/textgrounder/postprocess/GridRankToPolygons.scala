//  GridRankToPolygons.scala
//
//  Copyright (C) 2014 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder
package postprocess

import util.argparser._
import util.debug._
import util.experiment._
import util.io.stdfh

class GridRankToPolygonsParameters(ap: ArgParser) {
  var debug =
    ap.option[String]("debug", metavar = "FLAGS",
      help = """Output debug info of the given types.  Multiple debug
parameters can be specified, indicating different types of info to output.
Separate parameters by spaces, colons or semicolons.  Params can be boolean,
if given alone, or valueful, if given as PARAM=VALUE.  Certain params are
list-valued; multiple values are specified by including the parameter
multiple times, or by separating values by a comma.
""")

  if (ap.parsedValues && debug != null)
    parse_debug_spec(debug)
}

/**
 * A one-off script for converting the polygons (rectangles, really)
 * encoded in the output of --debug=gridrank or --debug=hier-gridrank (???)
 * to a different format, perhaps more suitable for use with R.
 * Unclear exactly what it was used for.
 */
object GridRankToPolygons extends ExperimentApp("GridRankToPolygons") {

  type TParam = GridRankToPolygonsParameters

  def create_param_object(ap: ArgParser) = new GridRankToPolygonsParameters(ap)

  def run_program(args: Array[String]) = {
    val infile = stdfh.openr("stdin")
    println("lat  long  rank  group")
    for ((line, group) <- infile zip Iterator.from(1)) {
      val fields = line.trim.split("""\s+""")
      val (sw, ne) = (fields(0), fields(1))
      val rank = if (fields.size > 2) fields(2) else group
      val Array(swlat, swlong) = sw.split(",")
      val Array(nelat, nelong) = ne.split(",")
      println(s"$swlat  $swlong  $rank  $group")
      println(s"$nelat  $swlong  $rank  $group")
      println(s"$nelat  $nelong  $rank  $group")
      println(s"$swlat  $nelong  $rank  $group")
    }
    0
  }
}
