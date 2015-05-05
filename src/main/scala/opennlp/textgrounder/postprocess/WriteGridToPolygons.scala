//  WriteGridToPolygons.scala
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
import util.io.localfh
import util.textdb._

class WriteGridToPolygonsParameters(ap: ArgParser) {
  var debug =
    ap.option[String]("debug", metavar = "FLAGS",
      help = """Output debug info of the given types.  Multiple debug
parameters can be specified, indicating different types of info to output.
Separate parameters by spaces, colons or semicolons.  Params can be boolean,
if given alone, or valueful, if given as PARAM=VALUE.  Certain params are
list-valued; multiple values are specified by including the parameter
multiple times, or by separating values by a comma.
""")
  var input =
    ap.option[String]("input", "i",
      help = """Input TextDB corpus.""")
  var output_rectangles =
    ap.option[String]("output-rectangles", "or",
      help = """File to output rectangles to.""")
  var output_centroids =
    ap.option[String]("output-centroids", "oc",
      help = """File to output centroids to.""")

  if (ap.parsedValues && debug != null)
    parse_debug_spec(debug)
}

/**
 * A one-off script for converting the polygons (rectangles, really)
 * and centroids encoded in the output of geolocate.WriteGrid
 * to a format more suitable for use with R.
 */
object WriteGridToPolygons extends ExperimentApp("WriteGridToPolygons") {

  type TParam = WriteGridToPolygonsParameters

  def create_param_object(ap: ArgParser) = new WriteGridToPolygonsParameters(ap)

  def run_program(args: Array[String]) = {
    val or = localfh.openw(params.output_rectangles)
    or.println("lat  long  group")
    val oc = localfh.openw(params.output_centroids)
    oc.println("lat  long  group")
    for ((row, group) <-
         TextDB.read_textdb(localfh, params.input) zip Iterator.from(1)) {
      val location = row.gets("location")
      val Array(sw, ne) = location.split(":")
      val Array(swlat, swlong) = sw.split(",")
      val Array(nelat, nelong) = ne.split(",")
      if (swlat.toDouble > nelat.toDouble || swlong.toDouble > nelong.toDouble) {
        println(s"Skipped rectangle crossing the 180th parallel: $location")
      } else {
        or.println(s"$swlat  $swlong  $group")
        or.println(s"$nelat  $swlong  $group")
        or.println(s"$nelat  $nelong  $group")
        or.println(s"$swlat  $nelong   $group")
        val centroid = row.gets("centroid")
        val Array(clat, clong) = centroid.split(",")
        oc.println(s"$clat  $clong  $group")
      }
    }
    or.close()
    oc.close()
    0
  }
}
