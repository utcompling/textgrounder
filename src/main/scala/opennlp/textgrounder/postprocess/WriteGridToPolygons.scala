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
  var geojson =
    ap.flag("geojson", help = """Output in GeoJSON format.""")

  if (ap.parsedValues && debug != null)
    parse_debug_spec(debug)
}

/**
 * A script for converting the polygons (rectangles, really) and centroids
 * encoded in the output of geolocate.WriteGrid to a format more suitable
 * for use with R. We also output the density of documents (i.e. number of
 * documents, normalized to produce a density) in each rectangle.
 */
object WriteGridToPolygons extends ExperimentApp("WriteGridToPolygons") {

  type TParam = WriteGridToPolygonsParameters

  def create_param_object(ap: ArgParser) = new WriteGridToPolygonsParameters(ap)

  def read_normalized(file: String) = {
    val rows =
      (for (row <- TextDB.read_textdb(localfh, file)) yield (
           row, row.gets("num-documents").toInt)).toIterable
    val sum = rows.map(_._2).sum
    rows.map { case (row, ndocs) => (row, ndocs.toDouble / sum) }
  }

  def run_program(args: Array[String]) = {
    val or = Option(params.output_rectangles).map(x => localfh.openw(x))
    val oc = Option(params.output_centroids).map(x => localfh.openw(x))
    if (!params.geojson) {
      or.foreach(_.println("lat  long  group  density"))
      oc.foreach(_.println("lat  long  group  density"))
    }
    for (((row, density), group) <-
         read_normalized(params.input) zip Stream.from(1)) {
      val location = row.gets("location")
      val Array(sw, ne) = location.split(":")
      val Array(swlat, swlong) = sw.split(",")
      val Array(nelat, nelong) = ne.split(",")
      val centroid = row.gets("centroid")
      val Array(clat, clong) = centroid.split(",")
      if (swlat.toDouble > nelat.toDouble || swlong.toDouble > nelong.toDouble) {
        println(s"Skipped rectangle crossing the 180th parallel: $location")
      } else if (params.geojson) {
        or.foreach { x =>
          x.println(s"""{ "type": "Polygon", "coordinates": [ [ [ $swlong, $swlat ], [ $swlong, $nelat ], [ $nelong, $nelat ], [ $nelong, $swlat ], [ $swlong, $swlat ] ] ] }""")
        }
        oc.foreach { x =>
          x.println(s"""{ "type": "Point", "coordinates": [ $clong, $clat ] }""")
        }
      } else {
        or.foreach { x =>
          x.println(s"$swlat  $swlong  $group  $density")
          x.println(s"$nelat  $swlong  $group  $density")
          x.println(s"$nelat  $nelong  $group  $density")
          x.println(s"$swlat  $nelong   $group  $density")
        }
        oc.foreach { x =>
          x.println(s"$clat  $clong  $group  $density")
        }
      }
    }
    or.foreach(_.close())
    oc.foreach(_.close())
    0
  }
}
