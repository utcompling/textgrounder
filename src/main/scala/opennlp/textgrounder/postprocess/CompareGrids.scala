//  CompareGrids.scala
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

import collection.mutable

import util.argparser._
import util.debug._
import util.experiment._
import util.io.localfh
import util.textdb._

class CompareGridsParameters(ap: ArgParser) {
  var debug =
    ap.option[String]("debug", metavar = "FLAGS",
      help = """Output debug info of the given types.  Multiple debug
parameters can be specified, indicating different types of info to output.
Separate parameters by spaces, colons or semicolons.  Params can be boolean,
if given alone, or valueful, if given as PARAM=VALUE.  Certain params are
list-valued; multiple values are specified by including the parameter
multiple times, or by separating values by a comma.
""")
  var input1 =
    ap.option[String]("input1", "i1",
      help = """First input TextDB corpus, output by WriteGrid.""")
  var input2 =
    ap.option[String]("input2", "i2",
      help = """Second input TextDB corpus, output by WriteGrid.""")
  var output_rectangles =
    ap.option[String]("output-rectangles", "or",
      help = """File to output rectangles to.""")

  if (ap.parsedValues && debug != null)
    parse_debug_spec(debug)
}

/**
 * Compare two grids output by WriteGrid by relative number of documents.
 * Output polygons (rectangles) for input by R. We normalize the number
 * of documents for each input grid to produce a density, then divide
 * the first density by the second, and remap so it ranges from 0
 * (numdocs1 infinitely greater than numdocs2) to 0.5 (same relative
 * density of documents in both) to 1 (numdocs2 infinitely greater than
 * numdocs1).
 */
object CompareGrids extends ExperimentApp("CompareGrids") {

  type TParam = CompareGridsParameters

  def create_param_object(ap: ArgParser) = new CompareGridsParameters(ap)

  val combinedlocs = mutable.Map[String, Double]()

  def read_normalized(file: String) = {
    val locs =
      (for (row <- TextDB.read_textdb(localfh, file)) yield (
           row.gets("location"), row.gets("num-documents").toInt)).toIterable
    val sum = locs.map(_._2).sum
    locs.map { case (loc, ndocs) => (loc, ndocs.toDouble / sum) }.toMap
  }

  def run_program(args: Array[String]) = {
    val locs1 = read_normalized(params.input1)
    val locs2 = read_normalized(params.input2)
    for ((loc, prob) <- locs1) {
      val div = prob / locs2.getOrElse(loc, 0.0)
      // Remap [0,inf) -> [1,0], with 1 -> 0.5
      val remap = 1 / (1 + div)
      combinedlocs(loc) = remap
    }
    for ((loc, prob) <- locs2 if !locs1.contains(loc))
      combinedlocs(loc) = 1.0
    val or = localfh.openw(params.output_rectangles)
    or.println("lat  long  group  density")
    for (((loc, prob), group) <- combinedlocs zip Stream.from(1)) {
      val Array(sw, ne) = loc.split(":")
      val Array(swlat, swlong) = sw.split(",")
      val Array(nelat, nelong) = ne.split(",")
      if (swlat.toDouble > nelat.toDouble || swlong.toDouble > nelong.toDouble) {
        println(s"Skipped rectangle crossing the 180th parallel: $loc")
      } else {
        or.println(s"$swlat  $swlong  $group  $prob")
        or.println(s"$nelat  $swlong  $group  $prob")
        or.println(s"$nelat  $nelong  $group  $prob")
        or.println(s"$swlat  $nelong   $group  $prob")
      }
    }
    or.close()
    0
  }
}
