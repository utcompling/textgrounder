//  GeneratePredictedKML.scala
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
import util.collection._
import util.experiment._
import util.io.localfh
import util.print._

import util.debug._

class GeneratePredictedKMLParameters(ap: ArgParser) {
  var input = ap.positional[String]("input",
    must = be_specified,
    help = """Results file to analyze, a textdb database. The value can be
  any of the following: Either the data or schema file of the database;
  the common prefix of the two; or the directory containing them, provided
  there is only one textdb in the directory.""")

  var debug =
    ap.option[String]("d", "debug", metavar = "FLAGS",
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
 * An application to generate KML showing the "locations" (location of first
 * tweet) of predicted toponyms in a file output by
 * '--debug toponym-prediction-prefix'.
 */
object GeneratePredictedKML extends ExperimentApp("GeneratePredictedKML") {

  type TParam = GeneratePredictedKMLParameters

  def create_param_object(ap: ArgParser) = new GeneratePredictedKMLParameters(ap)

  def run_program(args: Array[String]) = {
    val kml_placemarks =
      for (line <- localfh.openr(params.input)) yield {
        val Array(doc, sentind, topind, toponym, selected, location, coord) =
          line.split("\t")
        val Array(lat, long) = coord.split(",")
        <Placemark>
          <name>{ toponym }</name>
          <Point>
            <coordinates>{ s"$long,$lat" }</coordinates>
          </Point>
        </Placemark>
      }

    val kml =
<kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2" xmlns:kml="http://www.opengis.net/kml/2.2" xmlns:atom="http://www
.w3.org/2005/Atom">
  <Document>
    <name>{ "Predicted toponym locations from " + params.input }</name>
    <open>1</open>
    <Folder>
      <name>Predicted toponym locations</name>
      <open>1</open>
      <LookAt>
        <latitude>42</latitude>
        <longitude>-102</longitude>
        <altitude>0</altitude>
        <range>5000000</range>
        <tilt>53.454348562403</tilt>
        <heading>0</heading>
      </LookAt>
      { kml_placemarks }
    </Folder>
  </Document>
</kml>

    uniprint(kml.toString)

    0
  }
}
