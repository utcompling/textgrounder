//  GenerateTwitterUserLocationJS.scala
//
//  Copyright (C) 2013 Ben Wing, The University of Texas at Austin
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
import util.io
import util.print._
import util.textdb._

import util.debug._

class GenerateTwitterUserLocationJSParameters(ap: ArgParser) {
  var input = ap.positional[String]("input",
    help = "Results file to analyze.")

  var debug =
    ap.option[String]("d", "debug", metavar = "FLAGS",
      help = """Output debug info of the given types.  Multiple debug
parameters can be specified, indicating different types of info to output.
Separate parameters by spaces, colons or semicolons.  Params can be boolean,
if given alone, or valueful, if given as PARAM=VALUE.  Certain params are
list-valued; multiple values are specified by including the parameter
multiple times, or by separating values by a comma.
""")
}

/**
 * An application to generate a HTML file with embedded JavaScript,
 * showing (using Google Maps) the "locations" (location of first tweet)
 * of users in a file as output by ParseTweets.
 */
object GenerateTwitterUserLocationJS extends ExperimentApp("GenerateTwitterUserLocationJS") {

  type TParam = GenerateTwitterUserLocationJSParameters

  def create_param_object(ap: ArgParser) = new GenerateTwitterUserLocationJSParameters(ap)

  def initialize_parameters() {
    if (params.debug != null)
      parse_debug_spec(params.debug)
  }

  def output_freq_of_freq(filehand: io.FileHandler, file: String,
      map: collection.Map[String, Int]) {
    val numcells = map.values.sum
    var sofar = 0
    val outf = filehand.openw(file)
    for (((cell, count), ind) <-
        map.toSeq.sortWith(_._2 > _._2).zipWithIndex) {
      sofar += count
      outf.println("%s  %s  %s  %.2f%%" format (
        ind + 1, cell, count, sofar.toDouble / numcells * 100))
    }
    outf.close()
  }

  def run_program() = {
    val filehand = io.local_file_handler
    val input_file =
      if (params.input contains "/") params.input
      else "./" + params.input
    val (dir, base) = filehand.split_filename(input_file)
    val (schema, field_iter) =
      TextDB.read_textdb_with_schema(filehand, dir, prefix = base)
    val markers =
      for {fieldvals <- field_iter.flatten
           coord = schema.get_field(fieldvals, "coord")
           if coord != ""
           user = schema.get_field(fieldvals, "user")
      } yield "[ %s, '%s' ]" format (coord, user)
    val marker_text = markers mkString ",\n            "
    uniprint(
"""<!DOCTYPE html>
<html>
  <head>
    <title>Marker example</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no">
    <meta charset="utf-8">
    <style>
      html, body, #map-canvas {
        margin: 0;
        padding: 0;
        height: 100%%;
      }
    </style>
    <script src="https://maps.googleapis.com/maps/api/js?v=3.exp&sensor=false"></script>
    <script>
      var markers = [];
      var map;
      function initialize() {
        var latlons = [
          %s
        ];

        var mapOptions = {
          zoom: 11,
          center: new google.maps.LatLng(37.78, -122.42),
          mapTypeId: google.maps.MapTypeId.ROADMAP
        }
        map = new google.maps.Map(document.getElementById("map-canvas"),
          mapOptions);

        for (var i = 0; i < latlons.length; i++) {
          var coord = latlons[i];
          var latlng = new google.maps.LatLng(coord[0], coord[1]);
          markers.push(new google.maps.Marker({
            position: latlng,
            map: map,
            title:coord[2]
          }));
        }
      }
      google.maps.event.addDomListener(window, 'load', initialize);
    </script>
  </head>
  <body>
    <div id="map-canvas"></div>
  </body>
</html>
""" format marker_text)

    0
  }
}
