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

  var path = ap.flag("path",
      help = """Generate paths showing movement of user instead of just
a single marker for the first location of the user.""")

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

  def single_quote_escape_string(str: String) = str.replace("'", "\\'")
  def run_program(args: Array[String]) = {
    val input_file =
      if (params.input contains "/") params.input
      else "./" + params.input
    val (dir, base) = io.localfh.split_filename(input_file)
    val rows = TextDB.read_textdb(io.localfh, dir, prefix = base)

    // FIXME!! Currently we're hard-coding a view on the SF bay. Need to
    // compute centroid and bounding box of points given.
    val prelude =
"""<!DOCTYPE html>
<html>
  <head>
    <title>Twitter user locations</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no">
    <meta charset="utf-8">
    <style>
      html, body, #map-canvas {
        margin: 0;
        padding: 0;
        height: 100%;
      }
    </style>
    <script src="https://maps.googleapis.com/maps/api/js?v=3.exp&sensor=false"></script>
    <script>
      var markers = [];
      var lines = [];
      var map;
      // Convert to hex string, pad to specified length (default 2)
      function toHex(num, len) {
        len = (typeof len === "undefined") ? 2 : len;
        var hex = Math.floor(num).toString(16);
        while (hex.length < len)
          hex = "0" + hex;
        return hex;
      }
      function initialize() {
        var mapOptions = {
          zoom: 11,
          center: new google.maps.LatLng(37.78, -122.42),
          mapTypeId: google.maps.MapTypeId.ROADMAP
        }
        map = new google.maps.Map(document.getElementById("map-canvas"),
          mapOptions);
"""

    val postlude = """
      }
      google.maps.event.addDomListener(window, 'load', initialize);
    </script>
  </head>
  <body>
    <div id="map-canvas"></div>
  </body>
</html>
"""

    val marker_code_data = """
        var latlons = [
          %s
        ];
"""
    val marker_code_postlude = """
        for (var i = 0; i < latlons.length; i++) {
          var coord = latlons[i];
          var latlng = new google.maps.LatLng(coord[0], coord[1]);
          markers.push(new google.maps.Marker({
            position: latlng,
            map: map,
            title:coord[2]
          }));
        }
"""

    val path_code_data = """
        var user_paths = [
          %s
        ];
"""
    val path_code_postlude = """
        var lineSymbol = {
          path: google.maps.SymbolPath.FORWARD_CLOSED_ARROW
        };

        for (var i = 0; i < user_paths.length; i++) {
          var user = user_paths[i][0];
          var path = user_paths[i][1];

          // create a marker for the starting position
          var marker_pos = new google.maps.LatLng(path[0][0], path[0][1]);
          var timestr = new Date(path[0][2]).toLocaleString();
          markers.push(new google.maps.Marker({
            position: marker_pos,
            map: map,
            title:user + " #0," + timestr
          }));

          // create the path coordinates
          var path_coords = [];
          for (var j = 0; j < path.length; j++) {
            var path_coord = new google.maps.LatLng(path[j][0], path[j][1]);
            path_coords.push(path_coord);
          }
          for (var j = 1; j < path.length - 1; j++) {
            var path_coord = new google.maps.LatLng(path[j][0], path[j][1]);
            // assign color based on sequence of positions, to distinguish them,
            // ranging from #000000 to #FFFFFF
            var varcolor = toHex(j * 256 / path.length);
            var timestr = new Date(path[j][2]).toLocaleString();
            markers.push(new google.maps.Marker({
              position: path_coord,
              icon: {
                path: google.maps.SymbolPath.FORWARD_CLOSED_ARROW,
                //strokeColor: "gold",
                strokeWeight: 0,
                fillColor: "#" + varcolor + varcolor + varcolor,
                fillOpacity: 0.8,
                scale: 3
              },
              map: map,
              title: user + " #" + j + ", " + timestr
            }));
          }

          // assign color based on sequence of users, to distinguish them,
          // ranging from #FF0000 to #00FFFF
          var invcolor = toHex((user_paths.length - i) * 255 / user_paths.length);
          var varcolor = toHex(i * 255 / user_paths.length);

          // create the polyline, with an arrow at the end
          lines.push(new google.maps.Polyline({
            path: path_coords,
            strokeColor: "#" + invcolor + varcolor + varcolor,
            strokeOpacity: 1.0,
            strokeWeight: 1,
            icons: [{
              icon: lineSymbol,
              offset: '100%'
            }],
            map: map
          }));
        }
"""

    val indent12 = "\n            "
    val indent14 = "\n              "
    val middle_code =
      if (params.path) {
        val lines =
          for {row <- rows
               position_field = row.gets("positions")
               if position_field != ""
               user = row.gets("user")
          } yield {
            val positions = decode_string_map(position_field)
            val position_text =
              positions map {
                case (time, coord) => "[ %s, %s ]" format (coord, time)
              } mkString ("," + indent14)
            ("[ '%s', [" + indent14 + "%s ] ]") format (
              single_quote_escape_string(user), position_text)
          }
        val path_text = lines mkString ("," + indent12)

        (path_code_data format path_text) + path_code_postlude
      } else {
        val markers =
          for {row <- rows
               coord = row.gets("coord")
               if coord != ""
               user = row.gets("user")
          } yield "[ %s, '%s' ]" format (
              coord, single_quote_escape_string(user))

        val marker_text = markers mkString ("," + indent12)

        (marker_code_data format marker_text) + marker_code_postlude
      }

    uniprint(prelude + middle_code + postlude)
    0
  }
}
