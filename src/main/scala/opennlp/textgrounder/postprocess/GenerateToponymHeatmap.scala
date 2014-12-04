//  GenerateToponymHeatmap.scala
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
import util.experiment._
import util.io.localfh
import util.print._

import util.debug._

class GenerateToponymHeatmapParameters(ap: ArgParser) extends DebugParameters(ap) {
  var input = ap.positional[String]("input",
    must = be_specified,
    help = """File containing data to analyze, output during the toponym
resolution process, using '--debug toponym-prediction-prefix'.""")

  var title = ap.option[String]("title",
    help = """Title of KML file. If omitted, defaults to name of input file.""")
}

/**
 * An application to generate an HTML file with embedded JavaScript,
 * showing (using Google Maps) a heatmap of a bunch of coordinates, intended
 * to be the predictions of a toponym resolver as output by
 * '--debug toponym-prediction-prefix'.
 */
object GenerateToponymHeatmap extends ExperimentApp("GenerateToponymHeatmap") {

  type TParam = GenerateToponymHeatmapParameters

  def create_param_object(ap: ArgParser) = new GenerateToponymHeatmapParameters(ap)

  def run_program(args: Array[String]) = {
    val coord_list =
      for (line <- localfh.openr(params.input)) yield {
        val Array(doc, sentind, topind, toponym, selected, location, coords) =
          line.split("\t")
        coords
      }

    // FIXME!! Currently we're hard-coding a view on the SF bay. Need to
    // compute centroid and bounding box of points given.
    val prelude =
s"""<!DOCTYPE html>
<html>
  <head>
    <title>Heatmap of ${ if (params.title != null) params.title else params.input }</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no">
    <meta charset="utf-8">
    <style>
      html, body, #map-canvas {
        margin: 0;
        padding: 0;
        height: 100%;
      }
    </style>
    <script src="https://maps.googleapis.com/maps/api/js?v=3.exp&libraries=visualization"></script>
    <script>
      var latlngs = [];
      var map;
      function initialize() {
        var mapOptions = {
          zoom: 5,
          center: new google.maps.LatLng(38, -80),
          mapTypeId: google.maps.MapTypeId.ROADMAP
        }
        map = new google.maps.Map(document.getElementById("map-canvas"),
          mapOptions);
"""

    val postlude = """
        var pointArray = new google.maps.MVCArray(latlngs);

        heatmap = new google.maps.visualization.HeatmapLayer({
          data: pointArray
        });

        heatmap.setMap(map);
      }

      function toggleHeatmap() {
        heatmap.setMap(heatmap.getMap() ? null : map);
      }

      function changeGradient() {
        var gradient = [
          'rgba(0, 255, 255, 0)',
          'rgba(0, 255, 255, 1)',
          'rgba(0, 191, 255, 1)',
          'rgba(0, 127, 255, 1)',
          'rgba(0, 63, 255, 1)',
          'rgba(0, 0, 255, 1)',
          'rgba(0, 0, 223, 1)',
          'rgba(0, 0, 191, 1)',
          'rgba(0, 0, 159, 1)',
          'rgba(0, 0, 127, 1)',
          'rgba(63, 0, 91, 1)',
          'rgba(127, 0, 63, 1)',
          'rgba(191, 0, 31, 1)',
          'rgba(255, 0, 0, 1)'
        ]
        heatmap.set('gradient', heatmap.get('gradient') ? null : gradient);
      }

      function changeRadius() {
        heatmap.set('radius', heatmap.get('radius') ? null : 20);
      }

      function changeOpacity() {
        heatmap.set('opacity', heatmap.get('opacity') ? null : 0.2);
      }
      google.maps.event.addDomListener(window, 'load', initialize);
    </script>
  </head>
  <body>
    <div id="panel">
      <button onclick="toggleHeatmap()">Toggle Heatmap</button>
      <button onclick="changeGradient()">Change gradient</button>
      <button onclick="changeRadius()">Change radius</button>
      <button onclick="changeOpacity()">Change opacity</button>
    </div>
    <div id="map-canvas"></div>
  </body>
</html>
"""

    val coord_data = """
        var coords = [
          %s
        ];
"""
    val coord_code_postlude = """
        for (var i = 0; i < coords.length; i++) {
          var coord = coords[i];
          var latlng = new google.maps.LatLng(coord[0], coord[1]);
          latlngs.push(latlng);
        }
"""

    val indent12 = "\n          "
    val middle_code = {
      val coords = for {coord <- coord_list} yield "[ %s ]" format coord

      val coord_text = coords mkString ("," + indent12)

      (coord_data format coord_text) + coord_code_postlude
    }

    uniprint(prelude + middle_code + postlude)
    0
  }
}
