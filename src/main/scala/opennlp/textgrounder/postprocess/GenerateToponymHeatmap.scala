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
import util.textdb.TextDB

import util.debug._

class GenerateToponymHeatmapParameters(ap: ArgParser) extends DebugParameters(ap) {
  var input = ap.option[String]("input",
    must = be_specified,
    help = """File containing data to analyze, output during the toponym
resolution process, using '--debug toponym-prediction-prefix'; a textdb
database. The value can be any of the following: Either the data or schema
file of the database; the common prefix of the two; or the directory
containing them, provided there is only one textdb in the directory.""")

  var input2 = ap.option[String]("input2",
    help = """File containing data to compare against, output during the
toponym resolution process, using '--debug toponym-prediction-prefix';
a textdb database. The value can be any of the following: Either the data
or schema file of the database; the common prefix of the two; or the directory
containing them, provided there is only one textdb in the directory.
If this file is given, and '--errors-only' is not given, the program operates
in diff mode, showing two heatmaps each containing the points in one but not
the other. If this file is given along with '--errors-only', two heatmaps
are shown, each including the errors made by one of the input files.""")

  var title = ap.option[String]("title",
    help = """Title of heatmap. If omitted, defaults to name of input file.""")

  var errors_only = ap.flag("errors-only",
    help = """Only select points where an error was made.""")

  var compare_correct = ap.flag("compare-correct",
    help = """Overlay two heatmaps, comparing predicted to correct. Currently
incompatible with diff mode (when '--input2' is used).""")
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

  def read_coords(file: String) = {
    //for (line <- localfh.openr(file)) yield {
    //  val Array(doc, sentind, topind, toponym, selected, location, coords) =
    //    line.split("\t")
    //  coords
    //}
    val rows = TextDB.read_textdb(localfh, file)
    for {row <- rows
         predcoord = row.gets("predcoord")
         correctcoord = row.gets("correctcoord")
        } 
      yield (predcoord, correctcoord)
  }

  def get_error_coords(coords: Iterable[(String, String)]) = {
    coords.filter { case (pred, correct) =>
      pred != "" && correct != "" && pred != correct
    }
  }

  def get_error_pred_coords(coords: Iterable[(String, String)]) =
    get_error_coords(coords).map(_._1)

  def run_program(args: Array[String]) = {
    val raw_coord_list = read_coords(params.input).toList
    val isdiff = params.input2 != null
    val erronly = params.errors_only
    val comparecorr = params.compare_correct
    val raw_coord2_list = if (isdiff) read_coords(params.input2).toList else Seq()
    val (coord_list_1, coord2_list_1) =
      if (comparecorr) {
        val pred_correct_list =
          if (erronly)
            get_error_coords(raw_coord_list)
          else
            raw_coord_list
        pred_correct_list.unzip
      } else if (!erronly) {
        if (!isdiff)
          (raw_coord_list.map(_._1), Seq())
        else {
          //println("Raw coord list:")
          //println(raw_coord_list.take(50))
          //println("Raw coord2 list:")
          //println(raw_coord2_list.take(50))
          //println("Zipped:")
          //println(raw_coord_list zip raw_coord2_list)
          val difflist = raw_coord_list.map(_._1) zip raw_coord2_list.map(_._1) filter { x => x._1 != x._2 }
          difflist.unzip
        }
      } else {
        //erronly is set
        if (!isdiff)
          (get_error_pred_coords(raw_coord_list), Seq())
        else
          (get_error_pred_coords(raw_coord_list), get_error_pred_coords(raw_coord2_list))
      }
    val coord_list = coord_list_1.filter(_ != "")
    val coord2_list = coord2_list_1.filter(_ != "")
    val twoway = coord2_list.size > 0

    // FIXME!! Currently we're hard-coding a viewpoint. Need to
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
      var latlngs2 = [];
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

    val regheatmapsetup = """
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
        heatmap.set('gradient', heatmap.get('gradient') ? null : gradient);
      }

      function changeRadius() {
        var curval = heatmap.get('radius');
        heatmap.set('radius', curval == null ? 20 : curval == 50 ? null : curval + 10);
      }

      function changeOpacity() {
        heatmap.set('opacity', heatmap.get('opacity') ? null : 0.2);
      }
"""

    val diffheatmapsetup = """
        var pointArray = new google.maps.MVCArray(latlngs);
        var pointArray2 = new google.maps.MVCArray(latlngs2);

        heatmap = new google.maps.visualization.HeatmapLayer({
          data: pointArray
        });
        heatmap2 = new google.maps.visualization.HeatmapLayer({
          data: pointArray2
        });

        heatmap.setMap(map);
        heatmap2.setMap(map);
        heatmap2.set('gradient', gradient);
        heatmap.set('radius', 20);
        heatmap2.set('radius', 20);
      }

      function toggleHeatmap1() {
        heatmap.setMap(heatmap.getMap() ? null : map);
      }

      function toggleHeatmap2() {
        heatmap2.setMap(heatmap2.getMap() ? null : map);
      }

      function changeGradient() {
        heatmap.set('gradient', heatmap.get('gradient') ? null : gradient);
        heatmap2.set('gradient', heatmap2.get('gradient') ? null : gradient);
      }

      function changeRadius() {
        var curval = heatmap.get('radius');
        heatmap.set('radius', curval == null ? 20 : curval == 50 ? null : curval + 10);
        var curval2 = heatmap2.get('radius');
        heatmap2.set('radius', curval2 == null ? 20 : curval2 == 50 ? null : curval2 + 10);
      }

      function changeOpacity() {
        heatmap.set('opacity', heatmap.get('opacity') ? null : 0.2);
        heatmap2.set('opacity', heatmap2.get('opacity') ? null : 0.2);
      }
"""

    val postlude = s"""
      google.maps.event.addDomListener(window, 'load', initialize);
    </script>
  </head>
  <body>
    <div id="panel">
      ${if (twoway) """<button onclick="toggleHeatmap1()">Toggle Heatmap1</button>
      <button onclick="toggleHeatmap2()">Toggle Heatmap2</button>"""
        else """<button onclick="toggleHeatmap()">Toggle Heatmap</button>"""}
      <button onclick="changeGradient()">Change gradient</button>
      <button onclick="changeRadius()">Change radius</button>
      <button onclick="changeOpacity()">Change opacity</button>
    </div>
    <div id="map-canvas"></div>
  </body>
</html>
"""

    val coord_data = """
        var %s = [
          %s
        ];
"""
    val coord_code_postlude = """
        for (var i = 0; i < %s.length; i++) {
          var coord = %s[i];
          var latlng = new google.maps.LatLng(coord[0], coord[1]);
          %s.push(latlng);
        }
"""

    val indent12 = "\n          "
    val middle_code = if (twoway) {
      val coords = for {coord <- coord_list} yield "[ %s ]" format coord
      val coord_text = coords mkString ("," + indent12)
      val coords2 = for {coord <- coord2_list} yield "[ %s ]" format coord
      val coord2_text = coords2 mkString ("," + indent12)

      (coord_data format ("coords", coord_text)) +
        (coord_data format ("coords2", coord2_text)) +
        (coord_code_postlude format ("coords", "coords", "latlngs")) +
        (coord_code_postlude format ("coords2", "coords2", "latlngs2"))

    } else {
      val coords = for {coord <- coord_list} yield "[ %s ]" format coord

      val coord_text = coords mkString ("," + indent12)

      (coord_data format ("coords", coord_text)) + (
        coord_code_postlude format ("coords", "coords", "latlngs")
      )
    }

    uniprint(prelude + middle_code + (if (twoway) diffheatmapsetup else regheatmapsetup)
      + postlude)
    0
  }
}
