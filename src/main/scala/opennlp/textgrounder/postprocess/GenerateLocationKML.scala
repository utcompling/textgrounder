//  GenerateLocationKML.scala
//
//  Copyright (C) 2013-2014 Ben Wing, The University of Texas at Austin
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

class GenerateLocationKMLParameters(ap: ArgParser) {
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

  var text_field =
    ap.option[String]("text-field", metavar = "FIELD",
      help = """Field to use for text labeling markers. Default is no labeling.
""")

  if (ap.parsedValues && debug != null)
    parse_debug_spec(debug)
}

/**
 * An application to generate KML showing the "locations" (location of first
 * tweet) of users in a file as output by ParseTweets.
 */
object GenerateLocationKML extends ExperimentApp("GenerateLocationKML") {

  type TParam = GenerateLocationKMLParameters

  def create_param_object(ap: ArgParser) = new GenerateLocationKMLParameters(ap)

  val colors =
    for { i <- Seq("00", "ff", "7f")
          j <- Seq("00", "ff", "7f")
          k <- Seq("00", "ff", "7f")
        } yield {
      s"$i$j$k"
    }
    // Two other attempts to generate distinct colors from this Stack Overflow
    // article:
    // http://stackoverflow.com/questions/470690/how-to-automatically-generate-n-distinct-colors
//    Seq(
//      "FFB300", //Vivid Yellow
//      "803E75", //Strong Purple
//      "FF6800", //Vivid Orange
//      "A6BDD7", //Very Light Blue
//      "C10020", //Vivid Red
//      "CEA262", //Grayish Yellow
//      "817066", //Medium Gray
//
//      //The following will not be good for people with defective color vision
//      "007D34", //Vivid Green
//      "F6768E", //Strong Purplish Pink
//      "00538A", //Strong Blue
//      "FF7A5C", //Strong Yellowish Pink
//      "53377A", //Strong Violet
//      "FF8E00", //Vivid Orange Yellow
//      "B32851", //Strong Purplish Red
//      "F4C800", //Vivid Greenish Yellow
//      "7F180D", //Strong Reddish Brown
//      "93AA00", //Vivid Yellowish Green
//      "593315", //Deep Yellowish Brown
//      "F13A13", //Vivid Reddish Orange
//      "232C16" //Dark Olive Green
//    )
//    Seq("FE2E2E", "FE6E30", "FD9D0E", "F7E328", "CBF914", "8EFE1F", "4EF823",
//        "0EF725", "34FB83", "1EFAB8", "08F8F8", "29BEFE", "1671FA",
//        "3145F7", "5D36F9", "820BF8", "D429FF", "FF08E6", "FD31AB", "FA3872")
  

  def run_program(args: Array[String]) = {
    val rows = TextDB.read_textdb(io.localfh, params.input).toList
    val kml_colors =
      for { color <- colors } yield {
        <Style id={s"color$color"}>
          <LineStyle>
            <color>{s"FF${color}"}</color>
            <width>2</width>
          </LineStyle>
        </Style>
      }
    val kml_icon_styles =
      for { color <- colors } yield {
        <Style id={s"icon$color"}>
          <IconStyle>
            <color>{s"80${color}"}</color>
            <scale>1</scale>
            <Icon>
              <href>http://maps.google.com/mapfiles/kml/pushpin/wht-pushpin.png</href>
            </Icon>
          </IconStyle>
        </Style>
      }
    // <href>http://maps.google.com/mapfiles/kml/pal3/icon21.png</href>
    var next_color = 0
    val names = mutable.Map[String, Int]()
    val kml_placemarks =
      for {row <- rows
           coord = row.gets("coord")
           if coord != ""
           Array(lat, long) = coord.split(",")
      } yield {
        val field = params.text_field
        val color = colors(next_color % colors.size)
        if (field != null) {
          val fieldval = row.gets(field)
          if (!(names contains fieldval)) {
            names(fieldval) = next_color
            next_color += 1
          }
        }
        val name = if (field != null)
          Seq(<name>{ row.gets(field) }</name>)
        else
          Seq()

        <Placemark>
          { name }
          <styleUrl>{s"#icon$color"}</styleUrl>
          <Point>
            <coordinates>{ s"$long,$lat" }</coordinates>
          </Point>
        </Placemark>
      }

    val line_stretches =
      if (params.text_field != null) {
        (for {row <- rows
             coord = row.gets("coord")
             if coord != ""
             Array(lat, long) = coord.split(",")
          } yield (row.gets(params.text_field), s"$long,$lat")
        ).groupBy(_._1).map {
          case (field, fieldcoords) => (field, fieldcoords.map(_._2))
        }.toSeq.sortWith((x, y) => names(x._1) < names(y._1)).zipWithIndex.map {
          case ((field, coords), index) => {
            val color = colors(index % colors.size)

            <Placemark>
              <name>{field}</name>
              <visibility>0</visibility>
              <styleUrl>{s"#color$color"}</styleUrl>
              <LineString>
                <coordinates> { coords.mkString("\n") }
                </coordinates>
              </LineString>
            </Placemark>
          }
        }
      } else Seq()

    val kml =
<kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2" xmlns:kml="http://www.opengis.net/kml/2.2" xmlns:atom="http://www
.w3.org/2005/Atom">
  <Document>
    <name>{ "Twitter user locations from " + params.input }</name>
    <open>1</open>
    <Folder>
      <name>Twitter user locations</name>
      <open>1</open>
      <LookAt>
        <latitude>42</latitude>
        <longitude>-102</longitude>
        <altitude>0</altitude>
        <range>5000000</range>
        <tilt>53.454348562403</tilt>
        <heading>0</heading>
      </LookAt>
      { kml_colors }
      { kml_icon_styles }
      { kml_placemarks }
      { line_stretches }
    </Folder>
  </Document>
</kml>

    uniprint(kml.toString)

    0
  }
}
