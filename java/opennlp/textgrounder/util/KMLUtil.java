///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
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
package opennlp.textgrounder.util;

import opennlp.textgrounder.models.Model;
import opennlp.textgrounder.topostructs.Coordinate;

/**
 * Class of static methods for generating KML headers, footers and other stuff.
 *
 * @author tsmoon
 */
public class KMLUtil {

    public static String genKMLHeader(String inputFilename) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
              + "<kml xmlns=\"http://www.opengis.net/kml/2.2\" xmlns:gx=\"http://www.google.com/kml/ext/2.2\" xmlns:kml=\"http://www.opengis.net/kml/2.2\" xmlns:atom=\"http://www.w3.org/2005/Atom\">\n"
              + "\t<Document>\n"
              + "\t\t<Style id=\"bar\">\n"
              + "\t\t\t<PolyStyle>\n"
              + "\t\t\t\t<outline>0</outline>\n"
              + "\t\t\t</PolyStyle>\n"
              + "\t\t\t<IconStyle>\n"
              + "\t\t\t\t<Icon></Icon>\n"
              + "\t\t\t</IconStyle>\n"
              + "\t\t</Style>\n"
              + "\t\t<Folder>\n"
              + "\t\t\t<name>" + inputFilename + "</name>\n"
              + "\t\t\t<open>1</open>\n"
              + "\t\t\t<description>Distribution of place names found in " + inputFilename + "</description>\n"
              + "\t\t\t<LookAt>\n"
              + "\t\t\t\t<latitude>42</latitude>\n"
              + "\t\t\t\t<longitude>-102</longitude>\n"
              + "\t\t\t\t<altitude>0</altitude>\n"
              + "\t\t\t\t<range>5000000</range>\n"
              + "\t\t\t\t<tilt>53.454348562403</tilt>\n"
              + "\t\t\t\t<heading>0</heading>\n"
              + "\t\t\t</LookAt>\n";
    }

    public static String genKMLFooter() {
        return "\t\t</Folder>\n\t</Document>\n</kml>";
    }

    public static String genPolygon(String placename, Coordinate coord,
          double radius, String kmlPolygon) {
        return "\t\t\t<Placemark>\n"
              + "\t\t\t\t<name>" + placename + "</name>\n"
              + "\t\t\t\t<Region>"
              + "\t\t\t\t\t<LatLonAltBox>"
              + "\t\t\t\t\t\t<north>" + (coord.longitude + radius) + "</north>"
              + "\t\t\t\t\t\t<south>" + (coord.longitude - radius) + "</south>"
              + "\t\t\t\t\t\t<east>" + (coord.latitude + radius) + "</east>"
              + "\t\t\t\t\t\t<west>" + (coord.latitude - radius) + "</west>"
              + "\t\t\t\t\t</LatLonAltBox>"
              + "\t\t\t\t\t<Lod>"
              + "\t\t\t\t\t\t<minLodPixels>" + Model.MIN_LOD_PIXELS + "</minLodPixels>"
              + "\t\t\t\t\t</Lod>"
              + "\t\t\t\t</Region>"
              + "\t\t\t\t<styleUrl>#bar</styleUrl>\n"
              + "\t\t\t\t<Point>\n"
              + "\t\t\t\t\t<coordinates>\n"
              + "\t\t\t\t\t\t" + coord + ""
              + "\t\t\t\t\t</coordinates>"
              + "\t\t\t\t</Point>"
              + "\t\t\t</Placemark>"
              + "\t\t\t<Placemark>"
              + "\t\t\t\t<name>" + placename + " POLYGON</name>"
              + "\t\t\t\t<styleUrl>#bar</styleUrl>"
              + "\t\t\t\t<Style><PolyStyle><color>dc0155ff</color></PolyStyle></Style>"
              + "\t\t\t\t<Polygon>"
              + "\t\t\t\t\t<extrude>1</extrude><tessellate>1</tessellate>"
              + "\t\t\t\t\t<altitudeMode>relativeToGround</altitudeMode>"
              + "\t\t\t\t\t<outerBoundaryIs>"
              + "\t\t\t\t\t\t<LinearRing>"
              + "\t\t\t\t\t\t\t" + kmlPolygon + ""
              + "\t\t\t\t\t\t</LinearRing>"
              + "\t\t\t\t\t</outerBoundaryIs>"
              + "\t\t\t\t</Polygon>"
              + "\t\t\t</Placemark>\n";
    }

    public static String genSpiralpoint(String placename, String context,
          Coordinate spiralPoint, int j, double radius) {
        return "\t\t\t<Placemark>\n"
              + "\t\t\t\t<name>" + placename + " #" + (j + 1) + "</name>\n"
              + "\t\t\t\t<description>" + context + "</description>\n"
              + "\t\t\t\t<Region>"
              + "\t\t\t\t\t<LatLonAltBox>"
              + "\t\t\t\t\t\t<north>" + (spiralPoint.longitude + radius) + "</north>"
              + "\t\t\t\t\t\t<south>" + (spiralPoint.longitude - radius) + "</south>"
              + "\t\t\t\t\t\t<east>" + (spiralPoint.latitude + radius) + "</east>"
              + "\t\t\t\t\t\t<west>" + (spiralPoint.latitude - radius) + "</west>"
              + "\t\t\t\t\t</LatLonAltBox>"
              + "\t\t\t\t\t<Lod>"
              + "\t\t\t\t\t\t<minLodPixels>" + Model.MIN_LOD_PIXELS + "</minLodPixels>"
              + "\t\t\t\t\t</Lod>"
              + "\t\t\t\t</Region>"
              + "\t\t\t\t<styleUrl>#context</styleUrl>\n"
              + "\t\t\t\t<Point>\n"
              + "\t\t\t\t\t<coordinates>" + spiralPoint + "</coordinates>\n"
              + "\t\t\t\t</Point>\n"
              + "\t\t\t</Placemark>\n";
    }

    public static String genFloatingPlacemark(String word, Coordinate coord,
          double height) {
        return "<Placemark>"
              + "<name>" + word + "</name>"
              + "<visibility>1</visibility>"
              + "<LookAt>"
              + "<longitude>" + coord.longitude + "</longitude>"
              + "<latitude>" + coord.latitude + "</latitude>"
              + "<altitude>" + height + "</altitude>"
              + "<heading>-148.4122922628044</heading>"
              + "<tilt>40.5575073395506</tilt>"
              + "<range>500.6566641072245</range>"
              + "</LookAt>"
              + "<styleUrl>#downArrowIcon</styleUrl>"
              + "<Point>"
              + "<altitudeMode>relativeToGround</altitudeMode>"
              + "<coordinates>" + coord + ",50" + "</coordinates>"
              + "</Point>"
              + "</Placemark>";
    }
}
