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

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import opennlp.textgrounder.models.Model;
import opennlp.textgrounder.topostructs.Coordinate;

/**
 * Class of static methods for generating KML headers, footers and other stuff.
 *
 * @author tsmoon
 */
public class KMLUtil {
  // Minimum number of pixels the (small) square region (NOT our Region) represented by each city must occupy on the screen for its label to appear:
  public final static int MIN_LOD_PIXELS = 16;

  protected static void writeWithCharacters(XMLStreamWriter w, String localName, String text)
    throws XMLStreamException {
    w.writeStartElement(localName);
    w.writeCharacters(text);
    w.writeEndElement();
  }

  public static void writeHeader(XMLStreamWriter w, String name)
    throws XMLStreamException {
    w.writeStartDocument("UTF-8", "1.0");
    w.writeStartElement("kml");
    w.writeDefaultNamespace("http://www.opengis.net/kml/2.2");
    w.writeNamespace("gx", "http://www.google.com/kml/ext/2.2");
    w.writeNamespace("kml", "http://www.opengis.net/kml/2.2");
    w.writeNamespace("atom", "http://www.w3.org/2005/Atom");
    w.writeStartElement("Document");

    w.writeStartElement("Style");
    w.writeAttribute("id", "bar");
    w.writeStartElement("PolyStyle");
    KMLUtil.writeWithCharacters(w, "outline", "0");
    w.writeEndElement(); // PolyStyle
    w.writeStartElement("IconStyle");
    w.writeEmptyElement("Icon");
    w.writeEndElement(); // IconStyle
    w.writeEndElement(); // Style

    w.writeStartElement("Style");
    w.writeAttribute("id", "downArrowIcon");
    w.writeStartElement("IconStyle");
    w.writeStartElement("Icon");
    KMLUtil.writeWithCharacters(w, "href", "http://maps.google.com/mapfiles/kml/pal4/icon28.png");
    w.writeEndElement(); // Icon
    w.writeEndElement(); // IconStyle
    w.writeEndElement(); // Style

    w.writeStartElement("Style");
    w.writeAttribute("id", "smallDownArrowIcon");
    w.writeStartElement("IconStyle");
    KMLUtil.writeWithCharacters(w, "scale", "0.25");
    w.writeStartElement("Icon");
    KMLUtil.writeWithCharacters(w, "href", "http://maps.google.com/mapfiles/kml/pal4/icon28.png");
    w.writeEndElement(); // Icon
    w.writeEndElement(); // IconStyle
    w.writeEndElement(); // Style

    w.writeStartElement("Style");
    w.writeAttribute("id", "noIcon");
    w.writeStartElement("IconStyle");
    w.writeEmptyElement("Icon");
    w.writeEndElement(); // IconStyle
    w.writeEndElement(); // Style

    w.writeStartElement("Folder");
    KMLUtil.writeWithCharacters(w, "name", name);
    KMLUtil.writeWithCharacters(w, "open", "1");
    KMLUtil.writeWithCharacters(w, "description", "Distribution of place names found in " + name);
    KMLUtil.writeLookAt(w, 42, -102, 0, 5000000, 53.454348562403, 0);
  }

  public static void writeFooter(XMLStreamWriter w)
    throws XMLStreamException {
    w.writeEndElement(); // Folder
    w.writeEndElement(); // Document
    w.writeEndElement(); // kml
  }

  public static void writeCoordinate(XMLStreamWriter w, Coordinate coord,
                                     int sides, double radius, double height)
    throws XMLStreamException {
    final double radianUnit = 2 * Math.PI / sides;
    final double startRadian = radianUnit / 2;

    w.writeStartElement("coordinates");
    w.writeCharacters("\n");
    for (double currentRadian = startRadian; currentRadian <= 2 * Math.PI + startRadian; currentRadian += radianUnit) {
      double lat = coord.latitude + radius * Math.cos(currentRadian);
      double lon = coord.longitude + radius * Math.sin(currentRadian);
      w.writeCharacters(String.format("%f,%f,%f\n", lon, lat, height));
    }

    w.writeEndElement(); // coordinates
  }

  public static void writeRegion(XMLStreamWriter w, Coordinate coord, double radius)
    throws XMLStreamException {
    w.writeStartElement("Region");
    w.writeStartElement("LatLonAltBox");
    KMLUtil.writeWithCharacters(w, "north", String.format("%f", coord.latitude + radius));
    KMLUtil.writeWithCharacters(w, "south", String.format("%f", coord.latitude - radius));
    KMLUtil.writeWithCharacters(w, "east", String.format("%f", coord.longitude + radius));
    KMLUtil.writeWithCharacters(w, "west", String.format("%f", coord.longitude - radius));
    w.writeEndElement(); // LatLonAltBox
    w.writeStartElement("Lod");
    KMLUtil.writeWithCharacters(w, "minLodPixels", Integer.toString(KMLUtil.MIN_LOD_PIXELS));
    w.writeEndElement(); // Lod
    w.writeEndElement(); // Region
  }

  public static void writeLookAt(XMLStreamWriter w, double lat, double lon,
                                 double alt, double range, double tilt, double heading)
    throws XMLStreamException {
    w.writeStartElement("LookAt");
    KMLUtil.writeWithCharacters(w, "latitude", String.format("%f", lat));
    KMLUtil.writeWithCharacters(w, "longitude", String.format("%f", lon));
    KMLUtil.writeWithCharacters(w, "altitude", String.format("%f", alt));
    KMLUtil.writeWithCharacters(w, "range", String.format("%f", range));
    KMLUtil.writeWithCharacters(w, "tilt", String.format("%f", tilt));
    KMLUtil.writeWithCharacters(w, "heading", String.format("%f", heading));
    w.writeEndElement(); // LookAt
  }

  public static void writePolygon(XMLStreamWriter w, String name,
                                  Coordinate coord, int sides, double radius, double height)
    throws XMLStreamException {
    w.writeStartElement("Placemark");
    KMLUtil.writeWithCharacters(w, "name", name);
    KMLUtil.writeRegion(w, coord, radius);
    KMLUtil.writeWithCharacters(w, "styleUrl", "#bar");
    w.writeStartElement("Point");
    KMLUtil.writeWithCharacters(w, "coordinates", String.format("%f,%f", coord.longitude, coord.latitude));
    w.writeEndElement(); // Point
    w.writeEndElement(); // Placemark
    w.writeStartElement("Placemark");
    KMLUtil.writeWithCharacters(w, "name", name + " POLYGON");
    KMLUtil.writeWithCharacters(w, "styleUrl", "#bar");
    w.writeStartElement("Style");
    w.writeStartElement("PolyStyle");
    KMLUtil.writeWithCharacters(w, "color", "dc0155ff");
    w.writeEndElement(); // PolyStyle
    w.writeEndElement(); // Style
    w.writeStartElement("Polygon");
    KMLUtil.writeWithCharacters(w, "extrude", "1");
    KMLUtil.writeWithCharacters(w, "tessellate", "1");
    KMLUtil.writeWithCharacters(w, "altitudeMode", "relativeToGround");
    w.writeStartElement("outerBoundaryIs");
    w.writeStartElement("LinearRing");
    KMLUtil.writeCoordinate(w, coord, sides, radius, height);
    w.writeEndElement(); // LinearRing
    w.writeEndElement(); // outerBoundaryIs
    w.writeEndElement(); // Polygon
    w.writeEndElement(); // Placemark
  }

  public static void writeSpiralPoint(XMLStreamWriter w, String name, int id,
                                      String context, Coordinate coord, double radius)
    throws XMLStreamException {
    w.writeStartElement("Placemark");
    KMLUtil.writeWithCharacters(w, "name", String.format("%s #%d", name, id + 1));
    KMLUtil.writeWithCharacters(w, "description", context);
    KMLUtil.writeRegion(w, coord, radius);
    KMLUtil.writeWithCharacters(w, "styleUrl", "#context");
    w.writeStartElement("Point");
    KMLUtil.writeWithCharacters(w, "coordinates", String.format("%f,%f", coord.longitude, coord.latitude));
    w.writeEndElement(); // Point
    w.writeEndElement(); // Placemark
  }

  public static void writeFloatingPlacemark(XMLStreamWriter w, String name,
                                            Coordinate coord, double height)
    throws XMLStreamException {
    KMLUtil.writeFloatingPlacemark(w, name, coord, height, "#smallDownArrowIcon");
  }

  public static void writeFloatingPlacemark(XMLStreamWriter w, String name,
                                            Coordinate coord, double height,
                                            String styleUrl)
    throws XMLStreamException {
    w.writeStartElement("Placemark");
    KMLUtil.writeWithCharacters(w, "name", name);
    KMLUtil.writeWithCharacters(w, "visibility", "1");
    KMLUtil.writeLookAt(w, coord.latitude, coord.longitude, height, 500.6566641072245, 40.5575073395506, -148.4122922628044);
    KMLUtil.writeWithCharacters(w, "styleUrl", styleUrl);
    w.writeStartElement("Point");
    KMLUtil.writeWithCharacters(w, "altitudeMode", "relativeToGround");
    KMLUtil.writeWithCharacters(w, "coordinates", String.format("%f,%f,%f", coord.longitude, coord.latitude, height));
    w.writeEndElement(); // Point
    w.writeEndElement(); // Placemark
  }

    @Deprecated
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
              + "<Style id=\"downArrowIcon\">\n"
              + "<IconStyle>\n"
              + "<Icon>\n"
              + "<href>http://maps.google.com/mapfiles/kml/pal4/icon28.png</href>\n"
              + "</Icon>\n"
              + "</IconStyle>\n"
              + "</Style>\n"
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

    @Deprecated
    public static String genKMLFooter() {
        return "\t\t</Folder>\n\t</Document>\n</kml>";
    }

    @Deprecated
    public static String genPolygon(String placename, Coordinate coord,
          double radius, String kmlPolygon) {
        return "\t\t\t<Placemark>\n"
              + "\t\t\t\t<name>" + placename + "</name>\n"
              + "\t\t\t\t<Region>\n"
              + "\t\t\t\t\t<LatLonAltBox>\n"
              + "\t\t\t\t\t\t<north>" + (coord.latitude + radius) + "</north>\n"
              + "\t\t\t\t\t\t<south>" + (coord.latitude - radius) + "</south>\n"
              + "\t\t\t\t\t\t<east>" + (coord.longitude + radius) + "</east>\n"
              + "\t\t\t\t\t\t<west>" + (coord.longitude - radius) + "</west>\n"
              + "\t\t\t\t\t</LatLonAltBox>\n"
              + "\t\t\t\t\t<Lod>\n"
              + "\t\t\t\t\t\t<minLodPixels>" + KMLUtil.MIN_LOD_PIXELS + "</minLodPixels>\n"
              + "\t\t\t\t\t</Lod>\n"
              + "\t\t\t\t</Region>\n"
              + "\t\t\t\t<styleUrl>#bar</styleUrl>\n"
              + "\t\t\t\t<Point>\n"
              + "\t\t\t\t\t<coordinates>\n"
              + "\t\t\t\t\t\t" + String.format("%f,%f", coord.longitude, coord.latitude) + "\n"
              + "\t\t\t\t\t</coordinates>\n"
              + "\t\t\t\t</Point>\n"
              + "\t\t\t</Placemark>\n"
              + "\t\t\t<Placemark>\n"
              + "\t\t\t\t<name>" + placename + " POLYGON</name>\n"
              + "\t\t\t\t<styleUrl>#bar</styleUrl>\n"
              + "\t\t\t\t<Style><PolyStyle><color>dc0155ff</color></PolyStyle></Style>\n"
              + "\t\t\t\t<Polygon>\n"
              + "\t\t\t\t\t<extrude>1</extrude><tessellate>1</tessellate>\n"
              + "\t\t\t\t\t<altitudeMode>relativeToGround</altitudeMode>\n"
              + "\t\t\t\t\t<outerBoundaryIs>\n"
              + "\t\t\t\t\t\t<LinearRing>\n"
              + "\t\t\t\t\t\t\t" + kmlPolygon + "\n"
              + "\t\t\t\t\t\t</LinearRing>\n"
              + "\t\t\t\t\t</outerBoundaryIs>\n"
              + "\t\t\t\t</Polygon>\n"
              + "\t\t\t</Placemark>\n";
    }

    @Deprecated
    public static String genSpiralpoint(String placename, String context,
          Coordinate spiralPoint, int j, double radius) {
        return "\t\t\t<Placemark>\n"
              + "\t\t\t\t<name>" + placename + " #" + (j + 1) + "</name>\n"
              + "\t\t\t\t<description>" + context + "</description>\n"
              + "\t\t\t\t<Region>\n"
              + "\t\t\t\t\t<LatLonAltBox>\n"
              + "\t\t\t\t\t\t<north>" + (spiralPoint.latitude + radius) + "</north>\n"
              + "\t\t\t\t\t\t<south>" + (spiralPoint.latitude - radius) + "</south>\n"
              + "\t\t\t\t\t\t<east>" + (spiralPoint.longitude + radius) + "</east>\n"
              + "\t\t\t\t\t\t<west>" + (spiralPoint.longitude - radius) + "</west>\n"
              + "\t\t\t\t\t</LatLonAltBox>\n"
              + "\t\t\t\t\t<Lod>\n"
              + "\t\t\t\t\t\t<minLodPixels>" + KMLUtil.MIN_LOD_PIXELS + "</minLodPixels>\n"
              + "\t\t\t\t\t</Lod>\n"
              + "\t\t\t\t</Region>\n"
              + "\t\t\t\t<styleUrl>#context</styleUrl>\n"
              + "\t\t\t\t<Point>\n"
              + "\t\t\t\t\t<coordinates>" + String.format("%f,%f", spiralPoint.longitude, spiralPoint.latitude) + "</coordinates>\n"
              + "\t\t\t\t</Point>\n"
              + "\t\t\t</Placemark>\n";
    }

    @Deprecated
    public static String genFloatingPlacemark(String word, Coordinate coord,
          double height) {
        return "<Placemark>\n"
              + "<name>" + word + "</name>\n"
              + "<visibility>1</visibility>\n"
              + "<LookAt>\n"
              + "<longitude>" + coord.longitude + "</longitude>\n"
              + "<latitude>" + coord.latitude + "</latitude>\n"
              + "<altitude>" + height + "</altitude>\n"
              + "<heading>-148.4122922628044</heading>\n"
              + "<tilt>40.5575073395506</tilt>\n"
              + "<range>500.6566641072245</range>\n"
              + "</LookAt>\n"
              + "<styleUrl>#downArrowIcon</styleUrl>\n"
              + "<Point>\n"
              + "<altitudeMode>relativeToGround</altitudeMode>\n"
              + "<coordinates>" + String.format("%f,%f", coord.longitude, coord.latitude) + "," + height + "</coordinates>\n"
              + "</Point>\n"
              + "</Placemark>\n";
    }
}
