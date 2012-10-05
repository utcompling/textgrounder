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
package opennlp.textgrounder.tr.util;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

//import opennlp.textgrounder.old.topostructs.Coordinate;
import opennlp.textgrounder.tr.topo.*;
import java.text.*;

/**
 * Class of static methods for generating KML headers, footers and other stuff.
 *
 * @author tsmoon
 */
public class KMLUtil {
  // Minimum number of pixels the (small) square region (NOT our Region) represented by each city must occupy on the screen for its label to appear:
  private final static int MIN_LOD_PIXELS = 16;

    //public static double RADIUS = .15;
    //public static double SPIRAL_RADIUS = .2;
  public static double RADIUS = .05;
  public static double SPIRAL_RADIUS = .05;
  public static int SIDES = 10;
  public static int BARSCALE = 50000;

  private final static DecimalFormat df = new DecimalFormat("#.####");

  public static void writeWithCharacters(XMLStreamWriter w, String localName, String text)
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
    w.writeStartElement("LineStyle");
    KMLUtil.writeWithCharacters(w, "color", "ff0000ff");
    KMLUtil.writeWithCharacters(w, "width", "3");
    w.writeEndElement(); // LineStyle
    w.writeStartElement("IconStyle");
    w.writeEmptyElement("Icon");
    w.writeEndElement(); // IconStyle
    w.writeEndElement(); // Style

    w.writeStartElement("Style");
    w.writeAttribute("id", "whiteLine");
    w.writeStartElement("LineStyle");
    KMLUtil.writeWithCharacters(w, "color", "ffffffff");
    KMLUtil.writeWithCharacters(w, "width", "3");
    w.writeEndElement(); // LineStyle
    w.writeEndElement(); // Style

    w.writeStartElement("Style");
    w.writeAttribute("id", "redLine");
    w.writeStartElement("LineStyle");
    KMLUtil.writeWithCharacters(w, "color", "ff0000ff");
    KMLUtil.writeWithCharacters(w, "width", "3");
    w.writeEndElement(); // LineStyle
    w.writeEndElement(); // Style

    w.writeStartElement("Style");
    w.writeAttribute("id", "blue");
    w.writeStartElement("IconStyle");
    w.writeStartElement("Icon");
    KMLUtil.writeWithCharacters(w, "href", "http://maps.google.com/mapfiles/kml/paddle/blu-blank-lv.png");
    w.writeEndElement(); // Icon
    w.writeEndElement(); // IconStyle
    w.writeEndElement(); // Style

    w.writeStartElement("Style");
    w.writeAttribute("id", "yellow");
    w.writeStartElement("IconStyle");
    w.writeStartElement("Icon");
    KMLUtil.writeWithCharacters(w, "href", "http://maps.google.com/mapfiles/kml/paddle/ylw-blank-lv.png");
    w.writeEndElement(); // Icon
    w.writeEndElement(); // IconStyle
    w.writeEndElement(); // Style

    w.writeStartElement("Style");
    w.writeAttribute("id", "green");
    w.writeStartElement("IconStyle");
    w.writeStartElement("Icon");
    KMLUtil.writeWithCharacters(w, "href", "http://maps.google.com/mapfiles/kml/paddle/grn-blank-lv.png");
    w.writeEndElement(); // Icon
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

    w.writeStartElement("Style");
    w.writeAttribute("id", "context");
    w.writeStartElement("LabelStyle");
    KMLUtil.writeWithCharacters(w, "scale", "0");
    w.writeEndElement(); // LabelStyle
    w.writeEndElement(); // Style

    w.writeStartElement("Folder");
    KMLUtil.writeWithCharacters(w, "name", name);
    KMLUtil.writeWithCharacters(w, "open", "1");
    KMLUtil.writeWithCharacters(w, "description", "Distribution of place names found in " + name);
    KMLUtil.writeLookAt(w, 42, -102, 0, 5000000, 53.45434856, 0);
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
    //for (double currentRadian = startRadian; currentRadian <= startRadian + 2 * Math.PI; currentRadian += radianUnit) {
    for (double currentRadian = startRadian; currentRadian >= startRadian - 2 * Math.PI; currentRadian -= radianUnit) {
      double lat = coord.getLatDegrees() + radius * Math.cos(currentRadian);
      double lon = coord.getLngDegrees() + radius * Math.sin(currentRadian);
      w.writeCharacters(df.format(lon) + "," + df.format(lat) + "," + df.format(height) + "\n");
    }

    w.writeEndElement(); // coordinates
  }

  public static void writeRegion(XMLStreamWriter w, Coordinate coord, double radius)
    throws XMLStreamException {
    w.writeStartElement("Region");
    w.writeStartElement("LatLonAltBox");
    KMLUtil.writeWithCharacters(w, "north", df.format(coord.getLatDegrees() + radius));
    KMLUtil.writeWithCharacters(w, "south", df.format(coord.getLatDegrees() - radius));
    KMLUtil.writeWithCharacters(w, "east", df.format(coord.getLngDegrees() + radius));
    KMLUtil.writeWithCharacters(w, "west", df.format(coord.getLngDegrees() - radius));
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
    KMLUtil.writeWithCharacters(w, "latitude", "" + lat);
    KMLUtil.writeWithCharacters(w, "longitude", "" + lon);
    KMLUtil.writeWithCharacters(w, "altitude", "" + alt);
    KMLUtil.writeWithCharacters(w, "range", "" + range);
    KMLUtil.writeWithCharacters(w, "tilt", "" + tilt);
    KMLUtil.writeWithCharacters(w, "heading", "" + heading);
    w.writeEndElement(); // LookAt
  }

    public static void writePlacemark(XMLStreamWriter w, String name,
                                      Coordinate coord, double radius) throws XMLStreamException {
        w.writeStartElement("Placemark");
        KMLUtil.writeWithCharacters(w, "name", name);
        KMLUtil.writeRegion(w, coord, radius);
        KMLUtil.writeWithCharacters(w, "styleUrl", "#bar");
        w.writeStartElement("Point");
        KMLUtil.writeWithCharacters(w, "coordinates", df.format(coord.getLngDegrees()) + "," + df.format(coord.getLatDegrees()));
        w.writeEndElement(); // Point
        w.writeEndElement(); // Placemark
    }

    public static void writePinPlacemark(XMLStreamWriter w, String name,
                                         Coordinate coord) throws XMLStreamException {
        writePinPlacemark(w, name, coord, null);
    }

    public static void writePinTimeStampPlacemark(XMLStreamWriter w, String name,
                                                  Coordinate coord, String context,
                                                  int timeIndex) throws XMLStreamException {
        w.writeStartElement("Placemark");
        KMLUtil.writeWithCharacters(w, "name", name);
        KMLUtil.writeWithCharacters(w, "description", context);
        w.writeStartElement("TimeSpan");
        KMLUtil.writeWithCharacters(w, "begin", timeIndex + "");
        KMLUtil.writeWithCharacters(w, "end", (timeIndex + 5) + "");
        w.writeEndElement(); // TimeSpan
        w.writeStartElement("Point");
        KMLUtil.writeWithCharacters(w, "coordinates", df.format(coord.getLngDegrees()) + "," + df.format(coord.getLatDegrees()));
        w.writeEndElement(); // Point
        w.writeEndElement(); // Placemark
    }

    public static void writePinPlacemark(XMLStreamWriter w, String name,
                                         Coordinate coord, String styleUrl) throws XMLStreamException {
        w.writeStartElement("Placemark");
        //KMLUtil.writeWithCharacters(w, "name", name);
        //KMLUtil.writeRegion(w, coord, radius);
        if(styleUrl != null && styleUrl.length() > 0)
            KMLUtil.writeWithCharacters(w, "styleUrl", "#"+styleUrl);
        w.writeStartElement("Point");
        KMLUtil.writeWithCharacters(w, "coordinates", df.format(coord.getLngDegrees()) + "," + df.format(coord.getLatDegrees()));
        w.writeEndElement(); // Point
        w.writeEndElement(); // Placemark
    }

    public static void writeLinePlacemark(XMLStreamWriter w, Coordinate coord1, Coordinate coord2)
                                          throws XMLStreamException {
        writeLinePlacemark(w, coord1, coord2, "whiteLine");
    }

    public static void writeLinePlacemark(XMLStreamWriter w, Coordinate coord1, Coordinate coord2, String styleUrl)
                                          throws XMLStreamException {
        w.writeStartElement("Placemark");
        KMLUtil.writeWithCharacters(w, "styleUrl", "#"+styleUrl);
        w.writeStartElement("LineString");
        KMLUtil.writeWithCharacters(w, "gx:altitudeOffset", "0");
        KMLUtil.writeWithCharacters(w, "extrude", "1");
        KMLUtil.writeWithCharacters(w, "tessellate", "1");
        KMLUtil.writeWithCharacters(w, "altitudeMode", "clampToGround");
        KMLUtil.writeWithCharacters(w, "gx:drawOrder", "0");
        w.writeStartElement("coordinates");
        w.writeCharacters(df.format(coord1.getLngDegrees())+","+df.format(coord1.getLatDegrees())+",0\n");
        w.writeCharacters(df.format(coord2.getLngDegrees())+","+df.format(coord2.getLatDegrees())+",0\n");
        w.writeEndElement(); // coordinates
        w.writeEndElement(); // LineString
        w.writeEndElement(); // Placemark
    }

    public static void writeArcLinePlacemark(XMLStreamWriter w, Coordinate coord1, Coordinate coord2)
                                             throws XMLStreamException {
        w.writeStartElement("Placemark");
        KMLUtil.writeWithCharacters(w, "styleUrl", "#bar");
        w.writeStartElement("LineString");
        KMLUtil.writeWithCharacters(w, "gx:altitudeOffset", "0");
        KMLUtil.writeWithCharacters(w, "extrude", "0");
        KMLUtil.writeWithCharacters(w, "tessellate", "1");
        KMLUtil.writeWithCharacters(w, "altitudeMode", "relativeToGround");
        KMLUtil.writeWithCharacters(w, "gx:drawOrder", "0");
        w.writeStartElement("coordinates");
        double dist = coord1.distanceInKm(coord2);
        double lngDiff = coord2.getLngDegrees() - coord1.getLngDegrees();
        double latDiff = coord2.getLatDegrees() - coord1.getLatDegrees();
        w.writeCharacters(df.format(coord1.getLngDegrees())+","+df.format(coord1.getLatDegrees())+",0\n");
        for(double i = .1; i <= .9; i += .1) {
            w.writeCharacters(df.format(coord1.getLngDegrees()+lngDiff*i)+","
                              +df.format(coord1.getLatDegrees()+latDiff*i)+","
                              +(dist*dist)*.05/**(.5-Math.abs(.5-i))*/+"\n");
        }
        //w.writeCharacters(df.format(coord1.getLngDegrees()+lngDiff*.9)+","
        //                  +df.format(coord1.getLatDegrees()+latDiff*.9)+","+(dist*20)+"\n");
        w.writeCharacters(df.format(coord2.getLngDegrees())+","+df.format(coord2.getLatDegrees())+",0");
        w.writeEndElement(); // coordinates
        w.writeEndElement(); // LineString
        w.writeEndElement(); // Placemark
    }

  public static void writePolygon(XMLStreamWriter w, String name,
                                  Coordinate coord, int sides, double radius, double height)
    throws XMLStreamException {
    /*w.writeStartElement("Placemark");
    KMLUtil.writeWithCharacters(w, "name", name);
    KMLUtil.writeRegion(w, coord, radius);
    KMLUtil.writeWithCharacters(w, "styleUrl", "#bar");
    w.writeStartElement("Point");
    KMLUtil.writeWithCharacters(w, "coordinates", df.format(coord.getLngDegrees()) + "," + df.format(coord.getLatDegrees()));
    w.writeEndElement(); // Point
    w.writeEndElement(); // Placemark*/

    writePlacemark(w, name, coord, radius);

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
    KMLUtil.writeWithCharacters(w, "coordinates", df.format(coord.getLngDegrees()) + "," + df.format(coord.getLatDegrees()));
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
    KMLUtil.writeLookAt(w, coord.getLatDegrees(), coord.getLngDegrees(), height, 500.656, 40.557, -148.412);
    KMLUtil.writeWithCharacters(w, "styleUrl", styleUrl);
    w.writeStartElement("Point");
    KMLUtil.writeWithCharacters(w, "altitudeMode", "relativeToGround");
    KMLUtil.writeWithCharacters(w, "coordinates",
            df.format(coord.getLngDegrees()) + "," + df.format(coord.getLatDegrees()) + "," + df.format(height));
    w.writeEndElement(); // Point
    w.writeEndElement(); // Placemark
  }
}
