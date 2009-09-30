package opennlp.textgrounder.geo;

import java.io.*;
import java.util.*;

public class PlacesPlotter { // main class
	
    public static final int BAR_SCALE = 5000;
    
    public static void main(String[] args) throws Exception {
	if(args.length < 2) {
	    System.out.println("usage: java PlacesPlotter <input-text-filename> <gazetteer-filename> [output-filename]");
	    System.exit(0);
	}

	Gazetteer gazUSPopPlaces = new Gazetteer(args[1]);

	PlaceCounter placeCounts = new PlaceCounter(args[0], gazUSPopPlaces);
		
	String outputFilename;
	if(args.length >= 3)
	    outputFilename = args[2];
	else
	    outputFilename = "output.kml";
		
	BufferedWriter out = new BufferedWriter(new FileWriter(outputFilename));
		
	System.out.print("Writing KML file " + outputFilename + "...");
		
	out.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<kml xmlns=\"http://www.opengis.net/kml/2.2\" xmlns:gx=\"http://www.google.com/kml/ext/2.2\" xmlns:kml=\"http://www.opengis.net/kml/2.2\" xmlns:atom=\"http://www.w3.org/2005/Atom\">\n\t<Document>\n\t\t<Style id=\"transBluePoly\">\n\t\t\t<LineStyle>\n\t\t\t\t<width>1.5</width>\n\t\t\t</LineStyle>\n\t\t\t<PolyStyle>\n\t\t\t\t<color>7dff0000</color>\n\t\t\t</PolyStyle>\n\t\t</Style>\n\t\t<Folder>\n\t\t\t<name>" + args[0] + "</name>\n\t\t\t<open>1</open>\n\t\t\t<description>Distribution of place names found in " + args[0] + "</description>\n\t\t\t<LookAt>\n\t\t\t\t<latitude>42</latitude>\n\t\t\t\t<longitude>-102</longitude>\n\t\t\t\t<altitude>0</altitude>\n\t\t\t\t<range>5000000</range>\n\t\t\t\t<tilt>53.454348562403</tilt>\n\t\t\t\t<heading>0</heading>\n\t\t\t</LookAt>\n");
		
	for(String placename : placeCounts.keySet()) {
	    Coordinate centerCoord = gazUSPopPlaces.get(placename);
	    //System.out.println(placename + " " + centerCoord + ": " + placeCounts.get(placename));
	    Coordinate coord1 = new Coordinate(centerCoord.longitude - .25, centerCoord.latitude + .25);
	    Coordinate coord2 = new Coordinate(centerCoord.longitude + .25, centerCoord.latitude + .25);
	    Coordinate coord3 = new Coordinate(centerCoord.longitude + .25, centerCoord.latitude - .25);
	    Coordinate coord4 = new Coordinate(centerCoord.longitude - .25, centerCoord.latitude - .25);
	    int height = placeCounts.get(placename) * BAR_SCALE;
	    out.write("\t\t\t<Placemark>\n\t\t\t\t<name>" + placename + "</name>\n\t\t\t\t<styleUrl>#transBluePoly</styleUrl>\n\t\t\t\t<Polygon>\n\t\t\t\t\t<extrude>1</extrude>\n\t\t\t\t\t<altitudeMode>relativeToGround</altitudeMode>\n\t\t\t\t\t<outerBoundaryIs>\n\t\t\t\t\t\t<LinearRing>\n\t\t\t\t\t\t\t<coordinates>\n\t\t\t\t\t\t\t\t" + coord1 + "," + height + "\n\t\t\t\t\t\t\t\t" + coord2 + "," + height + "\n\t\t\t\t\t\t\t\t" + coord3 + "," + height + "\n\t\t\t\t\t\t\t\t" + coord3 + "," + height + "\n\t\t\t\t\t\t\t\t" + coord4 + "," + height + "\n\t\t\t\t\t\t\t\t" + coord1 + "," + height + "\n\t\t\t\t\t\t\t</coordinates>\n\t\t\t\t\t\t</LinearRing>\n\t\t\t\t\t</outerBoundaryIs>\n\t\t\t\t</Polygon>\n\t\t\t</Placemark>\n");
	}
		
	out.write("\t\t</Folder>\n\t</Document>\n</kml>");
	out.close();
		
	System.out.println("done.");
		
    }

}
