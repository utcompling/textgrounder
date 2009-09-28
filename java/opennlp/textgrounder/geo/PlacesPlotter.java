package opennlp.textgrounder.geo;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class PlacesPlotter { // main class
	
	public static final int BAR_SCALE = 5000;

	public static void main(String[] args) throws Exception {
		if(args.length < 2) {
			System.out.println("usage: java PlacesPlotter <input-text-filename> <gazetteer-filename> [output-filename]");
			System.exit(0);
		}
		
		Hashtable<String, Coordinate> placenamesToCoords = new Hashtable<String, Coordinate>();
		
		BufferedReader gazIn = new BufferedReader(new FileReader(args[1]));
		
		System.out.print("Populating gazetteer...");
		
		String curLine = gazIn.readLine(); // first line of gazetteer is legend
		while(true) {
			curLine = gazIn.readLine();
			if(curLine == null) break;
			//System.out.println(curLine);
			String[] tokens = curLine.split("\\|");
			/*for(String token : tokens) {
				System.out.println(token);
			}*/
			//System.out.println(tokens[9]);
			//System.out.println(tokens[10]);
			Coordinate curCoord = new Coordinate(Double.parseDouble(tokens[9]), Double.parseDouble(tokens[10]));
			addToHashtableIfAbsent(placenamesToCoords, tokens[1].toLowerCase(), curCoord);
			addToHashtableIfAbsent(placenamesToCoords, tokens[5].toLowerCase(), curCoord);
			addToHashtableIfAbsent(placenamesToCoords, tokens[16].toLowerCase(), curCoord);
		}
		
		System.out.println("done. Total number of actual place names = " + placenamesToCoords.size());
		/*System.out.println(placenamesToCoords.get("salt springs"));
		System.out.println(placenamesToCoords.get("galveston"));*/
		
		Hashtable<String, Integer> placenamesToCounts = new Hashtable<String, Integer>();
		
		BufferedReader textIn = new BufferedReader(new FileReader(args[0]));
		
		Pattern placeNamePattern = Pattern.compile("[A-Z][\\w]*([\\s][A-Z][\\w]*)*");
		
		System.out.print("Scanning raw text for place names...");
		
		//curLine = textIn.readLine();
		while(true) {
			//System.out.println(curLine);
			curLine = textIn.readLine();
			if(curLine == null) break;
			Matcher m = placeNamePattern.matcher(curLine);
			while(m != null && m.find()) {
				String potentialPlacename = m.group();
				if(placenamesToCoords.containsKey(potentialPlacename.toLowerCase())) {
					Integer curCount = placenamesToCounts.get(potentialPlacename.toLowerCase());
					if(curCount == null)
						placenamesToCounts.put(potentialPlacename.toLowerCase(), 1);
					else
						placenamesToCounts.put(potentialPlacename.toLowerCase(), curCount + 1);
					//System.out.println(potentialPlacename + ": " + placenamesToCoords.get(potentialPlacename.toLowerCase()));
				}
			}
		}
		
		System.out.println("done. Number of distinct place names found = " + placenamesToCounts.size());
		
		String outputFilename;
		if(args.length >= 3)
			outputFilename = args[2];
		else
			outputFilename = "output.kml";
		
		BufferedWriter out = new BufferedWriter(new FileWriter(outputFilename));
		
		System.out.print("Writing KML file " + outputFilename + "...");
		
		out.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<kml xmlns=\"http://www.opengis.net/kml/2.2\" xmlns:gx=\"http://www.google.com/kml/ext/2.2\" xmlns:kml=\"http://www.opengis.net/kml/2.2\" xmlns:atom=\"http://www.w3.org/2005/Atom\">\n\t<Document>\n\t\t<Style id=\"transBluePoly\">\n\t\t\t<LineStyle>\n\t\t\t\t<width>1.5</width>\n\t\t\t</LineStyle>\n\t\t\t<PolyStyle>\n\t\t\t\t<color>7dff0000</color>\n\t\t\t</PolyStyle>\n\t\t</Style>\n\t\t<Folder>\n\t\t\t<name>" + args[0] + "</name>\n\t\t\t<open>1</open>\n\t\t\t<description>Distribution of place names found in " + args[0] + "</description>\n\t\t\t<LookAt>\n\t\t\t\t<latitude>42</latitude>\n\t\t\t\t<longitude>-102</longitude>\n\t\t\t\t<altitude>0</altitude>\n\t\t\t\t<range>5000000</range>\n\t\t\t\t<tilt>53.454348562403</tilt>\n\t\t\t\t<heading>0</heading>\n\t\t\t</LookAt>\n");
		
		for(String placename : placenamesToCounts.keySet()) {
			Coordinate centerCoord = placenamesToCoords.get(placename);
			//System.out.println(placename + " " + centerCoord + ": " + placenamesToCounts.get(placename));
			Coordinate coord1 = new Coordinate(centerCoord.longitude - .25, centerCoord.latitude + .25);
			Coordinate coord2 = new Coordinate(centerCoord.longitude + .25, centerCoord.latitude + .25);
			Coordinate coord3 = new Coordinate(centerCoord.longitude + .25, centerCoord.latitude - .25);
			Coordinate coord4 = new Coordinate(centerCoord.longitude - .25, centerCoord.latitude - .25);
			int height = placenamesToCounts.get(placename) * BAR_SCALE;
			out.write("\t\t\t<Placemark>\n\t\t\t\t<name>" + placename + "</name>\n\t\t\t\t<styleUrl>#transBluePoly</styleUrl>\n\t\t\t\t<Polygon>\n\t\t\t\t\t<extrude>1</extrude>\n\t\t\t\t\t<altitudeMode>relativeToGround</altitudeMode>\n\t\t\t\t\t<outerBoundaryIs>\n\t\t\t\t\t\t<LinearRing>\n\t\t\t\t\t\t\t<coordinates>\n\t\t\t\t\t\t\t\t" + coord1 + "," + height + "\n\t\t\t\t\t\t\t\t" + coord2 + "," + height + "\n\t\t\t\t\t\t\t\t" + coord3 + "," + height + "\n\t\t\t\t\t\t\t\t" + coord3 + "," + height + "\n\t\t\t\t\t\t\t\t" + coord4 + "," + height + "\n\t\t\t\t\t\t\t\t" + coord1 + "," + height + "\n\t\t\t\t\t\t\t</coordinates>\n\t\t\t\t\t\t</LinearRing>\n\t\t\t\t\t</outerBoundaryIs>\n\t\t\t\t</Polygon>\n\t\t\t</Placemark>\n");
		}
		
		out.write("\t\t</Folder>\n\t</Document>\n</kml>");
		
		System.out.println("done.");
		
		textIn.close();
		gazIn.close();
		out.close();
		
	}

	private static void addToHashtableIfAbsent(Hashtable<String, Coordinate> hs, String s, Coordinate c) {
		if(!hs.containsKey(s))
			hs.put(s, c);
	}
	
	public static class Coordinate {
		public double longitude;
		public double latitude;
		
		public Coordinate(double lon, double lat) {
			longitude = lon;
			latitude = lat;
		}
		
		public String toString() {
			return latitude + "," + longitude;
		}
	}
}
