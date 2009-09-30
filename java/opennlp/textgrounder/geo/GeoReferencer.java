package opennlp.textgrounder.geo;

import java.io.*;
import java.util.*;

import gnu.trove.*;

public class GeoReferencer { // main class

    private int barScale = 50000;
    private Gazetteer gazetteer;

    public GeoReferencer(Gazetteer gaz, int bscale) {
	barScale = bscale;
	gazetteer = gaz;
    }

    public void plotFile(String inputFile, String outputFilename) 
	throws FileNotFoundException, IOException {

	PlaceCounter placeCounts = new PlaceCounter(inputFile, gazetteer);

	BufferedWriter out = new BufferedWriter(new FileWriter(outputFilename));
		
	System.out.print("Writing KML file " + outputFilename + "...");
		
	out.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<kml xmlns=\"http://www.opengis.net/kml/2.2\" xmlns:gx=\"http://www.google.com/kml/ext/2.2\" xmlns:kml=\"http://www.opengis.net/kml/2.2\" xmlns:atom=\"http://www.w3.org/2005/Atom\">\n\t<Document>\n\t\t<Style id=\"transBluePoly\">\n\t\t\t<PolyStyle>\n\t\t\t\t<outline>0</outline>\n\t\t\t</PolyStyle>\n\t\t</Style>\n\t\t<Folder>\n\t\t\t<name>" + inputFile + "</name>\n\t\t\t<open>1</open>\n\t\t\t<description>Distribution of place names found in " + inputFile + "</description>\n\t\t\t<LookAt>\n\t\t\t\t<latitude>42</latitude>\n\t\t\t\t<longitude>-102</longitude>\n\t\t\t\t<altitude>0</altitude>\n\t\t\t\t<range>5000000</range>\n\t\t\t\t<tilt>53.454348562403</tilt>\n\t\t\t\t<heading>0</heading>\n\t\t\t</LookAt>\n");
		
	TObjectIntIterator<String> placeIterator = placeCounts.iterator();
	for (int i = placeCounts.size(); i-- > 0;) {
	    placeIterator.advance();
	    String placename = placeIterator.key();
	    double height = Math.log(placeIterator.value()) * barScale;

	    Coordinate coord = gazetteer.get(placename);

	    //String kmlPolygon = coord.toKMLPolygon(4,.15,height);  // a square
	    String kmlPolygon = coord.toKMLPolygon(10,.15,height);

	    out.write("\t\t\t<Placemark>\n\t\t\t\t<name>" + placename + "</name>\n\t\t\t\t<styleUrl>#transBluePoly</styleUrl>\n\t\t\t\t<Style><PolyStyle><color>dc0155ff</color></PolyStyle></Style>\n\t\t\t\t<Polygon>\n\t\t\t\t\t<extrude>1</extrude><tessellate>1</tessellate>\n\t\t\t\t\t<altitudeMode>relativeToGround</altitudeMode>\n\t\t\t\t\t<outerBoundaryIs>\n\t\t\t\t\t\t<LinearRing>\n\t\t\t\t\t\t\t"+kmlPolygon+"\n\t\t\t\t\t\t</LinearRing>\n\t\t\t\t\t</outerBoundaryIs>\n\t\t\t\t</Polygon>\n\t\t\t</Placemark>\n");
	}
		
	out.write("\t\t</Folder>\n\t</Document>\n</kml>");
	out.close();
    }


    public static void main(String[] args) throws FileNotFoundException, IOException {
	if(args.length < 2) {
	    System.out.println("usage: java opennlp.textgrounder.geo.GeoReferencer <input-text-filename> <gazetteer-filename> [output-filename]");
	    System.exit(0);
	}

	GeoReferencer grefUS = new GeoReferencer(new Gazetteer(args[1]), 50000);
		
	String outputFilename;
	if(args.length >= 3)
	    outputFilename = args[2];
	else
	    outputFilename = "output.kml";

	grefUS.plotFile(args[0], outputFilename);
		
	System.out.println("done.");
    }

}
