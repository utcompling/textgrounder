package opennlp.textgrounder.models;

import java.io.IOException;
import opennlp.textgrounder.textstructs.TokenArrayBuffer;
import opennlp.textgrounder.textstructs.TextProcessor;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import opennlp.textgrounder.gazetteers.*;
import opennlp.textgrounder.textstructs.*;
import opennlp.textgrounder.models.callbacks.*;
import opennlp.textgrounder.ners.*;
import opennlp.textgrounder.topostructs.*;

/**
 * Base abstract class for all training models. Defines some methods for
 * interfacing with gazetteers. Declares some fields that deal with regions
 * and placenames.
 * 
 * @author 
 */
public abstract class Model {

    // Minimum number of pixels the (small) square region (NOT our Region) represented by each city must occupy on the screen for its label to appear:
    private final static int MIN_LOD_PIXELS = 16;
    /**
     * Number of paragraphs to consider as one document.
     */
    protected int paragraphsAsDocs;
    /**
     * Dimensions of regionArray.
     */
    protected int regionArrayWidth, regionArrayHeight;
    /**
     * Array of locations that have been identified in training data
     */
    protected List<Location> locations;
    /**
     * Training data
     */
    protected String inputPath;
    /**
     * Name of kml file (i.e. Google Earth format xml) to generate output to
     */
    protected String kmlOutputFilename;
    /**
     * Height of bars for Google Earth KML output
     */
    protected int barScale = 50000;
    /**
     * Gazetteer that holds geographic information
     */
    protected Gazetteer gazetteer;
    /**
     * Array of array of toponym indices by document. This includes only the
     * toponyms and none of the non-toponyms.
     */
    protected TextProcessor textProcessor;
    /**
     * Quick lookup table for gazetteer info based on toponym
     */
    protected Hashtable<String, List<Location>> gazCache;
    /**
     * Size of regions on cartesian reduction of globe. Globe is defined to be
     * 360 degrees longitude and 180 degrees latitude
     */
    protected double degreesPerRegion;
    /**
     * Array of regions defined by dividing globe dimensions by degreesPerRegion.
     */
    protected Region[][] regionArray;
    /**
     * Number of regions which have been activated in regionArray
     */
    protected int activeRegions;
    /**
     * Collection of training data
     */
    protected Lexicon lexicon;
    /**
     * Flag that tells system to ignore the input file(s) and instead run on every locality in the gazetteer
     */
    protected boolean runWholeGazetteer = false;
    protected int windowSize;
    protected TokenArrayBuffer tokenArrayBuffer;
    //protected int indexInTAB = 0;

    /**
     * Remove punctuation from first and last characters of a string
     *
     * @param aString String to strip
     * @return Input stripped of punctuation
     */
    protected String stripPunc(String aString) {
        while (aString.length() > 0 && !Character.isLetterOrDigit(aString.charAt(0))) {
            aString = aString.substring(1);
        }
        while (aString.length() > 0 && !Character.isLetterOrDigit(aString.charAt(aString.length() - 1))) {
            aString = aString.substring(0, aString.length() - 1);
        }
        return aString;
    }

    /**
     * Add locations to the 2D regionArray. To be used by classes that
     * do not use a RegionMapperCallback.
     *
     * @param locs list of locations.
     */
    protected void addLocationsToRegionArray(List<Location> locs) {
        addLocationsToRegionArray(locs, new NullRegionMapperCallback());
    }

    /**
     * Add locations to 2D regionArray. To be used by classes and methods that
     * use a RegionMapperCallback
     *
     * @param locs list of locations
     * @param regionMapper callback class for handling mappings of locations
     * and regions
     */
    protected void addLocationsToRegionArray(List<Location> locs,
          RegionMapperCallback regionMapper) {
        for (Location loc : locs) {
	    /*if(loc.coord.latitude < -180.0 || loc.coord.longitude > 180.0
	       || loc.coord.longitude < -90.0 || loc.coord.longitude > 900) {
		// switched?
	       }*/
            int curX = (int) (loc.coord.latitude + 180) / (int) degreesPerRegion;
            int curY = (int) (loc.coord.longitude + 90) / (int) degreesPerRegion;
	    //System.out.println(loc.coord.latitude + ", " + loc.coord.longitude + " goes to");
	    //System.out.println(curX + " " + curY);
	    if(curX < 0 || curY < 0) {
		if(curX < 0) curX = 0;
		if(curY < 0) curY = 0;
		System.err.println("Warning: " + loc.name + " had invalid coordinates (" + loc.coord + "); mapping it to [" + curX + "][" + curY + "]");
	    }
            if (regionArray[curX][curY] == null) {
                double minLon = loc.coord.longitude - loc.coord.longitude % degreesPerRegion;
                double maxLon = minLon + degreesPerRegion;
                double minLat = loc.coord.latitude - loc.coord.latitude % degreesPerRegion;
                double maxLat = minLat + degreesPerRegion;
                regionArray[curX][curY] = new Region(minLon, maxLon, minLat, maxLat);
                activeRegions++;
            }
            regionMapper.addToPlace(loc, regionArray[curX][curY]);
        }
    }

    public void processPath(File myPath, TextProcessor textProcessor,
          TokenArrayBuffer tokenArrayBuffer, StopwordList stopwordList) throws IOException {
        if (myPath.isDirectory()) {
            for (String pathname : myPath.list()) {
                processPath(new File(myPath.getCanonicalPath() + File.separator + pathname), textProcessor, tokenArrayBuffer, stopwordList);
            }
        } else {
            textProcessor.addToponymsFromFile(myPath.getCanonicalPath(), tokenArrayBuffer, stopwordList);
        }
    }

    /**
     * Output tagged and disambiguated placenames to Google Earth kml file.
     * 
     * @throws Exception
     */
    public void writeXMLFile() throws Exception {
        if (!runWholeGazetteer) {
            writeXMLFile(inputPath, kmlOutputFilename, locations);
        } else {
            writeXMLFile("WHOLE_GAZETTEER", kmlOutputFilename, locations);
        }
    }

    /**
     * Output tagged and disambiguated placenames to Google Earth kml file.
     *
     * @param inputPath
     * @param kmlOutputFilename
     * @param locations
     * @throws Exception
     */
    public void writeXMLFile(String inputFilename, String outputFilename,
          List<Location> locations) throws Exception {

        BufferedWriter out = new BufferedWriter(new FileWriter(outputFilename));
        int dotKmlIndex = outputFilename.lastIndexOf(".kml");
        String contextFilename = outputFilename.substring(0, dotKmlIndex) + "-context.kml";
        BufferedWriter contextOut = new BufferedWriter(new FileWriter(contextFilename));

        out.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
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
              + "\t\t\t</LookAt>\n");

        contextOut.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
              + "<kml xmlns=\"http://www.opengis.net/kml/2.2\" xmlns:gx=\"http://www.google.com/kml/ext/2.2\" xmlns:kml=\"http://www.opengis.net/kml/2.2\" xmlns:atom=\"http://www.w3.org/2005/Atom\">\n"
              + "\t<Document>\n"
              + "\t\t<Style id=\"context\">\n"
              + "\t\t\t<LabelStyle>\n"
              + "\t\t\t\t<scale>0</scale>\n"
              + "\t\t\t</LabelStyle>\n"
              + "\t\t</Style>\n"
              + "\t\t<Folder>\n"
              + "\t\t\t<name>" + inputFilename + " CONTEXTS</name>\n"
              + "\t\t\t<open>1</open>\n"
              + "\t\t\t<description>Contexts of place names found in " + inputFilename + "</description>\n"
              + "\t\t\t<LookAt>\n"
              + "\t\t\t\t<latitude>42</latitude>\n"
              + "\t\t\t\t<longitude>-102</longitude>\n"
              + "\t\t\t\t<altitude>0</altitude>\n"
              + "\t\t\t\t<range>5000000</range>\n"
              + "\t\t\t\t<tilt>53.454348562403</tilt>\n"
              + "\t\t\t\t<heading>0</heading>\n"
              + "\t\t\t</LookAt>\n");

        /*TObjectIntIterator<String> placeIterator = placeCounts.iterator();
        for (int i = placeCounts.size(); i-- > 0;) {
        placeIterator.advance();
        String placename = placeIterator.key();
        double height = Math.log(placeIterator.value()) * barScale;

        Coordinate coord;
        if(gazetteer instanceof WGGazetteer)
        coord = ((WGGazetteer)gazetteer).baselineGet(placename);
        else
        coord = gazetteer.get(placename);

        if(coord.longitude == 9999.99) // sentinel
        continue;*/

        for (int i = 0; i < locations.size(); i++) {//Location loc : locations) {
            Location loc = locations.get(i);

            double height = Math.log(loc.count) * barScale;

            double radius = .15;
            //String kmlPolygon = coord.toKMLPolygon(4,radius,height);  // a square
            String kmlPolygon = loc.coord.toKMLPolygon(10, radius, height);

            String placename = loc.name;
            Coordinate coord = loc.coord;
            out.write("\t\t\t<Placemark>\n"
                  + "\t\t\t\t<name>" + placename + "</name>\n"
                  + "\t\t\t\t<Region>"
                  + "\t\t\t\t\t<LatLonAltBox>"
                  + "\t\t\t\t\t\t<north>" + (coord.longitude + radius) + "</north>"
                  + "\t\t\t\t\t\t<south>" + (coord.longitude - radius) + "</south>"
                  + "\t\t\t\t\t\t<east>" + (coord.latitude + radius) + "</east>"
                  + "\t\t\t\t\t\t<west>" + (coord.latitude - radius) + "</west>"
                  + "\t\t\t\t\t</LatLonAltBox>"
                  + "\t\t\t\t\t<Lod>"
                  + "\t\t\t\t\t\t<minLodPixels>" + MIN_LOD_PIXELS + "</minLodPixels>"
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
                  + "\t\t\t</Placemark>\n");

            //System.out.println("Contexts for " + placename);
	    /*while(indexInTAB < tokenArrayBuffer.toponymVector.size() && tokenArrayBuffer.toponymVector.get(indexInTAB) == 0) {
            indexInTAB++;
            }*/
            for (int j = 0; j < loc.backPointers.size(); j++) {
                int index = loc.backPointers.get(j);
                String context = tokenArrayBuffer.getContextAround(index, windowSize, true);
                Coordinate spiralPoint = coord.getNthSpiralPoint(j, 0.1);

                contextOut.write("\t\t\t<Placemark>\n"
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
                      + "\t\t\t\t\t\t<minLodPixels>" + MIN_LOD_PIXELS + "</minLodPixels>"
                      + "\t\t\t\t\t</Lod>"
                      + "\t\t\t\t</Region>"
                      + "\t\t\t\t<styleUrl>#context</styleUrl>\n"
                      + "\t\t\t\t<Point>\n"
                      + "\t\t\t\t\t<coordinates>" + spiralPoint + "</coordinates>\n"
                      + "\t\t\t\t</Point>\n"
                      + "\t\t\t</Placemark>\n");
            }
            //indexInTAB++;

            //if(i >= 10) System.exit(0);

        }

        out.write("\t\t</Folder>\n\t</Document>\n</kml>");
        contextOut.write("\t\t</Folder>\n\t</Document>\n</kml>");

        out.close();
        contextOut.close();
    }

    /**
     * Train model. For access from main routines.
     */
    public abstract void train();
}
