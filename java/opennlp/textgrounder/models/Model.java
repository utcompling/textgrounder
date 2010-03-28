package opennlp.textgrounder.models;

import opennlp.textgrounder.models.callbacks.RegionMapperCallback;
import opennlp.textgrounder.models.callbacks.NullRegionMapperCallback;
import opennlp.textgrounder.io.DocumentSet;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;

import java.util.Hashtable;
import java.util.List;

import opennlp.textgrounder.gazetteers.*;
import opennlp.textgrounder.geo.*;
import opennlp.textgrounder.ners.SNERPairListSet;
import opennlp.textgrounder.ners.ToponymSpan;
import opennlp.textgrounder.topostructs.*;

/**
 * Base abstract class for all training models. Defines some methods for
 * interfacing with gazetteers. Declares some fields that deal with regions
 * and placenames.
 * 
 * @author 
 */
public abstract class Model {

    /**
     * Dimensions of regionArray.
     */
    protected int regionArrayWidth, regionArrayHeight;
    /**
     * Base class for Stanford NER system.
     */
    protected CRFClassifier classifier;
    /**
     * Array of locations that have been identified in training data
     */
    protected List<Location> locations;
    /**
     * Training data
     */
    protected String inputFilename;
    /**
     * Name of kml file (i.e. Google Earth format xml) to generate output to
     */
    protected String outputFilename;
    /**
     * Height of bars for Google Earth KML output
     */
    protected int barScale = 50000;
    /**
     * Gazetteer that holds geographic information
     */
    protected Gazetteer gazetteer;
    /**
     * Array of array of ToponymSpan objects that indicate locations of toponyms
     * in input
     */
    protected SNERPairListSet pairListSet;
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
    protected DocumentSet docSet;

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
     * String of placename given the offsets of the tokens in some document.
     * The placename may have two or more words.
     *
     * @param curTopSpan
     * @param docIndex
     * @return
     */
    protected String getPlacenameString(ToponymSpan curTopSpan, int docIndex) {
        String toReturn = "";
        ArrayList<Integer> curDoc = docSet.get(docIndex);

        for (int i = curTopSpan.begin; i < curTopSpan.end; i++) {
            toReturn += docSet.getWordForInt(curDoc.get(i)) + " ";
        }

        return stripPunc(toReturn.trim());
    }

    /**
     * 
     *
     * @param locs
     */
    protected void addLocationsToRegionArray(List<Location> locs) {
        addLocationsToRegionArray(locs, new NullRegionMapperCallback());
    }

    /**
     *
     *
     * @param locs
     */
    protected void addLocationsToRegionArray(List<Location> locs, RegionMapperCallback regionMapper) {
        for (Location loc : locs) {
            int curX = (int) (loc.coord.latitude + 180) / (int) degreesPerRegion;
            int curY = (int) (loc.coord.longitude + 90) / (int) degreesPerRegion;
            Region current = null;
            if (regionArray[curX][curY] == null) {
                double minLon = loc.coord.longitude - loc.coord.longitude % degreesPerRegion;
                double maxLon = minLon + degreesPerRegion;
                double minLat = loc.coord.latitude - loc.coord.latitude % degreesPerRegion;
                double maxLat = minLat + degreesPerRegion;
                current = new Region(minLon, maxLon, minLat, maxLat);
                regionMapper.addRegion(activeRegions, current);
                regionArray[curX][curY] = current;
                activeRegions++;
            }
        }
    }

    /**
     * Output tagged and disambiguated placenames to Google Earth kml file.
     * 
     * @throws Exception
     */
    public void writeXMLFile() throws Exception {
        writeXMLFile(inputFilename, outputFilename, locations);
    }

    /**
     * Output tagged and disambiguated placenames to Google Earth kml file.
     *
     * @param inputFilename
     * @param outputFilename
     * @param locations
     * @throws Exception
     */
    public void writeXMLFile(String inputFilename, String outputFilename,
          List<Location> locations) throws Exception {

        BufferedWriter out = new BufferedWriter(new FileWriter(outputFilename));

        out.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
              + "<kml xmlns=\"http://www.opengis.net/kml/2.2\" xmlns:gx=\"http://www.google.com/kml/ext/2.2\" xmlns:kml=\"http://www.opengis.net/kml/2.2\" xmlns:atom=\"http://www.w3.org/2005/Atom\">\n"
              + "\t<Document>\n"
              + "\t\t<Style id=\"transBluePoly\">\n"
              + "\t\t\t<PolyStyle>\n"
              + "\t\t\t\t<outline>0</outline>\n"
              + "\t\t\t</PolyStyle>\n"
              + "\t\t\t<IconStyle>\n"
              + "\t\t\t\t<Icon></Icon>\n"
              + "\t\t\t</IconStyle>\n\t\t</Style>\n"
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

        for (Location loc : locations) {

            double height = Math.log(loc.count) * barScale;

            //String kmlPolygon = coord.toKMLPolygon(4,.15,height);  // a square
            String kmlPolygon = loc.coord.toKMLPolygon(10, .15, height);

            String placename = loc.name;
            Coordinate coord = loc.coord;
            out.write("\t\t\t<Placemark>\n"
                  + "\t\t\t\t<name>" + placename + "</name>\n"
                  + "\t\t\t\t<styleUrl>#transBluePoly</styleUrl>\n"
                  + "\t\t\t\t<Point>\n"
                  + "\t\t\t\t\t<coordinates>\n"
                  + "\t\t\t\t\t\t" + coord + ""
                  + "\t\t\t\t\t</coordinates>"
                  + "\t\t\t\t</Point>"
                  + "\t\t\t</Placemark>"
                  + "\t\t\t<Placemark>"
                  + "\t\t\t\t<name>" + placename + " POLYGON</name>"
                  + "\t\t\t\t<styleUrl>#transBluePoly</styleUrl>"
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
        }

        out.write("\t\t</Folder>\n\t</Document>\n</kml>");

        out.close();
    }

    /**
     * Train model. For access from main routines.
     */
    public abstract void train();
}
