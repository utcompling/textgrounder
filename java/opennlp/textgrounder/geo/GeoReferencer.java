package opennlp.textgrounder.geo;

import java.io.*;
import java.util.*;

import gnu.trove.*;

import edu.stanford.nlp.ie.crf.*;

import org.apache.commons.cli.*;

import opennlp.textgrounder.gazetteers.*;
import opennlp.textgrounder.topostructs.*;
import opennlp.textgrounder.util.*;
import opennlp.textgrounder.ners.*;

public class GeoReferencer {

    private int barScale = 50000;
    private Gazetteer gazetteer;
    private Hashtable<String, List<Location>> gazCache = new Hashtable<String, List<Location>>();
    private double degreesPerRegion = 3.0;
    private Region[][] regionArray;
    private int regionArrayWidth, regionArrayHeight, activeRegions;
    private CRFClassifier classifier;
    private DocumentSet docSet;
    private boolean initializedXMLFile = false;
    private boolean finalizedXMLFile = false;

    public GeoReferencer(Gazetteer gaz, int bscale, CRFClassifier classif,
          int paragraphsAsDocs) {
        barScale = bscale;
        gazetteer = gaz;
        classifier = classif;
        docSet = new DocumentSet(paragraphsAsDocs);
    }

    private String stripPunc(String aString) {
        while (aString.length() > 0 && !Character.isLetterOrDigit(aString.charAt(0))) {
            aString = aString.substring(1);
        }
        while (aString.length() > 0 && !Character.isLetterOrDigit(aString.charAt(aString.length() - 1))) {
            aString = aString.substring(0, aString.length() - 1);
        }
        return aString;
    }

    public String getPlacenameString(ToponymSpan curTopSpan, int docIndex) {
        String toReturn = "";
        ArrayList<Integer> curDoc = docSet.get(docIndex);

        for (int i = curTopSpan.begin; i < curTopSpan.end; i++) {
            toReturn += docSet.getWordForInt(curDoc.get(i)) + " ";
        }

        return stripPunc(toReturn.trim());
    }

    private void initializeRegionArray() {
        activeRegions = 0;

        regionArrayWidth = 360 / (int) degreesPerRegion;
        regionArrayHeight = 180 / (int) degreesPerRegion;

        regionArray = new Region[regionArrayWidth][regionArrayHeight];
        for (int w = 0; w < regionArrayWidth; w++) {
            for (int h = 0; h < regionArrayHeight; h++) {
                regionArray[w][h] = null;
            }
        }
    }

    private void addLocationsToRegionArray(List<Location> locs) {
        for (Location loc : locs) {
            int curX = (int) (loc.coord.latitude + 180) / (int) degreesPerRegion;
            int curY = (int) (loc.coord.longitude + 90) / (int) degreesPerRegion;
            if (regionArray[curX][curY] == null) {
                double minLon = loc.coord.longitude - loc.coord.longitude % degreesPerRegion;
                double maxLon = minLon + degreesPerRegion;
                double minLat = loc.coord.latitude - loc.coord.latitude % degreesPerRegion;
                double maxLat = minLat + degreesPerRegion;
                regionArray[curX][curY] = new Region(minLon, maxLon, minLat, maxLat);
                activeRegions++;
            }
        }
    }

    private void printRegionArray() {
        System.out.println();

        for (int h = 0; h < regionArrayHeight; h++) {
            for (int w = 0; w < regionArrayWidth; w++) {
                if (regionArray[w][h] == null) {
                    System.out.print("0");
                } else {
                    System.out.print("1");
                }
            }
            System.out.println();
        }

        System.out.println(activeRegions + " active regions for this document out of a possible "
              + (regionArrayHeight * regionArrayWidth) + " (region size = "
              + degreesPerRegion + " x " + degreesPerRegion + " degrees).");
    }

    private List<Location> disambiguateAndCountPlacenames(
          SNERPairListSet pairListSet) throws Exception {

        System.out.println("Disambiguating place names found...");

        ArrayList<Location> locs = new ArrayList<Location>();
        //TIntHashSet locationsFound = new TIntHashSet();

        TIntIntHashMap idsToCounts = new TIntIntHashMap();

        /*	TObjectIntIterator<String> placeIterator = placeCounts.iterator();
        for (int i = placeCounts.size(); i-- > 0;) {
        placeIterator.advance();*/
        assert (pairListSet.size() == docSet.size());
        for (int docIndex = 0; docIndex < docSet.size(); docIndex++) {
            ArrayList<ToponymSpan> curDocSpans = pairListSet.get(docIndex);
            ArrayList<Integer> curDoc = docSet.get(docIndex);

            for (int topSpanIndex = 0; topSpanIndex < curDocSpans.size();
                  topSpanIndex++) {
                System.out.println("topSpanIndex: " + topSpanIndex);
                ToponymSpan curTopSpan = curDocSpans.get(topSpanIndex);

                String placename = getPlacenameString(curTopSpan, docIndex).toLowerCase();
                System.out.println(placename);

                if (!gazetteer.contains(placename)) // quick lookup to see if it has even 1 place by that name
                {
                    continue;
                }

                // try the cache first. if not in there, do a full DB lookup and add that pair to the cache:
                List<Location> possibleLocations = gazCache.get(placename);
                if (possibleLocations == null) {
                    possibleLocations = gazetteer.get(placename);
                    gazCache.put(placename, possibleLocations);
                    addLocationsToRegionArray(possibleLocations);
                }

                Location curLocation = popBaselineDisambiguate(possibleLocations);
                if (curLocation == null) {
                    continue;
                }

                // instantiate the region containing curLocation

                /*if(!locationsFound.contains(curLocation.id)) {
                locs.add(curLocation);
                locationsFound.add(curLocation.id);
                }*/

                int curCount = idsToCounts.get(curLocation.id);
                if (curCount == 0) {// sentinel for not found in hashmap
                    locs.add(curLocation);
                    idsToCounts.put(curLocation.id, 1);
                    System.out.println("Found first " + curLocation.name + "; id = " + curLocation.id);
                } else {
                    idsToCounts.increment(curLocation.id);
                    System.out.println("Found " + curLocation.name + " #" + idsToCounts.get(curLocation.id));
                }
            }



            //String placename = getPlacenameStrin(;
            //int count = placeIterator.value();
            //double height = Math.log(placeIterator.value()) * barScale;

            /*List<Location> possibleLocations = gazetteer.get(placename);
            Location curLocation = popBaselineDisambiguate(possibleLocations);
            if(curLocation == null) continue;
            curLocation.count = count;
            locs.add(curLocation);*/


            /*Coordinate coord;
            if(gazetteer instanceof WGGazetteer)
            coord = ((WGGazetteer)gazetteer).baselineGet(placename);
            else
            coord = gazetteer.get(placename);

            if(coord.longitude == 9999.99) // sentinel
            continue;*/
        }

        for (int i = 0; i < locs.size(); i++) // set all the counts properly
        {
            locs.get(i).count = idsToCounts.get(locs.get(i).id);
        }

        System.out.println("Done.");

        return locs;
    }

    private Location popBaselineDisambiguate(List<Location> possibleLocations)
          throws Exception {
        int maxPointPop = -1;
        Location pointToReturn = null;
        int maxRegionPop = -1;
        Location maxRegion = null;

        // establish the biggest region by this name:
        for (Location loc : possibleLocations) {
            if (!loc.type.equals("locality")) {
                if (loc.pop > maxRegionPop) {
                    maxRegion = loc;
                    maxRegionPop = loc.pop;
                }
            }
        }

        // do the disambiguation:
        for (Location loc : possibleLocations) {
            if (loc.type.equals("locality")) {
                if (loc.pop > maxPointPop && (maxRegion == null || loc.pop > maxRegionPop || loc.container.equals(maxRegion.name))) {
                    pointToReturn = loc;
                    maxPointPop = loc.pop;
                }
            }
        }

        return pointToReturn;
    }

    public void writeXMLFile(List<Location> locs, String outputFile,
          String inputFilename) throws Exception {

        BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));

        out.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<kml xmlns=\"http://www.opengis.net/kml/2.2\" xmlns:gx=\"http://www.google.com/kml/ext/2.2\" xmlns:kml=\"http://www.opengis.net/kml/2.2\" xmlns:atom=\"http://www.w3.org/2005/Atom\">\n\t<Document>\n\t\t<Style id=\"transBluePoly\">\n\t\t\t<PolyStyle>\n\t\t\t\t<outline>0</outline>\n\t\t\t</PolyStyle>\n\t\t\t<IconStyle>\n\t\t\t\t<Icon></Icon>\n\t\t\t</IconStyle>\n\t\t</Style>\n\t\t<Folder>\n\t\t\t<name>" + inputFilename + "</name>\n\t\t\t<open>1</open>\n\t\t\t<description>Distribution of place names found in " + inputFilename + "</description>\n\t\t\t<LookAt>\n\t\t\t\t<latitude>42</latitude>\n\t\t\t\t<longitude>-102</longitude>\n\t\t\t\t<altitude>0</altitude>\n\t\t\t\t<range>5000000</range>\n\t\t\t\t<tilt>53.454348562403</tilt>\n\t\t\t\t<heading>0</heading>\n\t\t\t</LookAt>\n");

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

        for (Location loc : locs) {

            double height = Math.log(loc.count) * barScale;

            //String kmlPolygon = coord.toKMLPolygon(4,.15,height);  // a square
            String kmlPolygon = loc.coord.toKMLPolygon(10, .15, height);

            String placename = loc.name;
            Coordinate coord = loc.coord;
            out.write("\t\t\t<Placemark>\n\t\t\t\t<name>" + placename + "</name>\n\t\t\t\t<styleUrl>#transBluePoly</styleUrl>\n\t\t\t\t<Point>\n\t\t\t\t\t<coordinates>\n\t\t\t\t\t\t" + coord + "\n\t\t\t\t\t</coordinates>\n\t\t\t\t</Point>\n\t\t\t</Placemark>\n\t\t\t<Placemark>\n\t\t\t\t<name>" + placename + " POLYGON</name>\n\t\t\t\t<styleUrl>#transBluePoly</styleUrl>\n\t\t\t\t<Style><PolyStyle><color>dc0155ff</color></PolyStyle></Style>\n\t\t\t\t<Polygon>\n\t\t\t\t\t<extrude>1</extrude><tessellate>1</tessellate>\n\t\t\t\t\t<altitudeMode>relativeToGround</altitudeMode>\n\t\t\t\t\t<outerBoundaryIs>\n\t\t\t\t\t\t<LinearRing>\n\t\t\t\t\t\t\t" + kmlPolygon + "\n\t\t\t\t\t\t</LinearRing>\n\t\t\t\t\t</outerBoundaryIs>\n\t\t\t\t</Polygon>\n\t\t\t</Placemark>\n");
        }

        out.write("\t\t</Folder>\n\t</Document>\n</kml>");

        out.close();
    }

    public void processPath(File myPath, SNERPairListSet pairListSet) throws
          Exception {
        if (myPath.isDirectory()) {
            for (String pathname : myPath.list()) {
                processPath(new File(myPath.getCanonicalPath() + File.separator + pathname), pairListSet);
            }
        } else {
            docSet.addDocumentFromFile(myPath.getCanonicalPath());
            pairListSet.addToponymSpansFromFile(myPath.getCanonicalPath());
        }
    }

    public static void main(String[] args) throws Exception {
        /*if(args.length < 2) {
        System.out.println("usage: java opennlp.textgrounder.geo.GeoReferencer <input-text-filename-or-dirname> <gazetteer-type{g,c,w}> [output-filename]");
        System.exit(0);
        }*/

        int gazType;
        Gazetteer myGaz = null;
        File argFile = null;
        String outputFilename = null;

        double dpr = 3.0; // degrees per region

        CommandLineParser optparse = new PosixParser();

        Options options = new Options();
        options.addOption("g", "gazetteer", true, "gazetteer to use [world, census, NGA, USGS; default = world]");
        options.addOption("m", "model", true, "model [default = PopBaseline]"); // nothing is done with this yet
        options.addOption("i", "input", true, "input file or directory name");
        options.addOption("o", "output", true, "output filename [default = 'output.kml']");
        options.addOption("d", "degrees-per-region", true, "size of Region squares in degrees [default = 3.0]");
        options.addOption("p", "paragraphs-as-docs", true, "number of paragraphs to treat as a document. Set the argument to 0 if documents should be treated whole");
        options.addOption("h", "help", false, "print help");

        int paragraphsAsDocs = 0;
        try {
            CommandLine cline = optparse.parse(options, args);

            if (cline.hasOption('h')) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("java GeoReferencer", options);
                System.exit(0);
            }

            if (!cline.hasOption('i')) {
                System.out.println("Error: You must specify an input filename with the -i flag.");
                System.exit(0);
            }
            argFile = new File(cline.getOptionValue('i'));

            if (cline.hasOption('g')) {
                String gazTypeArg = cline.getOptionValue('g').toLowerCase();
                if (gazTypeArg.startsWith("c")) {
                    myGaz = new CensusGazetteer();
                } else if (gazTypeArg.startsWith("n")) {
                    myGaz = new NGAGazetteer();
                } else if (gazTypeArg.startsWith("u")) {
                    myGaz = new USGSGazetteer();
                } else if (gazTypeArg.startsWith("w")) {
                    myGaz = new WGGazetteer();
		} else if (gazTypeArg.startsWith("t")) {
		    myGaz = new TRGazetteer();
                } else {
                    System.err.println("Error: unrecognized gazetteer type: " + gazTypeArg);
                    System.err.println("Please enter w, c, u, or g.");
                    System.exit(0);
                    //myGaz = new WGGazetteer();
                }
            } else {
                myGaz = new WGGazetteer();
            }

            if (cline.hasOption('p')) {
                paragraphsAsDocs = Integer.parseInt(cline.getOptionValue('p'));
            }

            if (cline.hasOption('o')) {
                outputFilename = cline.getOptionValue('o');
            } else {
                outputFilename = "output.kml";
            }

            if (cline.hasOption("d")) {
                dpr = Double.parseDouble(cline.getOptionValue("d"));
            }

        } catch (ParseException exp) {
            System.out.println("Unexpected exception parsing command line options: " + exp.getMessage());
            //} catch(IOException exp) {
            //System.out.println("IOException: " + exp.getMessage());
            System.exit(0);
        }

        //File argFile = new File(args[0]);
        if (!argFile.exists()) {
            System.out.println("Error: could not find input file or directory at " + args[0]);
            System.exit(0);
        }

        /*String outputFilename;
        if(args.length >= 3)
        outputFilename = args[2];
        else
        outputFilename = "output.kml";*/


        Properties myClassifierProperties = new Properties();
        CRFClassifier myClassifier = new CRFClassifier(myClassifierProperties);
        myClassifier.loadClassifier(Constants.STANFORD_NER_HOME + "/classifiers/ner-eng-ie.crf-3-all2008-distsim.ser.gz");

        GeoReferencer grefUS = new GeoReferencer(myGaz, 50000, myClassifier, paragraphsAsDocs);
        grefUS.degreesPerRegion = dpr;

        System.out.println("Writing KML file " + outputFilename + " ...");
        //SNERPlaceCounter placeCounts = new SNERPlaceCounter(myGaz, myClassifier);
        SNERPairListSet pairListSet = new SNERPairListSet(myClassifier);
        System.out.println(argFile.getCanonicalPath());
        grefUS.processPath(argFile, pairListSet);
        grefUS.initializeRegionArray();
        List<Location> locs = grefUS.disambiguateAndCountPlacenames(pairListSet);
        grefUS.printRegionArray();
        grefUS.writeXMLFile(locs, outputFilename, args[0]);
    }
}
