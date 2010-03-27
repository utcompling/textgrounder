package opennlp.textgrounder.models;

import edu.stanford.nlp.ie.crf.CRFClassifier;

import gnu.trove.TIntIntHashMap;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import opennlp.textgrounder.gazetteers.*;
import opennlp.textgrounder.geo.*;
import opennlp.textgrounder.io.*;
import opennlp.textgrounder.ners.*;
import opennlp.textgrounder.topostructs.*;
import opennlp.textgrounder.util.Constants;

/**
 *
 * @author 
 */
public class BaselineModel extends Model {

    protected File inputFile;
    protected boolean initializedXMLFile = false;
    protected boolean finalizedXMLFile = false;

    public BaselineModel(Gazetteer gaz, int bscale, CRFClassifier classif,
          int paragraphsAsDocs) {
        barScale = bscale;
        gazetteer = gaz;
        classifier = classif;
        docSet = new DocumentSet(paragraphsAsDocs);
    }

    public BaselineModel(CommandLineOptions options) throws Exception {
        inputFilename = options.getInput();
        if (inputFilename == null) {
            System.out.println("Error: You must specify an input filename with the -i flag.");
            System.exit(0);
        }
        inputFile = new File(inputFilename);

        String gazTypeArg = options.getGazetteType().toLowerCase();
        if (gazTypeArg.startsWith("c")) {
            gazetteer = new CensusGazetteer();
        } else if (gazTypeArg.startsWith("n")) {
            gazetteer = new NGAGazetteer();
        } else if (gazTypeArg.startsWith("u")) {
            gazetteer = new USGSGazetteer();
        } else if (gazTypeArg.startsWith("w")) {
            gazetteer = new WGGazetteer();
        } else if (gazTypeArg.startsWith("t")) {
            gazetteer = new TRGazetteer();
        } else {
            System.err.println("Error: unrecognized gazetteer type: " + gazTypeArg);
            System.err.println("Please enter w, c, u, or g.");
            System.exit(0);
            //myGaz = new WGGazetteer();
        }

        outputFilename = options.getOutput();
        degreesPerRegion = options.getDegreesPerRegion();

        Properties myClassifierProperties = new Properties();
        classifier = new CRFClassifier(myClassifierProperties);
        classifier.loadClassifier(Constants.STANFORD_NER_HOME + "/classifiers/ner-eng-ie.crf-3-all2008-distsim.ser.gz");

        pairListSet = new SNERPairListSet(classifier);

        gazCache = new Hashtable<String, List<Location>>();
        barScale = options.getBarScale();
        docSet = new DocumentSet(options.getParagraphsAsDocs());
    }

    public void initializeRegionArray() {
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

    public void printRegionArray() {
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

    public List<Location> disambiguateAndCountPlacenames() throws Exception {

        System.out.println("Disambiguating place names found...");

        locations = new ArrayList<Location>();
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
                locations.add(curLocation);
                locationsFound.add(curLocation.id);
                }*/

                int curCount = idsToCounts.get(curLocation.id);
                if (curCount == 0) {// sentinel for not found in hashmap
                    locations.add(curLocation);
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
            locations.add(curLocation);*/


            /*Coordinate coord;
            if(gazetteer instanceof WGGazetteer)
            coord = ((WGGazetteer)gazetteer).baselineGet(placename);
            else
            coord = gazetteer.get(placename);

            if(coord.longitude == 9999.99) // sentinel
            continue;*/
        }

        for (int i = 0; i < locations.size(); i++) // set all the counts properly
        {
            locations.get(i).count = idsToCounts.get(locations.get(i).id);
        }

        System.out.println("Done.");

        return locations;
    }

    protected Location popBaselineDisambiguate(List<Location> possibleLocations)
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

    public void writeXMLFile(String inputFilename) throws Exception {
        writeXMLFile(inputFilename, outputFilename, locations);
    }

    public void processPath() throws Exception {
        processPath(getInputFile(), pairListSet);
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

    /**
     * @return the outputFilename
     */
    public String getOutputFilename() {
        return outputFilename;
    }

    /**
     * @return the inputFile
     */
    public File getInputFile() {
        return inputFile;
    }

    @Override
    public void train() {
        try {
            processPath();
        } catch (Exception ex) {
            Logger.getLogger(BaselineModel.class.getName()).log(Level.SEVERE, null, ex);
        }
        initializeRegionArray();
        try {
            disambiguateAndCountPlacenames();
        } catch (Exception ex) {
            Logger.getLogger(BaselineModel.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
