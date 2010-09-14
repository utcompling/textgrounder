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
package opennlp.textgrounder.models;

/*import opennlp.textgrounder.textstructs.StopwordList;
import opennlp.textgrounder.textstructs.NullStopwordList;
import opennlp.textgrounder.textstructs.TokenArrayBuffer;
import opennlp.textgrounder.textstructs.TextProcessor;*/
import gnu.trove.TIntHashSet;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntIterator;

import java.io.File;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import opennlp.textgrounder.gazetteers.old.Gazetteer;
import opennlp.textgrounder.gazetteers.old.WGGazetteer;
import opennlp.textgrounder.geo.*;
import opennlp.textgrounder.textstructs.*;
import opennlp.textgrounder.ners.*;
import opennlp.textgrounder.topostructs.*;

/**
 * Baseline model that disambiguates toponyms by picking the location with the
 * highest population.
 * 
 * @author
 */
public class PopulationBaselineModel extends SelfTrainedModelBase {

    //protected File trainInputFile;
    //protected boolean initializedXMLFile = false;
    //protected boolean finalizedXMLFile = false;
    public PopulationBaselineModel(Gazetteer gaz, int bscale, int paragraphsAsDocs) {
        super(gaz, bscale, paragraphsAsDocs);
        /*barScale = bscale;
        gazetteer = gaz;
        lexicon = new Lexicon();*/
    }

    public PopulationBaselineModel(CommandLineOptions options) throws Exception {
        super(options);
        /*
        runWholeGazetteer = options.getRunWholeGazetteer();

        if (!runWholeGazetteer) {
        trainInputPath = options.getTrainInputPath();
        if (trainInputPath == null) {
        System.out.println("Error: You must specify an input filename with the -i flag.");
        System.exit(0);
        }
        trainInputFile = new File(trainInputPath);
        } else {
        trainInputPath = null;
        trainInputFile = null;
        }

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
        System.err.println("Please enter w, c, u, g, or t.");
        System.exit(0);
        //myGaz = new WGGazetteer();
        }

        kmlOutputFilename = options.getKMLOutputFilename();
        degreesPerRegion = options.getDegreesPerRegion();

        if (!runWholeGazetteer) {
        gazCache = new Hashtable<String, List<Location>>();
        paragraphsAsDocs = options.getParagraphsAsDocs();
        lexicon = new Lexicon();

        textProcessor = new TextProcessor(lexicon, paragraphsAsDocs);
        }

        barScale = options.getBarScale();

        windowSize = options.getWindowSize();

         */
    }

    /*
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

    public void activateRegionsForWholeGaz() throws Exception {
    System.out.println("Running whole gazetteer through system...");

    //locations = new ArrayList<Location>();
    locations = gazetteer.getAllLocalities();
    addLocationsToRegionArray(locations);
    //locations = null; //////////////// uncomment this to get a null pointer but much faster termination
    //                                   if you only want to know the number of active regions :)
    }*/
    public TIntHashSet disambiguateAndCountPlacenames() throws Exception {

        System.out.println("Disambiguating place names found...");

        if(trainTokenArrayBuffer == null)
            trainTokenArrayBuffer = evalTokenArrayBuffer;

        locations = new TIntHashSet();
        //TIntHashSet locationsFound = new TIntHashSet();

        TIntIntHashMap idsToCounts = new TIntIntHashMap();

        /*	TObjectIntIterator<String> placeIterator = placeCounts.iterator();
        for (int i = placeCounts.size(); i-- > 0;) {
        placeIterator.advance();*/
//        assert (textProcessor.size() == lexicon.size());
        //int wvCounter = 0;
        for (int i = 0; i < trainTokenArrayBuffer.size(); i++) {
            /*for (int docIndex = 0; docIndex < lexicon.size(); docIndex++) {
            ArrayList<Integer> curDocSpans = textProcessor.get(docIndex);

            for (int i = 0; i < curDocSpans.size(); i++) {//int topidx : curDocSpans) {*/
            if (trainTokenArrayBuffer.toponymVector[i] == 0) {
                if(evalInputPath != null)
                    evalTokenArrayBuffer.modelLocationArrayList.add(null);
                continue;
            }
            int topidx = trainTokenArrayBuffer.wordVector[i];
            System.out.println("toponym (in int form): " + topidx);

            String placename = lexicon.getWordForInt(topidx).toLowerCase();
            System.out.println(placename);
            //assert(lexicon.getWordForInt(lexicon.getIntForWord(placename)).equalsIgnoreCase(placename));
		/*if(placename.contains(" "))
            System.out.println("CONTAINS SPACE");*/

            if (!gazetteer.contains(placename)) // quick lookup to see if it has even 1 place by that name
            {
                if(evalInputPath != null)
                    evalTokenArrayBuffer.modelLocationArrayList.add(null);
                continue;
            }

            // try the cache first. if not in there, do a full DB lookup and add that pair to the cache:
            TIntHashSet possibleLocations = gazetteer.get(placename);
            addLocationsToRegionArray(possibleLocations);

            Location curLocation = popBaselineDisambiguate(possibleLocations);
            if(evalInputPath != null)
                evalTokenArrayBuffer.modelLocationArrayList.add(curLocation);
            if (curLocation == null) {
                continue;
            }

            // instantiate the region containing curLocation

            /*if(!locationsFound.contains(curLocation.id)) {
            locations.add(curLocation);
            locationsFound.add(curLocation.id);
            }*/

            int curCount = idsToCounts.get(curLocation.getId());
            if (curCount == 0) {// sentinel for not found in hashmap
                locations.add(curLocation.getId());
                curLocation.setBackPointers(new ArrayList<Integer>());
                idsToCounts.put(curLocation.getId(), 1);
                System.out.println("Found first " + curLocation.getName() + "; id = " + curLocation.getId());
            } else {
                idsToCounts.increment(curLocation.getId());
                System.out.println("Found " + curLocation.getName() + " #" + idsToCounts.get(curLocation.getId()));
            }
            //DocIdAndIndex curDocIdAndIndex = new DocIdAndIndex(docIndex, i);
            curLocation.getBackPointers().add(i);
            //System.out.println(lexicon.getContext(curDocIdAndIndex, 10));
            //System.out.println(trainTokenArrayBuffer.wordArrayList
            //}



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

        for (TIntIterator it = locations.iterator(); it.hasNext();) {
            int locid = it.next();
            Location loc = gazetteer.getLocation(locid);
            loc.setCount(idsToCounts.get(locid));
        }

        System.out.println("Done.");

        return locations;
    }

    protected Location popBaselineDisambiguate(TIntHashSet possibleLocations)
          throws Exception {
        int maxPointPop = -1;
        Location pointToReturn = null;
        int maxRegionPop = -1;
        Location maxRegion = null;

        if (gazetteer instanceof WGGazetteer) {
            // establish the biggest region by this name:
            for (TIntIterator it = possibleLocations.iterator(); it.hasNext();) {
                int locid = it.next();
                Location loc = gazetteer.getLocation(locid);
                if (!loc.getType().equals("locality")) {
                    if (loc.getPop() > maxRegionPop) {
                        maxRegion = loc;
                        maxRegionPop = loc.getPop();
                    }
                }
            }

            // do the disambiguation:
            for (TIntIterator it = possibleLocations.iterator(); it.hasNext();) {
                int locid = it.next();
                Location loc = gazetteer.getLocation(locid);
                if (loc.getType().equals("locality")) {
                    if (loc.getPop() > maxPointPop && (maxRegion == null || loc.getPop() > maxRegionPop || (loc.getContainer() != null && loc.getContainer().equals(maxRegion.getName())))) {
                        pointToReturn = loc;
                        maxPointPop = loc.getPop();
                    }
                }
            }
        } else { // just return the most populous Location:
            for (TIntIterator it = possibleLocations.iterator(); it.hasNext();) {
                int locid = it.next();
                Location loc = gazetteer.getLocation(locid);
                if (loc.getPop() > maxPointPop) {
                    pointToReturn = loc;
                    maxPointPop = loc.getPop();
                }
            }
        }

        //if(maxPointPop <= 0) // abstain if no population information was known
        //    return null;

        return pointToReturn;
    }

    /*public void writeXMLFile(String inputFilename) throws Exception {
    writeXMLFile(inputFilename, kmlOutputFilename, locations);
    }

    public void processTrainInputPath() throws Exception {
    trainTokenArrayBuffer = new TokenArrayBuffer(lexicon);
    processTrainInputPath(trainInputFile, textProcessor, trainTokenArrayBuffer, new NullStopwordList());
    trainTokenArrayBuffer.convertToPrimitiveArrays();
    }*/
    /**
     * @return the kmlOutputFilename
     */
    //public String getOutputFilename() {
    //    return kmlOutputFilename;
    //}
    /**
     * @return the trainInputFile
     */
    //public File getInputFile() {
    //   return trainInputFile;
    //}
    /**
     * @return the textProcessor
     */
    //public TextProcessor getTextProcessor() {
    //    return textProcessor;
    // }
    @Override
    public void train() {
        super.train();
    }
}
