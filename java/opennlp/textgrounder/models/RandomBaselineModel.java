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

import gnu.trove.TIntIntHashMap;

import java.io.File;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import opennlp.textgrounder.gazetteers.*;
import opennlp.textgrounder.geo.*;
import opennlp.textgrounder.textstructs.*;
import opennlp.textgrounder.models.callbacks.*;
import opennlp.textgrounder.ners.*;
import opennlp.textgrounder.topostructs.*;

/**
 *
 * @author 
 */
public class RandomBaselineModel extends Model {

    public static Random myRandom = new Random();

    public RandomBaselineModel(Gazetteer gaz, int bscale, int paragraphsAsDocs) {
	super(gaz, bscale, paragraphsAsDocs);
    }

    public RandomBaselineModel(CommandLineOptions options) throws Exception {
	super(options);
    }

    public List<Location> disambiguateAndCountPlacenames() throws Exception {

        System.out.println("Disambiguating place names found...");

        locations = new ArrayList<Location>();
        //TIntHashSet locationsFound = new TIntHashSet();

        TIntIntHashMap idsToCounts = new TIntIntHashMap();

        /*	TObjectIntIterator<String> placeIterator = placeCounts.iterator();
        for (int i = placeCounts.size(); i-- > 0;) {
        placeIterator.advance();*/
//        assert (textProcessor.size() == lexicon.size());
        //int wvCounter = 0;
        for (int i = 0; i < tokenArrayBuffer.size(); i++) {
            /*for (int docIndex = 0; docIndex < lexicon.size(); docIndex++) {
            ArrayList<Integer> curDocSpans = textProcessor.get(docIndex);

            for (int i = 0; i < curDocSpans.size(); i++) {//int topidx : curDocSpans) {*/
            if (tokenArrayBuffer.toponymVector[i] == 0) {
		tokenArrayBuffer.modelLocationArrayList.add(null);
                continue;
            }
            int topidx = tokenArrayBuffer.wordVector[i];
            System.out.println("toponym (in int form): " + topidx);

            String placename = lexicon.getWordForInt(topidx).toLowerCase();
            System.out.println(placename);
            //assert(lexicon.getWordForInt(lexicon.getIntForWord(placename)).equalsIgnoreCase(placename));
		/*if(placename.contains(" "))
            System.out.println("CONTAINS SPACE");*/

            if (!gazetteer.contains(placename)) // quick lookup to see if it has even 1 place by that name
            {
		tokenArrayBuffer.modelLocationArrayList.add(null);
                continue;
            }

            // try the cache first. if not in there, do a full DB lookup and add that pair to the cache:
            List<Location> possibleLocations = gazCache.get(placename);
            if (possibleLocations == null) {
                possibleLocations = gazetteer.get(placename);
		/*System.out.println(possibleLocations.size());
		for(Location temploc : possibleLocations) {
		    System.out.println(temploc);
		    }*/
                gazCache.put(placename, possibleLocations);
                addLocationsToRegionArray(possibleLocations);
            }

            Location curLocation = randomDisambiguate(possibleLocations);
	    tokenArrayBuffer.modelLocationArrayList.add(curLocation);
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
                curLocation.backPointers = new ArrayList<Integer>();
                idsToCounts.put(curLocation.id, 1);
                System.out.println("Found first " + curLocation.name + "; id = " + curLocation.id);
            } else {
                idsToCounts.increment(curLocation.id);
                System.out.println("Found " + curLocation.name + " #" + idsToCounts.get(curLocation.id));
            }
            //DocIdAndIndex curDocIdAndIndex = new DocIdAndIndex(docIndex, i);
            curLocation.backPointers.add(i);
            //System.out.println(lexicon.getContext(curDocIdAndIndex, 10));
            //System.out.println(tokenArrayBuffer.wordArrayList
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

        for (int i = 0; i < locations.size(); i++) // set all the counts properly
        {
            locations.get(i).count = idsToCounts.get(locations.get(i).id);
        }

        System.out.println("Done.");

        return locations;
    }

    // return a random Location
    protected Location randomDisambiguate(List<Location> possibleLocations) throws Exception {
	
	int size = possibleLocations.size();

	if(size == 0)
	    return null;

	int randIndex = myRandom.nextInt(size);

	Location toReturn = possibleLocations.get(randIndex);

	/*System.out.println("Random number " + randIndex + " yielded:");
	  System.out.println(toReturn);*/

	return toReturn;
    }

    @Override
    public void train() {
        initializeRegionArray();
        if (!runWholeGazetteer) {
            try {
                processPath();
            } catch (Exception ex) {
                Logger.getLogger(BaselineModel.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
                disambiguateAndCountPlacenames();
            } catch (Exception ex) {
                Logger.getLogger(BaselineModel.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            try {
                activateRegionsForWholeGaz();
            } catch (Exception ex) {
                Logger.getLogger(BaselineModel.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
