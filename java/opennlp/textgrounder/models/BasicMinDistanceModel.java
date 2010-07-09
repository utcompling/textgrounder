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

import gnu.trove.*;

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
 * Model that disambiguates toponyms by picking the location with minimum total
 * distance to some resolution of the other toponyms in this document. Falls
 * back on random disambiguator if there are no other toponyms to compare to.
 * 
 * FIXME: Explain better how the algorithm works.
 * 
 * @author Taesun Moon
 */
public class BasicMinDistanceModel extends SelfTrainedModelBase {

    public static Random myRandom = new Random();

    public static TObjectDoubleHashMap<String> distanceCache = new TObjectDoubleHashMap<String>();

    public BasicMinDistanceModel(Gazetteer<Location> gaz, int bscale, int paragraphsAsDocs) {
        super(gaz, bscale, paragraphsAsDocs);
    }

    public BasicMinDistanceModel(CommandLineOptions options) throws Exception {
        super(options);
    }

    public TIntHashSet disambiguateAndCountPlacenames() throws Exception {

        System.out.println("Disambiguating place names found...");

        if(trainTokenArrayBuffer == null)
            trainTokenArrayBuffer = evalTokenArrayBuffer;

        locations = new TIntHashSet();

        TIntIntHashMap idsToCounts = new TIntIntHashMap();

	int curDocNumber = 0;
	int curDocBeginIndex = 0;

        for (int i = 0; i < trainTokenArrayBuffer.size(); i++) {

	    if(trainTokenArrayBuffer.documentVector[i] != curDocNumber) {
		curDocNumber = trainTokenArrayBuffer.documentVector[i];
		curDocBeginIndex = i;
	    }

            if (trainTokenArrayBuffer.toponymVector[i] == 0) {
                if(evalInputPath != null)
                    evalTokenArrayBuffer.modelLocationArrayList.add(null);
                continue;
            }
            int topidx = trainTokenArrayBuffer.wordVector[i];
            System.out.println("toponym (in int form): " + topidx);

            String placename = lexicon.getWordForInt(topidx).toLowerCase();
            System.out.println(placename);

            if (!gazetteer.contains(placename)) // quick lookup to see if it has even 1 place by that name
            {
                if(evalInputPath != null)
                    evalTokenArrayBuffer.modelLocationArrayList.add(null);
                continue;
            }

            // try the cache first. if not in there, do a full DB lookup and add that pair to the cache:
            TIntHashSet possibleLocations = gazetteer.get(placename);
            addLocationsToRegionArray(possibleLocations);

            int curLocationIdx = basicMinDistanceDisambiguate(possibleLocations, trainTokenArrayBuffer, i, curDocBeginIndex);
            Location curLocation = gazetteer.getLocation(curLocationIdx);
            if(evalInputPath != null)
                evalTokenArrayBuffer.modelLocationArrayList.add(curLocation);
            if (curLocation == null) {
                continue;
            }

            int curCount = idsToCounts.get(curLocation.getId());
            if (curCount == 0) {// sentinel for not found in hashmap
                locations.add(curLocation.getId());
                curLocation.setBackPointers(new ArrayList<Integer>());
                idsToCounts.put(curLocation.getId(), 1);
                System.out.println("Found first " + curLocation.getName() + ": " + curLocation);
            } else {
                idsToCounts.increment(curLocation.getId());
                System.out.println("Found " + curLocation.getName() + " #" + idsToCounts.get(curLocation.getId()));
            }
            //DocIdAndIndex curDocIdAndIndex = new DocIdAndIndex(docIndex, i);
            curLocation.getBackPointers().add(i);
            //System.out.println(lexicon.getContext(curDocIdAndIndex, 10));
            //System.out.println(trainTokenArrayBuffer.wordArrayList
            //}

        }

        for (TIntIterator it = locations.iterator(); it.hasNext();) {
            int locid = it.next();
            Location loc = gazetteer.getLocation(locid);
            loc.setCount(idsToCounts.get(locid));
        }

        System.out.println("Done. Returning " + locations.size() + " locations.");

	/*
	for(int curLocation : locations.toArray()) {
	    System.out.println(gazetteer.getLocation(curLocation));
	}
	*/

        return locations;
    }

    // return the Location with minimum total distance to some resolution of the other toponyms in this document.
    // falls back on random disambiguator if there are no other toponyms to compare to.
    protected int basicMinDistanceDisambiguate(TIntHashSet possibleLocations,
					       TokenArrayBuffer tokenArrayBuffer,
					       int curIndex, int curDocBeginIndex) throws Exception {

	int[] possibleLocationIds = possibleLocations.toArray();
	TIntDoubleHashMap totalDistances = new TIntDoubleHashMap();

	int curDocNumber = tokenArrayBuffer.documentVector[curDocBeginIndex];
	String thisPlacename = lexicon.getWordForInt(tokenArrayBuffer.wordVector[curIndex]).toLowerCase();

	// for each possible Location we could disambiguate to, compute the total distance from it to any solution of the
	//   other toponyms in this document:
	for(int curLocId : possibleLocationIds) {
	    Location curLoc = gazetteer.getLocation(curLocId);
	    if(curLoc == null) continue;

	    int otherToponymCounter = 0;

	    for(int i = curDocBeginIndex; i < tokenArrayBuffer.size() && tokenArrayBuffer.documentVector[i] == curDocNumber; i++) {
		if(tokenArrayBuffer.toponymVector[i] == 0) continue; // ignore non-toponyms
		if(i == curIndex) continue; // don't measure distance to yourself
		
		String otherPlacename = lexicon.getWordForInt(tokenArrayBuffer.wordVector[i]).toLowerCase();
		if(!gazetteer.contains(otherPlacename)) continue;
		otherToponymCounter++;

		//System.out.println("Computing minimum distance from " + thisPlacename
		//		   + " to any possible disambiguation of " + placename);

		/*if(distanceCache.contains(thisPlacename + ";" + otherPlacename)) {
		    totalDistances.put(curLocId, totalDistances.get(curLocId) + distanceCache.get(thisPlacename + ";" + otherPlacename));
		    continue;
		}*/

		TIntHashSet otherPossibleLocs = gazetteer.get(otherPlacename);

		double minDistance = Double.MAX_VALUE;
		for(int otherPossibleLoc : otherPossibleLocs.toArray()) {
		    Location otherLoc = gazetteer.getLocation(otherPossibleLoc);
		    double curDistance = curLoc.computeDistanceTo(otherLoc);
		    if(curDistance < minDistance) {
			minDistance = curDistance;
		    }
		}
		totalDistances.put(curLocId, totalDistances.get(curLocId) + minDistance);
		//distanceCache.put(thisPlacename + ";" + otherPlacename, minDistance);
		//distanceCache.put(otherPlacename + ";" + thisPlacename, minDistance); // insert doubly since distance is symmetric
	    }
	    if(otherToponymCounter == 0) {
		//System.out.println("No other toponyms to compare to; backing off to random baseline.");
		return randomDisambiguate(possibleLocations);
	    }
	}

	// find and return the least such total distance:
	double minTotalDistance = Double.MAX_VALUE;
	int toReturn = -1;
	for(int curLocId : totalDistances.keys()) {
	    double curDistance = totalDistances.get(curLocId);
	    if(totalDistances.get(curLocId) < minTotalDistance) {
		minTotalDistance = curDistance;
		toReturn = curLocId;
	    }
	}

        //if(toReturn == -1)

	System.out.println("Returning location ID " + toReturn + " which had min total distance " + minTotalDistance);

        return toReturn;
    }

    // return a random Location index
    protected int randomDisambiguate(TIntHashSet possibleLocations)
          throws Exception {

        int size = possibleLocations.size();

        if (size == 0) {
            return -1;
        }

        int randIndex = myRandom.nextInt(size);

        int toReturn = possibleLocations.toArray()[randIndex];

        /*System.out.println("Random number " + randIndex + " yielded:");
        System.out.println(toReturn);*/

        return toReturn;
    }

    @Override
    public void train() {
        super.train();
    }
}
