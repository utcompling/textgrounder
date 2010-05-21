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
 *
 * @author 
 */
public class ProbabilisticMinDistanceModel extends Model {

    //private static int NUM_ITERATIONS = 10; // replaced by model-iterations option from command line
    private static ArrayList<ArrayList<Double>> pseudoWeights;// = new ArrayList<ArrayList<Double>>();
    private static ArrayList<TIntArrayList> allPossibleLocations;

    public static Random myRandom = new Random();

    public static TObjectDoubleHashMap<String> distanceCache = new TObjectDoubleHashMap<String>();

    public ProbabilisticMinDistanceModel(Gazetteer gaz, int bscale, int paragraphsAsDocs) {
        super(gaz, bscale, paragraphsAsDocs);
    }

    public ProbabilisticMinDistanceModel(CommandLineOptions options) throws Exception {
        super(options);
    }

    public TIntHashSet disambiguateAndCountPlacenames() throws Exception {

	System.out.print("Initializing probabilistic minimum distance model data structure (this probably involves lots of SQLite lookups, so may take a while, but then all necessary information from the database will be cached so the rest should be fast)...");

	pseudoWeights = new ArrayList<ArrayList<Double>>();
	allPossibleLocations = new ArrayList<TIntArrayList>();
	for(int i = 0; i < evalTokenArrayBuffer.size(); i++) {
	    if(evalTokenArrayBuffer.toponymVector[i] == 0) {
		pseudoWeights.add(null);
		allPossibleLocations.add(null);
	    }
	    else {
		String placename = lexicon.getWordForInt(evalTokenArrayBuffer.wordVector[i]).toLowerCase();
		TIntHashSet curPossibleLocations = gazetteer.get(placename);
		TIntArrayList curPossibleLocationsAL = new TIntArrayList();
		for(int possibleLocation : curPossibleLocations.toArray())
		    curPossibleLocationsAL.add(possibleLocation);
		allPossibleLocations.add(curPossibleLocationsAL);

		ArrayList<Double> initWeights = new ArrayList<Double>();
		for(int j = 0; j < curPossibleLocations.size(); j++) {
		    initWeights.add(1.0/curPossibleLocations.size());
		}
		pseudoWeights.add(initWeights);
		System.out.println("Added " + curPossibleLocations.size() + " initial weights to pseudoWeights for " + placename);
	    }
	}

	System.out.println("done.");

        System.out.println("Disambiguating place names found...");

        locations = new TIntHashSet();

        TIntIntHashMap idsToCounts = new TIntIntHashMap();

	for(int j = 0; j < modelIterations; j++) {
	    System.out.println("Iteration: " + j);

	    int curDocNumber = -1;
	    int curDocBeginIndex = 0;

	    for (int i = 0; i < evalTokenArrayBuffer.size(); i++) {

		if(evalTokenArrayBuffer.documentVector[i] == curDocNumber) {
		    continue;
		}
		else {
		    curDocNumber = evalTokenArrayBuffer.documentVector[i];
		    curDocBeginIndex = i;
		}

		/*if (trainTokenArrayBuffer.toponymVector[i] == 0) {
		    trainTokenArrayBuffer.modelLocationArrayList.add(null);
		    continue;
		    }
		int topidx = trainTokenArrayBuffer.wordVector[i];
		System.out.println("toponym (in int form): " + topidx);

		String placename = lexicon.getWordForInt(topidx).toLowerCase();
		System.out.println(placename);

		if (!gazetteer.contains(placename)) // quick lookup to see if it has even 1 place by that name
		    {
			trainTokenArrayBuffer.modelLocationArrayList.add(null);
			continue;
		    }

		// try the cache first. if not in there, do a full DB lookup and add that pair to the cache:
		TIntHashSet possibleLocations = gazetteer.get(placename);
		addLocationsToRegionArray(possibleLocations); */
		
		assignWeights(evalTokenArrayBuffer, curDocBeginIndex);
		/*Location curLocation = gazetteer.getLocation(curLocationIdx);
		trainTokenArrayBuffer.modelLocationArrayList.add(curLocation);
		if (curLocation == null) {
		    continue;
		}
		
		int curCount = idsToCounts.get(curLocation.id);
		if (curCount == 0) {// sentinel for not found in hashmap
		    locations.add(curLocation.id);
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
		//System.out.println(trainTokenArrayBuffer.wordArrayList
		//}
		*/
	    }
	}

	System.out.println("Final step of disambiguation; pseudoWeights.size() == " + pseudoWeights.size());
	for(int i = 0; i < pseudoWeights.size(); i++) {
	    if(pseudoWeights.get(i) == null) {
		evalTokenArrayBuffer.modelLocationArrayList.add(null);
		continue;
	    }

	    String placename = lexicon.getWordForInt(evalTokenArrayBuffer.wordVector[i]).toLowerCase();

	    if (!gazetteer.contains(placename)) // quick lookup to see if it has even 1 place by that name
            {
                evalTokenArrayBuffer.modelLocationArrayList.add(null);
                continue;
            }

	    double maxWeight = 0;
	    int maxIndex = -1;
	    for(int j = 0; j < pseudoWeights.get(i).size(); j++) {
		if(pseudoWeights.get(i).get(j) > maxWeight) {
		    maxWeight = pseudoWeights.get(i).get(j);
		    maxIndex = j;
		}
	    }
	    if(maxIndex == -1) {
		evalTokenArrayBuffer.modelLocationArrayList.add(null);
		continue;
	    }
	    int curLocationIdx = allPossibleLocations.get(i).get(maxIndex);
	    locations.add(curLocationIdx);

	    Location curLocation = gazetteer.getLocation(curLocationIdx);
	    evalTokenArrayBuffer.modelLocationArrayList.add(curLocation);
	    if (curLocation == null) {
		continue;
	    }
	    
	    int curCount = idsToCounts.get(curLocation.id);
	    if (curCount == 0) {// sentinel for not found in hashmap
		locations.add(curLocation.id);
		curLocation.backPointers = new ArrayList<Integer>();
		idsToCounts.put(curLocation.id, 1);
		System.out.println("Found first " + curLocation.name + "; id = " + curLocation.id);
	    } else {
		idsToCounts.increment(curLocation.id);
		System.out.println("Found " + curLocation.name + " #" + idsToCounts.get(curLocation.id));
	    }
	}

	for (TIntIterator it = locations.iterator(); it.hasNext();) {
	  int locid = it.next();
	  Location loc = gazetteer.getLocation(locid);
	  loc.count = idsToCounts.get(locid);
	}

	addLocationsToRegionArray(locations);

        System.out.println("Done. Returning " + locations.size() + " locations.");

	/*
	for(int curLocation : locations.toArray()) {
	    System.out.println(gazetteer.getLocation(curLocation));
	}
	*/

        return locations;
    }

    // assigns weights for this document (1 iteration)
    protected void assignWeights(TokenArrayBuffer tokenArrayBuffer, int curDocBeginIndex) throws Exception {

	int curDocNumber = tokenArrayBuffer.documentVector[curDocBeginIndex];

	ArrayList<ArrayList<Double>> newWeights = new ArrayList<ArrayList<Double>>(); // new weights for this document only
	for(int j = curDocBeginIndex; j < pseudoWeights.size() && tokenArrayBuffer.documentVector[j] == curDocNumber; j++) {
	    if(tokenArrayBuffer.toponymVector[j] == 0) {
		newWeights.add(null);
	    }
	    else {
		/*
		TIntHashSet curPossibleLocations = gazetteer.get(lexicon.getWordForInt(trainTokenArrayBuffer.wordVector[j]).toLowerCase());

		ArrayList<Double> curNewWeights = new ArrayList<Double>();
		for(int k = 0; k < curPossibleLocations.size(); k++)
		    curNewWeights.add(null);
		newWeights.add(curNewWeights);
		*/

		ArrayList<Double> curNewWeights = new ArrayList<Double>();
		for(int k = 0; k < pseudoWeights.get(j).size(); k++)
		    curNewWeights.add(pseudoWeights.get(j).get(k));
		newWeights.add(curNewWeights);
	    }
	}

	for(int j = curDocBeginIndex; j < tokenArrayBuffer.size() && tokenArrayBuffer.documentVector[j] == curDocNumber; j++) {
	    if(tokenArrayBuffer.toponymVector[j] == 0) continue; // ignore non-toponyms
	    int curTopidx = tokenArrayBuffer.wordVector[j];

	    String thisPlacename = lexicon.getWordForInt(tokenArrayBuffer.wordVector[j]).toLowerCase();

	    TIntHashSet possibleLocations = gazetteer.get(curTopidx);

	    if(possibleLocations == null) continue;

	    int[] possibleLocationIds = possibleLocations.toArray();
	    TIntDoubleHashMap totalWeightedDistances = new TIntDoubleHashMap();

	    for(int curLocId : possibleLocationIds) {
		Location curLoc = gazetteer.getLocation(curLocId);
		if(curLoc == null) continue;

		//int otherToponymCounter = 0;

		for(int i = curDocBeginIndex; i < tokenArrayBuffer.size() && tokenArrayBuffer.documentVector[i] == curDocNumber; i++) {
		    if(tokenArrayBuffer.toponymVector[i] == 0) continue;
		    if(i == j) continue; // don't measure distance to yourself

		    String placename = lexicon.getWordForInt(tokenArrayBuffer.wordVector[i]).toLowerCase();
		    if(!gazetteer.contains(placename)) continue;
		    //otherToponymCounter++;

		    // seems like we can't use distanceCache here due to weights

		    System.out.println("Calculating pseudoweights for " + thisPlacename + " based on " + placename + "...");

		    TIntHashSet otherPossibleLocs = gazetteer.get(placename);

		    double minWeightedDistance = Double.MAX_VALUE;
		    int[] otherPossibleLocsArray = otherPossibleLocs.toArray();
		    for(int k = 0; k < otherPossibleLocsArray.length; k++) {
			int otherPossibleLoc = otherPossibleLocsArray[k];
			Location otherLoc = gazetteer.getLocation(otherPossibleLoc);
			double curWeightedDistance = curLoc.computeDistanceTo(otherLoc) / pseudoWeights.get(i).get(k);

			if(curWeightedDistance < minWeightedDistance)
			    minWeightedDistance = curWeightedDistance;
		    }
		    totalWeightedDistances.put(curLocId, totalWeightedDistances.get(curLocId) + minWeightedDistance);
		}
	    }

	    // set new weights:
	    for(int n = 0; n < allPossibleLocations.get(j).size(); n++) {
		newWeights.get(j-curDocBeginIndex).set(n, 1.0/totalWeightedDistances.get(allPossibleLocations.get(j).get(n)));
	    }
	}

	System.out.println("Normalizing pseudoweights...");

	System.out.println("pseudoWeights.size() == " + pseudoWeights.size());
	System.out.println("newWeights.size() == " + newWeights.size());
	
	// normalize new weights and overwrite old weights:
	for(int m = curDocBeginIndex; m < pseudoWeights.size() && tokenArrayBuffer.documentVector[m] == curDocNumber; m++) {
	    if(pseudoWeights.get(m) == null) continue;

	    double Z = 0; // normalization factor
	    for(int n = 0; n < pseudoWeights.get(m).size(); n++)
		Z += newWeights.get(m-curDocBeginIndex).get(n);
	    for(int n = 0; n < pseudoWeights.get(m).size(); n++) {
		pseudoWeights.get(m).set(n, newWeights.get(m-curDocBeginIndex).get(n) / Z);
	    }
	}

	/*
	int[] possibleLocationIds = possibleLocations.toArray();
	TIntDoubleHashMap totalDistances = new TIntDoubleHashMap();
	

	int curDocNumber = trainTokenArrayBuffer.documentVector[curDocBeginIndex];
	//String thisPlacename = lexicon.getWordForInt(trainTokenArrayBuffer.wordVector[curIndex]).toLowerCase();

	// for each possible Location we could disambiguate to, compute the total distance from it to any solution of the
	//   other toponyms in this document:
	for(int curLocId : possibleLocationIds) {
	    Location curLoc = gazetteer.getLocation(curLocId);
	    if(curLoc == null) continue;

	    int otherToponymCounter = 0;

	    for(int i = curDocBeginIndex; i < trainTokenArrayBuffer.size() && trainTokenArrayBuffer.documentVector[i] == curDocNumber; i++) {
		if(trainTokenArrayBuffer.toponymVector[i] == 0) continue; // ignore non-toponyms
		if(i == curIndex) continue; // don't measure distance to yourself
		
		String placename = lexicon.getWordForInt(trainTokenArrayBuffer.wordVector[i]).toLowerCase();
		if(!gazetteer.contains(placename)) continue;
		otherToponymCounter++;

		//System.out.println("Computing minimum distance from " + thisPlacename
		//		   + " to any possible disambiguation of " + placename);

		if(distanceCache.contains(thisPlacename + ";" + placename)) {
		    totalDistances.put(curLocId, totalDistances.get(curLocId) + distanceCache.get(thisPlacename + ";" + placename));
		    continue;
		}

		TIntHashSet otherPossibleLocs = gazetteer.get(placename);

		double minDistance = Double.MAX_VALUE;
		for(int otherPossibleLoc : otherPossibleLocs.toArray()) {
		    Location otherLoc = gazetteer.getLocation(otherPossibleLoc);
		    double curDistance = curLoc.computeDistanceTo(otherLoc);
		    if(curDistance < minDistance) {
			minDistance = curDistance;
		    }
		}
		totalDistances.put(curLocId, totalDistances.get(curLocId) + minDistance);
		distanceCache.put(thisPlacename + ";" + placename, minDistance);
		distanceCache.put(placename + ";" + thisPlacename, minDistance); // insert doubly since distance is symmetric
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

	System.out.println("Returning location ID " + toReturn + " which had min total distance " + minTotalDistance);
	*/
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
        initializeRegionArray();
        if (!runWholeGazetteer) {
            try {
                processEvalInputPath();
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
