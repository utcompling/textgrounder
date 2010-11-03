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
package opennlp.textgrounder.old.models;

import gnu.trove.*;

import java.util.*;

import opennlp.textgrounder.old.gazetteers.older.Gazetteer;
import opennlp.textgrounder.old.geo.*;
import opennlp.textgrounder.old.textstructs.*;
import opennlp.textgrounder.old.topostructs.*;

/**
 * Model that disambiguates toponyms in a similar fashion to
 * BasicMinDistanceModel.java, but assigns weights so that the distance info
 * from some toponyms is considered more important than the info from other
 * toponyms.
 * 
 * FIXME: Explain better how the algorithm works.
 * 
 * @author Taesun Moon
 */
public class WeightedMinDistanceModel extends SelfTrainedModelBase {

    protected static final boolean INCLUDE_CORPUS_LEVEL_WEIGHTS = true; // true gives best results
    protected static final boolean FINAL_DISAMBIGUATE_ON_CORPUS_LEVEL_WEIGHTS_ONLY = false; // false gives best results

    //private static int NUM_ITERATIONS = 10; // replaced by model-iterations option from command line
    private static ArrayList<ArrayList<Double>> pseudoWeights;// = new ArrayList<ArrayList<Double>>();
    private static ArrayList<TIntArrayList> allPossibleLocations;

    private static TIntDoubleHashMap locationWeightsAcrossCorpus = new TIntDoubleHashMap();

    public static Random myRandom = new Random();

    public static TObjectDoubleHashMap<String> distanceCache = new TObjectDoubleHashMap<String>();

    public WeightedMinDistanceModel(Gazetteer gaz, int bscale, int paragraphsAsDocs) {
        super(gaz, bscale, paragraphsAsDocs);
    }

    public WeightedMinDistanceModel(CommandLineOptions options) throws Exception {
        super(options);
    }

    public void printArray(int[] array) {
        System.out.print("[");
        for(int o : array) {
            System.out.print(o + ",");
        }
        System.out.println("]");
    }

    public TIntHashSet disambiguateAndCountPlacenames() throws Exception {

	System.out.println("Initializing probabilistic minimum distance model data structure (this probably involves lots of SQLite lookups, so may take a while, but then all necessary information from the database will be cached so the rest should be fast)...");

        if(trainTokenArrayBuffer == null) {
            if(evalTokenArrayBuffer != null)
                trainTokenArrayBuffer = evalTokenArrayBuffer;
            else {
                System.err.println("Error: Both trainTokenArrayBuffer and evalTokenArrayBuffer are NULL.");
                System.exit(1);
            }
        }
        else {
            if(evalTokenArrayBuffer != null) {
                //System.out.println("Concatenating trainTokenArrayBuffer [" + trainTokenArrayBuffer.size() + "] and evalTokenArrayBuffer [" + evalTokenArrayBuffer.size() + "]");
                trainTokenArrayBuffer = evalTokenArrayBuffer.concatenate(trainTokenArrayBuffer);
                //System.out.println("New size: " + trainTokenArrayBuffer.size());
                //printArray(trainTokenArrayBuffer.wordVector);
                //printArray(trainTokenArrayBuffer.toponymVector);
                //printArray(trainTokenArrayBuffer.documentVector);
                //printArray(trainTokenArrayBuffer.stopwordVector);
                //System.exit(0);
            }
        }

	pseudoWeights = new ArrayList<ArrayList<Double>>();
	allPossibleLocations = new ArrayList<TIntArrayList>();
	for(int i = 0; i < trainTokenArrayBuffer.size(); i++) {
	    if(trainTokenArrayBuffer.toponymVector[i] == 0) {
		pseudoWeights.add(null);
		allPossibleLocations.add(null);
	    }
	    else {
		String placename = lexicon.getWordForInt(trainTokenArrayBuffer.wordVector[i]).toLowerCase();
		TIntHashSet curPossibleLocations = gazetteer.get(placename);
		/*if(curPossibleLocations.size() == 1) {
		    System.out.println(curPossibleLocations.toArray()[0]);
		    System.out.println(gazetteer.getLocation(curPossibleLocations.toArray()[0]));
		    }*/
		TIntArrayList curPossibleLocationsAL = new TIntArrayList();
		for(int possibleLocation : curPossibleLocations.toArray()) {
		    curPossibleLocationsAL.add(possibleLocation);
		}
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

	//updateLocationWeightsAcrossCorpus(pseudoWeights, locationWeightsAcrossCorpus);
	//System.out.println(pseudoWeights);
	//System.exit(0);

        System.out.println("Disambiguating place names found...");

        locations = new TIntHashSet();

	for(int j = 0; j < modelIterations; j++) {
	    System.out.println("Iteration: " + j);

	    int curDocNumber = -1;
	    int curDocBeginIndex = 0;

	    for (int i = 0; i < trainTokenArrayBuffer.size(); i++) {

		if(trainTokenArrayBuffer.documentVector[i] == curDocNumber) {
		    continue;
		}
		else {
		    curDocNumber = trainTokenArrayBuffer.documentVector[i];
		    curDocBeginIndex = i;
		}
		
		assignWeights(trainTokenArrayBuffer, curDocBeginIndex);
                //normalizePseudoWeights(pseudoWeights);///////////////////
                if(INCLUDE_CORPUS_LEVEL_WEIGHTS)
                    updateLocationWeightsAcrossCorpus(pseudoWeights, locationWeightsAcrossCorpus);
		//System.out.println(pseudoWeights);
		//System.exit(0);
	    }
	}

	System.out.println("Final step of disambiguation...");

	locations = doFinalDisambiguate(trainTokenArrayBuffer);

        System.out.println("Done. Returning " + locations.size() + " locations.");

        return locations;
    }

    // assigns weights for this document (1 iteration) - "distance step"
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

	//System.out.println("line 323");
	for(int j = curDocBeginIndex; j < tokenArrayBuffer.size() && tokenArrayBuffer.documentVector[j] == curDocNumber; j++) {
	    if(tokenArrayBuffer.toponymVector[j] == 0) {
		//System.out.println("ignoring non-toponym " + tokenArrayBuffer.wordVector[j]);
		continue; // ignore non-toponyms
	    }
	    int curTopidx = tokenArrayBuffer.wordVector[j];

	    String thisPlacename = lexicon.getWordForInt(tokenArrayBuffer.wordVector[j]).toLowerCase();

	    TIntHashSet possibleLocations = gazetteer.get(thisPlacename);

	    if(possibleLocations == null) {
		//System.out.println("null possibleLocations for " + thisPlacename);
		continue;
	    }

	    int[] possibleLocationIds = possibleLocations.toArray();
	    TIntDoubleHashMap totalWeightedDistances = new TIntDoubleHashMap();

	    //System.out.println("Calculating pseudoweights for " + thisPlacename);

	    //System.out.println("line 337");
	    for(int curLocId : possibleLocationIds) {
		Location curLoc = gazetteer.getLocation(curLocId);
		if(curLoc == null) continue;

		//int otherToponymCounter = 0;

		//System.out.println("line 344");
		for(int i = curDocBeginIndex; i < tokenArrayBuffer.size() && tokenArrayBuffer.documentVector[i] == curDocNumber; i++) {
		    if(tokenArrayBuffer.toponymVector[i] == 0) {
			//System.out.println("Skipping non-toponym " + tokenArrayBuffer.wordVector[i]);
			continue;
		    }
		    if(i == j) {
			//System.out.println("i == j; skipping");
			continue; // don't measure distance to yourself
		    }

		    String placename = lexicon.getWordForInt(tokenArrayBuffer.wordVector[i]).toLowerCase();
		    if(!gazetteer.contains(placename)) {
			//System.out.println(placename + " not found in gazetteer; skipping");
			continue;
		    }
		    //System.out.println("made it past the skips");
		    //otherToponymCounter++;

		    // seems like we can't use distanceCache here due to weights

		    //System.out.println("Calculating pseudoweights for " + thisPlacename + " based on " + placename + "...");

		    TIntHashSet otherPossibleLocs = gazetteer.get(placename);

		    double minWeightedDistance = Double.MAX_VALUE;
                    //double sumWeightedDistance = 0;
		    int[] otherPossibleLocsArray = otherPossibleLocs.toArray();
		    for(int k = 0; k < otherPossibleLocsArray.length; k++) {
			int otherPossibleLoc = otherPossibleLocsArray[k];
			Location otherLoc = gazetteer.getLocation(otherPossibleLoc);
			double curWeightedDistance = curLoc.computeDistanceTo(otherLoc) * pseudoWeights.get(i).get(k); // used to divide here
			//System.out.println("curWeightedDistance = " + curWeightedDistance);

			if(curWeightedDistance > 0 && curWeightedDistance < minWeightedDistance) {
			    minWeightedDistance = curWeightedDistance;
                            //sumWeightedDistance += curWeightedDistance;
                        }
		    }
		    if(minWeightedDistance < Double.MAX_VALUE) {
                        //minWeightedDistance /= sumWeightedDistance; // pseudo-weight normalization
			totalWeightedDistances.put(curLocId, totalWeightedDistances.get(curLocId) + minWeightedDistance);
			//System.out.println("Adding " + minWeightedDistance + " to " + placename);
		    }
		}
	    }

	    // set new weights:
	    for(int n = 0; n < allPossibleLocations.get(j).size(); n++) {
		double weightToSet = totalWeightedDistances.get(allPossibleLocations.get(j).get(n));
		newWeights.get(j-curDocBeginIndex).set(n, weightToSet);
	    }
	}

	//System.out.println("Normalizing pseudoweights...");

	//System.out.println("pseudoWeights.size() == " + pseudoWeights.size());
	//System.out.println("newWeights.size() == " + newWeights.size());

	//System.out.println(newWeights);
	//System.exit(0);

	// set pseudoWeights to newWeights:
	for(int m = curDocBeginIndex; m < pseudoWeights.size() && tokenArrayBuffer.documentVector[m] == curDocNumber; m++) {
	    if(pseudoWeights.get(m) == null) continue;

	    //double Z = 0; // normalization factor
	    //for(int n = 0; n < pseudoWeights.get(m).size(); n++)
	    //  Z += newWeights.get(m-curDocBeginIndex).get(n);
	    for(int n = 0; n < pseudoWeights.get(m).size(); n++) {
		pseudoWeights.get(m).set(n, newWeights.get(m-curDocBeginIndex).get(n)/* / Z*/); // normalization now happens in normalizePseudoWeights
		//System.out.println(pseudoWeights.get(m));
	    }
	}
	//System.exit(0);

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

    protected void normalizePseudoWeights(ArrayList<ArrayList<Double>> pseudoWeights) {
        for(int i = 0; i < pseudoWeights.size(); i++) {
	    ArrayList<Double> curTopWeights = pseudoWeights.get(i);
	    if(curTopWeights == null) continue;

            double sum = 0;
	    for(int j = 0; j < curTopWeights.size(); j++)
                sum += curTopWeights.get(j);
            for(int j = 0; j < curTopWeights.size(); j++)
                curTopWeights.set(j, curTopWeights.get(j) / sum);
        }
    }

    // "weight step"
    protected void updateLocationWeightsAcrossCorpus(ArrayList<ArrayList<Double>> pseudoWeights,
						     TIntDoubleHashMap locationWeightsAcrossCorpus) {

	TObjectDoubleHashMap<String> sums = new TObjectDoubleHashMap<String>(); // for normalization

	// initialize to 0
	for(int i = 0; i < pseudoWeights.size(); i++) {
	    ArrayList<Double> curTopWeights = pseudoWeights.get(i);
	    if(curTopWeights == null) continue;

	    for(int j = 0; j < curTopWeights.size(); j++) {
		int curLocationID = allPossibleLocations.get(i).get(j);
		locationWeightsAcrossCorpus.putIfAbsent(curLocationID, 0.0);
	    }
	    String placename = lexicon.getWordForInt(trainTokenArrayBuffer.wordVector[i]).toLowerCase();
	    sums.putIfAbsent(placename, 0.0);
	}

	// update
	for(int i = 0; i < pseudoWeights.size(); i++) {
	    ArrayList<Double> curTopWeights = pseudoWeights.get(i);
	    if(curTopWeights == null) continue;

	    String placename = lexicon.getWordForInt(trainTokenArrayBuffer.wordVector[i]).toLowerCase();

	    for(int j = 0; j < curTopWeights.size(); j++) {
		int curLocationID = allPossibleLocations.get(i).get(j);
		double valueToAdd = pseudoWeights.get(i).get(j);
		locationWeightsAcrossCorpus.put(curLocationID, locationWeightsAcrossCorpus.get(curLocationID) + valueToAdd);
		sums.put(placename, sums.get(placename) + valueToAdd);
		//if(curTopWeights.size() == 1) {
		/*
		if(j == 0) {
		    System.out.println("placename = " + placename);
		    System.out.println("curLocationID = " + curLocationID);
		    System.out.println("valueToAdd = " + valueToAdd);
		    System.out.println("updated locationWeightsAcrossCorpus.get(" + curLocationID + ") = " +
				       locationWeightsAcrossCorpus.get(curLocationID));
		    System.out.println("updated sums.get(" + placename + ") = " + sums.get(placename));
		}
		*/
	    }
	}
        
	// normalize corpus-level weights
	for(int curLocationID : locationWeightsAcrossCorpus.keys()) {
	    double Z = sums.get(gazetteer.getLocation(curLocationID).getName());
	    locationWeightsAcrossCorpus.put(curLocationID, locationWeightsAcrossCorpus.get(curLocationID) / Z);
	}

	// reset pseudoWeights
	for(int i = 0; i < pseudoWeights.size(); i++) {
	    ArrayList<Double> curTopWeights = pseudoWeights.get(i);
	    if(curTopWeights == null) continue;

	    for(int j = 0; j < curTopWeights.size(); j++) {
		int curLocationID = allPossibleLocations.get(i).get(j);

                /*if(includeDocLevelWeights)
                    pseudoWeights.get(i).set(j, locationWeightsAcrossCorpus.get(curLocationID));
                else*/
                // now includes document-level weights
                pseudoWeights.get(i).set(j, /*pseudoWeights.get(i).get(j) * */locationWeightsAcrossCorpus.get(curLocationID));
	    }
	}
    }

    protected TIntHashSet doFinalDisambiguate(TokenArrayBuffer tokenArrayBuffer) throws Exception {

	TIntHashSet locations = new TIntHashSet();
	TIntIntHashMap idsToCounts = new TIntIntHashMap();
	int curDocNumber = -1;
	int curDocBeginIndex = 0;

	TIntIntHashMap modelGuesses = new TIntIntHashMap();

	int range = evalTokenArrayBuffer != null? evalTokenArrayBuffer.size() : tokenArrayBuffer.size();
        for (int p = 0; p < range; p++) {

	    if(tokenArrayBuffer.documentVector[p] == curDocNumber) {
		continue;
	    }
	    else {
		curDocNumber = tokenArrayBuffer.documentVector[p];
		curDocBeginIndex = p;
	    }
	    
	    for(int j = curDocBeginIndex; j < range && tokenArrayBuffer.documentVector[j] == curDocNumber; j++) {
		if(tokenArrayBuffer.toponymVector[j] == 0) {
		    continue; // ignore non-toponyms
		}
		int curTopidx = tokenArrayBuffer.wordVector[j];

		String thisPlacename = lexicon.getWordForInt(curTopidx).toLowerCase();
		
		TIntHashSet possibleLocations = gazetteer.get(thisPlacename);
                addLocationsToRegionArray(possibleLocations);
		
		int[] possibleLocationIds = possibleLocations.toArray();
		TIntDoubleHashMap totalWeightedDistances = new TIntDoubleHashMap();

                int otherToponymCounter = 0;

		for(int curLocId : possibleLocationIds) {
		    Location curLoc = gazetteer.getLocation(curLocId);
		    if(curLoc == null) continue;

		    for(int i = curDocBeginIndex; i < range && tokenArrayBuffer.documentVector[i] == curDocNumber; i++) {
			if(tokenArrayBuffer.toponymVector[i] == 0) {
			    //System.out.println("Skipping non-toponym " + tokenArrayBuffer.wordVector[i]);
			    continue;
			}
			if(i == j) {
			    //System.out.println("i == j; skipping");
			    continue; // don't measure distance to yourself
			}
			
			String otherPlacename = lexicon.getWordForInt(tokenArrayBuffer.wordVector[i]).toLowerCase();
			if(!gazetteer.contains(otherPlacename)) {
			    //System.out.println(placename + " not found in gazetteer; skipping");
			    continue;
			}

                        otherToponymCounter++;
			//System.out.println("made it past the skips");
			
			// seems like we can't use distanceCache here due to weights
			
			//System.out.println("Calculating pseudoweights for " + thisPlacename + " based on " + placename + "...");
			
			TIntHashSet otherPossibleLocs = gazetteer.get(otherPlacename);
			
			double minWeightedDistance = Double.MAX_VALUE;
                        //double sumWeightedDistance = 0;
			int[] otherPossibleLocsArray = otherPossibleLocs.toArray();
			for(int k = 0; k < otherPossibleLocsArray.length; k++) {
			    int otherPossibleLoc = otherPossibleLocsArray[k];
			    Location otherLoc = gazetteer.getLocation(otherPossibleLoc);
                            double curWeightedDistance;
                            if(FINAL_DISAMBIGUATE_ON_CORPUS_LEVEL_WEIGHTS_ONLY)
                                curWeightedDistance = pseudoWeights.get(i).get(k); // used to divide here
                            else
                                curWeightedDistance = curLoc.computeDistanceTo(otherLoc) * pseudoWeights.get(i).get(k); // used to divide here
			    //System.out.println("curWeightedDistance = " + curWeightedDistance);
			    
			    if(curWeightedDistance > 0 && curWeightedDistance < minWeightedDistance) {
				minWeightedDistance = curWeightedDistance;
                                //sumWeightedDistance += curWeightedDistance;
			    }
			}
			if(minWeightedDistance < Double.MAX_VALUE) {
                            //minWeightedDistance /= sumWeightedDistance; // pseudo-weight normalization
			    totalWeightedDistances.put(curLocId, totalWeightedDistances.get(curLocId) + minWeightedDistance);
			    //System.out.println("Adding " + minWeightedDistance + " to " + placename);
                        }
                    }
		}

                int locIdToReturn = -1;

                // just get most prominent location for documents with only one toponym:
                if(otherToponymCounter == 0) {
                    double minPseudoWeight = Double.MAX_VALUE;
                    ArrayList<Double> curPseudoWeights = pseudoWeights.get(j);
                    for(int k = 0; k < curPseudoWeights.size(); k++) {
                        double curPseudoWeight = curPseudoWeights.get(k);
                        if(curPseudoWeight < minPseudoWeight) {
                            minPseudoWeight = curPseudoWeight;
                            locIdToReturn = allPossibleLocations.get(j).get(k);
                        }
                    }
                    //locIdToReturn = randomDisambiguate(possibleLocations);
                }
                else {
                    double minTotalWeightedDistance = Double.MAX_VALUE;
                    for(int curLocIdx : totalWeightedDistances.keys()) {
                        double curTotalWeightedDistance = totalWeightedDistances.get(curLocIdx);
                        if(curTotalWeightedDistance < minTotalWeightedDistance) {
                            minTotalWeightedDistance = curTotalWeightedDistance;
                            locIdToReturn = curLocIdx;
                        }
                    }
                }

                if(locIdToReturn == -1) {
                    //trainTokenArrayBuffer.modelLocationArrayList.add(null);
                    continue;
                }

                locations.add(locIdToReturn);

                Location curLocation = gazetteer.getLocation(locIdToReturn);
                //trainTokenArrayBuffer.modelLocationArrayList.add(curLocation);
                /*if(curLocation != null
                   && !trainTokenArrayBuffer.goldLocationArrayList.get(j-1).name.equals(trainTokenArrayBuffer.modelLocationArrayList.get(j-1).name)) {
                    System.out.println(trainTokenArrayBuffer.goldLocationArrayList.get(j-1).name);
                    System.out.println(trainTokenArrayBuffer.modelLocationArrayList.get(j-1).name);
                    System.out.println(j-1);
                    System.exit(0);
                    }*/
                if (curLocation == null) {
                    //System.out.println("null 3");
                    continue;
                }
                else {
                    modelGuesses.put(j, locIdToReturn);
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
	    }


	    
	    for (TIntIterator it = locations.iterator(); it.hasNext();) {
		int locid = it.next();
		Location loc = gazetteer.getLocation(locid);
                //if(loc == null)
                //    System.out.println("hi");
		loc.setCount(idsToCounts.get(locid));
	    }

	    //addLocationsToRegionArray(locations);
	}

        if(evalInputPath != null) {
            for(int i = 0; i < evalTokenArrayBuffer.size(); i++) {
                if(modelGuesses.contains(i)) {
                    int modelGuess = modelGuesses.get(i);
                    evalTokenArrayBuffer.modelLocationArrayList.add(gazetteer.getLocation(modelGuess));
                }
                else {
                    evalTokenArrayBuffer.modelLocationArrayList.add(null);
                }
            }
        }

	return locations;
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
