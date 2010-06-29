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

import gnu.trove.TIntHashSet;
import gnu.trove.TIntIterator;
import gnu.trove.TIntObjectHashMap;
import java.io.FileInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import opennlp.textgrounder.annealers.*;
import opennlp.textgrounder.ec.util.MersenneTwisterFast;
import opennlp.textgrounder.geo.CommandLineOptions;
import opennlp.textgrounder.models.callbacks.*;
import opennlp.textgrounder.textstructs.*;
import opennlp.textgrounder.topostructs.*;

/**
 * 
 * @author tsmoon
 */
public class NonRandomStartRegionModel<E extends SmallLocation> extends RegionModel<E> {

    protected EvalTokenArrayBuffer<Location> weightedEvalTokenArrayBuffer;

    public NonRandomStartRegionModel(CommandLineOptions _options,
          E _genericsKludgeFactor) {
        regionMapperCallback = new RegionMapperCallback();
        genericsKludgeFactor = _genericsKludgeFactor;

        try {
            initialize(_options);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(NonRandomStartRegionModel.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(NonRandomStartRegionModel.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(EvalRegionModel.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(EvalRegionModel.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     *
     * @param _options
     */
    @Override
    protected void initialize(CommandLineOptions _options) throws
          FileNotFoundException, IOException, ClassNotFoundException,
          SQLException {
        modelIterations = _options.getModelIterations();
        evalIterations = _options.getEvalIterations();
        degreesPerRegion = _options.getDegreesPerRegion();

        int randSeed = _options.getRandomSeed();
        if (randSeed == 0) {
            /**
             * Case for complete random seeding
             */
            rand = new MersenneTwisterFast();
        } else {
            /**
             * Case for non-random seeding. For debugging. Also, the default
             */
            rand = new MersenneTwisterFast(randSeed);
        }

        alpha = _options.getAlpha();
        beta = _options.getBeta();

        SerializableRegionTrainingParameters<E> serializableRegionTrainingParameters = new SerializableRegionTrainingParameters<E>();
        serializableRegionTrainingParameters.loadParameters(_options.getSerializedDataParametersFilename(), this);

        stopwordList = new StopwordList();
        initializeRegionArray();
        locationSet = new TIntHashSet();
        setAllocateRegions(evalTokenArrayBuffer);

        String weightedEvalTokenArrayBufferFilename = _options.getSerializedEvalTokenArrayBufferFilename();

        ObjectInputStream modelIn = null;
        try {
            modelIn =
                  new ObjectInputStream(new GZIPInputStream(new FileInputStream(weightedEvalTokenArrayBufferFilename)));
        } catch (FileNotFoundException e) {
            modelIn =
                  new ObjectInputStream(new GZIPInputStream(new FileInputStream(weightedEvalTokenArrayBufferFilename + ".gz")));
        }
        weightedEvalTokenArrayBuffer = (EvalTokenArrayBuffer<Location>) modelIn.readObject();

        annealer = new EvalAnnealer(_options.getEvalIterations());
    }

    /**
     * Randomly initialize fields for training. If word is a toponym, choose
     * random region only from regions aligned to name.
     */
    @Override
    public void randomInitialize() {
        int wordid, docid, topicid;
        int istoponym, isstopword;
        int wordoff, docoff;
        double[] probs = new double[T];
        double totalprob, max, r;

        ArrayList<Location> weightedModelLocationArray = weightedEvalTokenArrayBuffer.modelLocationArrayList;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            if (isstopword == 0) {
                wordid = wordIdMapper[wordVector[i]];
                docid = documentVector[i];
                docoff = docid * T;
                istoponym = toponymVector[i];

                totalprob = 0;
                if (istoponym == 1) {
                    Location modelLoc = weightedModelLocationArray.get(i);
                    int regid = getRegionForLocation(modelLoc);
                    if (regid > -1) {
                        try {
                            for (int j = 0;; ++j) {
                                totalprob = probs[j] = 0;
                            }
                        } catch (ArrayIndexOutOfBoundsException e) {
                        }
                        totalprob += probs[regid] = 1;
                    } else {
                        wordoff = wordid * T;
                        try {
                            for (int j = 0;; ++j) {
                                totalprob += probs[j] =
                                      regionByToponymFilter[wordoff + j]
                                      * activeRegionByDocumentFilter[docoff + j];
                            }
                        } catch (ArrayIndexOutOfBoundsException e) {
                        }
                    }
                } else {
                    try {
                        for (int j = 0;; ++j) {
                            totalprob += probs[j] =
                                  activeRegionByDocumentFilter[docoff + j];
                        }
                    } catch (ArrayIndexOutOfBoundsException e) {
                    }
                }

                r = rand.nextDouble() * totalprob;

                max = probs[0];
                topicid = 0;
                while (r > max) {
                    topicid++;
                    max += probs[topicid];
                }

                topicVector[i] = topicid;
                topicCounts[topicid]++;
                topicByDocumentCounts[docid * T + topicid]++;
                wordByTopicCounts[wordid * T + topicid]++;
            }
        }
    }

    protected int getRegionForLocation(Location loc) {
        if (loc == null) {
            return -1;
        }
        int curX = (int) (loc.getCoord().latitude + 180) / (int) degreesPerRegion;
        int curY = (int) (loc.getCoord().longitude + 90) / (int) degreesPerRegion;

        if (regionArray[curX][curY] == null) {
            /**
             * means this location has no resolution in current model. return -1 one and let model initialize randomly
             */
            return -1;
        }
        return regionMapperCallback.getRegionToIdxMap().get(regionArray[curX][curY]);
    }

    /**
     *
     */
    @Override
    public void normalize() {
        for (TIntIterator it = locationSet.iterator(); it.hasNext();) {
            int locid = it.next();
            E loc = dataSpecificLocationMap.get(locid);
            loc.setCount(loc.getCount() + beta);
        }

        TIntObjectHashMap<TIntHashSet> toponymRegionToLocations = regionMapperCallback.getToponymRegionToLocations();

        int wordid, topicid;
        int istoponym;

        int newlocid = 0;
        for (int locid : dataSpecificLocationMap.keys()) {
            if (locid > newlocid) {
                newlocid = locid;
            }
        }
        int curlocid = newlocid + 1;

        for (int i = 0; i < N; ++i) {
            istoponym = toponymVector[i];
            if (istoponym == 1) {
                wordid = wordVector[i];
                topicid = topicVector[i];
                ToponymRegionPair trp = new ToponymRegionPair(wordid, topicid);
                TIntHashSet locs = toponymRegionToLocations.get(trp.hashCode());
                try {
                    int size = locs.size();
                    int randIndex = rand.nextInt(size);
                    int curLocationIdx = locs.toArray()[randIndex];
                    E curLocation = dataSpecificLocationMap.get(curLocationIdx);
                    if (curLocation == null) {
                        throw new NullPointerException();
                    }
                    evalTokenArrayBuffer.modelLocationArrayList.add(curLocation);
                } catch (NullPointerException e) {
                    locs = new TIntHashSet();
                    Region r = regionMapperCallback.getIdxToRegionMap().get(topicid);
                    Coordinate coord = new Coordinate(r.centLon, r.centLat);
                    E curLocation = generateLocation(curlocid, lexicon.getWordForInt(wordid), null, coord, 0, null, 1, wordid);
                    dataSpecificLocationMap.put(curlocid, curLocation);
                    evalTokenArrayBuffer.modelLocationArrayList.add(curLocation);
                    locs.add(curLocation.getId());
                    toponymRegionToLocations.put(trp.hashCode(), locs);
                    curlocid += 1;
                }
            } else {
                evalTokenArrayBuffer.modelLocationArrayList.add(null);
            }
        }
    }

    @Override
    public void evaluate() {
        evaluate(evalTokenArrayBuffer);
    }
}
