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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import opennlp.textgrounder.annealers.*;
import opennlp.textgrounder.gazetteers.Gazetteer;
import opennlp.textgrounder.geo.CommandLineOptions;
import opennlp.textgrounder.models.callbacks.*;
import opennlp.textgrounder.textstructs.*;
import opennlp.textgrounder.topostructs.*;

/**
 * 
 * @author tsmoon
 */
public class NonRandomStartRegionModel<E extends SmallLocation> extends EvalRegionModel<E> {

    public NonRandomStartRegionModel(CommandLineOptions _options, E _genericsKludgeFactor) {
        super(_options, _genericsKludgeFactor);
    }

    protected void initialize() throws FileNotFoundException, IOException,
          ClassNotFoundException, SQLException {

        regionMapperCallback = new RegionMapperCallback();

        lexicon = trainRegionModel.lexicon;
        stopwordList = trainRegionModel.stopwordList;
        evalTokenArrayBuffer = trainRegionModel.evalTokenArrayBuffer;
        dataSpecificLocationMap = trainRegionModel.dataSpecificLocationMap;
        dataSpecificGazetteer = trainRegionModel.dataSpecificGazetteer;

        locationSet = new TIntHashSet();

        beta = trainRegionModel.beta;
        alpha = trainRegionModel.alpha;

        degreesPerRegion = trainRegionModel.degreesPerRegion;
        initializeRegionArray();
        regionArrayHeight = trainRegionModel.regionArrayHeight;
        regionArrayWidth = trainRegionModel.regionArrayWidth;

        setAllocateRegions(evalTokenArrayBuffer);

        annealer = new EvalAnnealer();
        rand = trainRegionModel.rand;
    }

    /**
     *
     */
    @Override
    protected void setAllocateRegions(TokenArrayBuffer _tokenArrayBuffer) {

        super.setAllocateRegions(_tokenArrayBuffer);

        regionIdMapper = new int[trainRegionModel.T];

        for (int w = 0; w < regionArrayWidth; w++) {
            for (int h = 0; h < regionArrayHeight; h++) {
                Region fromRegion = trainRegionModel.regionArray[w][h];
                Region toRegion = regionArray[w][h];
                if (fromRegion != null) {
                    int fromIdx = trainRegionModel.regionMapperCallback.getRegionToIdxMap().get(fromRegion);
                    if (toRegion != null) {
                        int toIdx = regionMapperCallback.getRegionToIdxMap().get(toRegion);
                        regionIdMapper[fromIdx] = toIdx;
                    } else {
                        regionIdMapper[fromIdx] = -1;
                    }
                }
            }
        }

        topicByDocumentCounts = new int[D * T];
        for (int i = 0; i < D * T; ++i) {
            topicByDocumentCounts[i] = 0;
        }

        wordByTopicCounts = new int[fW * T];
        hyperWordByTopicProbs = new float[fW * T];
        float[] trainWordByTopicParams = trainRegionModel.annealer.getWordByTopicSampleCounts();
        for (int i = 0; i < fW * T; ++i) {
            wordByTopicCounts[i] = 0;
            hyperWordByTopicProbs[i] = (float) beta;
        }

        for (int i = 0; i < wordIdMapper.length; ++i) {
            int evalwordid = wordIdMapper[i];
            if (evalwordid > -1) {
                int trainwordid = trainRegionModel.wordIdMapper[i];
                if (trainwordid > -1) {
                    int evalwordoff = evalwordid * T;
                    int trainwordoff = trainwordid * trainRegionModel.T;
                    for (int j = 0; j < trainRegionModel.T; ++j) {
                        int evalregionid = regionIdMapper[j];
                        if (evalregionid > -1) {
                            hyperWordByTopicProbs[evalwordoff + evalregionid] += trainWordByTopicParams[trainwordoff + j];
                        }
                    }
                }
            }
        }

        topicCounts = new int[T];
        hyperTopicProbs = new float[T];
        for (int i = 0; i < T; ++i) {
            hyperTopicProbs[i] = topicCounts[i] = 0;
        }
        for (int i = 0; i < fW; ++i) {
            if (!stopwordList.isStopWord(lexicon.getWordForInt(i))) {
                int wordoff = i * T;
                for (int j = 0; j < T; ++j) {
                    hyperTopicProbs[j] += hyperWordByTopicProbs[wordoff + j];
                }
            }
        }
    }

    /**
     * Randomly initialize fields for training. If word is a toponym, choose
     * random region only from regions aligned to name.
     */
    @Override
    public void randomInitialize() {
        int wordid, docid, topicid;
        int istoponym, isstopword;
        int docoff, wordoff;
        double[] probs = new double[T];
        double totalprob, max, r;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            if (isstopword == 0) {
                wordid = wordIdMapper[wordVector[i]];
                docid = documentVector[i];
                istoponym = toponymVector[i];
                docoff = docid * T;
                wordoff = wordid * T;

                try {
                    if (istoponym == 1) {
                        for (int j = 0;; ++j) {
                            probs[j] = (wordByTopicCounts[wordoff + j] + hyperWordByTopicProbs[wordoff + j])
                                  / (topicCounts[j] + hyperTopicProbs[j])
                                  * (topicByDocumentCounts[docoff + j] + alpha)
                                  * regionByToponymFilter[wordoff + j]
                                  * activeRegionByDocumentFilter[docoff + j];
                        }
                    } else {
                        for (int j = 0;; ++j) {
                            probs[j] = (wordByTopicCounts[wordoff + j] + hyperWordByTopicProbs[wordoff + j])
                                  / (topicCounts[j] + hyperTopicProbs[j])
                                  * (topicByDocumentCounts[docoff + j] + alpha)
                                  * activeRegionByDocumentFilter[docoff + j];
                        }
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                }
                totalprob = annealer.annealProbs(probs);
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

    /**
     * Train topics
     *
     * @param annealer Annealing scheme to use
     */
    @Override
    public void train(Annealer annealer) {
        int wordid, docid, topicid;
        int wordoff, docoff;
        int istoponym, isstopword;
        double[] probs = new double[T];
        double totalprob, max, r;

        while (annealer.nextIter()) {
            for (int i = 0; i < N; ++i) {
                isstopword = stopwordVector[i];
                if (isstopword == 0) {
                    wordid = wordIdMapper[wordVector[i]];
                    docid = documentVector[i];
                    topicid = topicVector[i];
                    istoponym = toponymVector[i];
                    docoff = docid * T;
                    wordoff = wordid * T;

                    topicCounts[topicid]--;
                    topicByDocumentCounts[docoff + topicid]--;
                    wordByTopicCounts[wordoff + topicid]--;

                    try {
                        if (istoponym == 1) {
                            for (int j = 0;; ++j) {
                                probs[j] = (wordByTopicCounts[wordoff + j] + hyperWordByTopicProbs[wordoff + j])
                                      / (topicCounts[j] + hyperTopicProbs[j])
                                      * (topicByDocumentCounts[docoff + j] + alpha)
                                      * regionByToponymFilter[wordoff + j]
                                      * activeRegionByDocumentFilter[docoff + j];
                            }
                        } else {
                            for (int j = 0;; ++j) {
                                probs[j] = (wordByTopicCounts[wordoff + j] + hyperWordByTopicProbs[wordoff + j])
                                      / (topicCounts[j] + hyperTopicProbs[j])
                                      * (topicByDocumentCounts[docoff + j] + alpha)
                                      * activeRegionByDocumentFilter[docoff + j];
                            }
                        }
                    } catch (ArrayIndexOutOfBoundsException e) {
                    }
                    totalprob = annealer.annealProbs(probs);
                    r = rand.nextDouble() * totalprob;

                    max = probs[0];
                    topicid = 0;
                    while (r > max) {
                        topicid++;
                        max += probs[topicid];
                    }
                    topicVector[i] = topicid;

                    topicCounts[topicid]++;
                    topicByDocumentCounts[docoff + topicid]++;
                    wordByTopicCounts[wordoff + topicid]++;
                }
            }

            annealer.collectSamples(topicCounts, wordByTopicCounts);
        }
    }

    /**
     *
     */
    @Override
    protected TIntObjectHashMap<E> normalizeLocations() {
        for (TIntIterator it = locationSet.iterator(); it.hasNext();) {
            int locid = it.next();
            E loc = dataSpecificLocationMap.get(locid);
            loc.setCount(loc.getCount() + beta);
        }

        TIntObjectHashMap<TIntHashSet> toponymRegionToLocations = regionMapperCallback.getToponymRegionToLocations();

        int wordid, topicid;
        int istoponym, isstopword;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            boolean added = false;
            if (isstopword == 0) {
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
                        evalTokenArrayBuffer.modelLocationArrayList.add(curLocation);
                    } catch (NullPointerException e) {
                        locs = new TIntHashSet();
                        Region r = regionMapperCallback.getIdxToRegionMap().get(topicid);
                        Coordinate coord = new Coordinate(r.centLon, r.centLat);
                        E loc = generateLocation(-1, lexicon.getWordForInt(wordid), null, coord, 0, null, 1, wordid);
                        evalTokenArrayBuffer.modelLocationArrayList.add(loc);
                        locs.add(loc.getId());
                        toponymRegionToLocations.put(trp.hashCode(), locs);
                    }
                    added = true;
                }
            }
            if (!added) {
                evalTokenArrayBuffer.modelLocationArrayList.add(null);
            }
        }
        return dataSpecificLocationMap;
    }

    @Override
    public void evaluate() {
        evaluate(evalTokenArrayBuffer);
    }
}
