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
import opennlp.textgrounder.models.callbacks.*;
import opennlp.textgrounder.textstructs.*;
import opennlp.textgrounder.topostructs.*;

/**
 * 
 * @author tsmoon
 */
public class EvalRegionModel<E extends SmallLocation> extends RegionModel<E> {

    /**
     *
     */
    protected RegionModel rm;
    /**
     *
     */
    protected double[] hyperWordByTopicProbs;
    /**
     * 
     */
    protected double[] hyperTopicProbs;

    /**
     * 
     * @param rm
     */
    public EvalRegionModel(RegionModel rm) {
        this.rm = rm;
        try {
            initialize();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(EvalRegionModel.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(EvalRegionModel.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(EvalRegionModel.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(EvalRegionModel.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    protected void initialize() throws FileNotFoundException, IOException,
          ClassNotFoundException, SQLException {
        lexicon = new Lexicon();
        stopwordList = rm.stopwordList;
        evalInputFile = rm.evalInputFile;
        evalTokenArrayBuffer = new EvalTokenArrayBuffer(lexicon, new TrainingMaterialCallback(lexicon));
        processEvalInputPath(evalInputFile, new TextProcessorTR(lexicon, genericsKludgeFactor), evalTokenArrayBuffer, stopwordList);
        evalTokenArrayBuffer.convertToPrimitiveArrays();

        degreesPerRegion = rm.degreesPerRegion;
        initializeRegionArray();

        locationSet = new TIntHashSet();
        beta = rm.beta;
        alpha = rm.alpha;
        gazetteerGenerator = rm.gazetteerGenerator;
        regionMapperCallback = new RegionMapperCallback();
        regionArrayHeight = rm.regionArrayHeight;
        regionArrayWidth = rm.regionArrayWidth;
        setAllocateFields(evalTokenArrayBuffer);
        annealer = new EvalAnnealer();
        rand = rm.rand;
    }

    /**
     *
     */
    protected void setAllocateFields(TokenArrayBuffer evalTokenArrayBuffer) {
        N = evalTokenArrayBuffer.size();

        fW = lexicon.getDictionarySize();
        D = evalTokenArrayBuffer.getNumDocs();
        sW = stopwordList.size();
        W = fW - sW;

        wordVector = evalTokenArrayBuffer.wordVector;
        documentVector = evalTokenArrayBuffer.documentVector;
        toponymVector = evalTokenArrayBuffer.toponymVector;
        stopwordVector = evalTokenArrayBuffer.stopwordVector;

        /**
         * There is no need to initialize the topicVector. It will be randomly
         * initialized
         */
        topicVector = new int[N];

        TIntHashSet toponymsNotInGazetteer = null; //buildTopoTable();
        T = regionMapperCallback.getNumRegions();

        RegionModelBridge regionModelBridge = new RegionModelBridge(this, rm);
        int[] regionIdToRegionId = regionModelBridge.matchRegionId();
        int[] wordIdToWordId = regionModelBridge.matchWordId();

        topicByDocumentCounts = new int[D * T];
        for (int i = 0; i < D * T; ++i) {
            topicByDocumentCounts[i] = 0;
        }

        wordByTopicCounts = new int[fW * T];
        hyperWordByTopicProbs = new double[fW * T];
        double[] trainWordByTopicParams = rm.annealer.getWordByTopicSampleCounts();
        for (int i = 0; i < fW * T; ++i) {
            hyperWordByTopicProbs[i] = wordByTopicCounts[i] = 0;
        }
        for (int i = 0; i < fW; ++i) {
            int ewordoff = i * T;
            int twordoff = wordIdToWordId[i] * rm.T;
            if (twordoff < 0) {
                for (int j = 0; j < T; ++j) {
                    hyperWordByTopicProbs[ewordoff + j] = beta;
                }
            } else {
                for (int j = 0; j < T; ++j) {
                    int tregid = regionIdToRegionId[j];
                    if (tregid < 0) {
                        hyperWordByTopicProbs[ewordoff + j] = beta;
                    } else {
                        hyperWordByTopicProbs[ewordoff + j] = trainWordByTopicParams[twordoff + tregid] + beta;
                    }
                }
            }
        }

        topicCounts = new int[T];
        hyperTopicProbs = new double[T];
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

        regionByToponymFilter = new int[fW * T];
        for (int i = 0; i < fW * T; ++i) {
            regionByToponymFilter[i] = 0;
        }

//        buildTopoFilter(toponymsNotInGazetteer);
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
                wordid = wordVector[i];
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
                                  * regionByToponymFilter[wordoff + j];
                        }
                    } else {
                        for (int j = 0;; ++j) {
                            probs[j] = (wordByTopicCounts[wordoff + j] + hyperWordByTopicProbs[wordoff + j])
                                  / (topicCounts[j] + hyperTopicProbs[j])
                                  * (topicByDocumentCounts[docoff + j] + alpha);
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
                    wordid = wordVector[i];
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
                                      * regionByToponymFilter[wordoff + j];
                            }
                        } else {
                            for (int j = 0;; ++j) {
                                probs[j] = (wordByTopicCounts[wordoff + j] + hyperWordByTopicProbs[wordoff + j])
                                      / (topicCounts[j] + hyperTopicProbs[j])
                                      * (topicByDocumentCounts[docoff + j] + alpha);
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
        Gazetteer<E> gazetteer = gazetteerGenerator.generateGazetteer();
        for (TIntIterator it = locationSet.iterator(); it.hasNext();) {
            int locid = it.next();
            E loc = gazetteer.safeGetLocation(locid);
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
                        E curLocation = gazetteer.getLocation(curLocationIdx);
                        evalTokenArrayBuffer.modelLocationArrayList.add(curLocation);
                    } catch (NullPointerException e) {
                        locs = new TIntHashSet();
                        Region r = regionMapperCallback.getRegionMap().get(topicid);
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
        return gazetteer.getIdxToLocationMap();
    }

    @Override
    public void evaluate() {
        evaluate(evalTokenArrayBuffer);
    }
}
