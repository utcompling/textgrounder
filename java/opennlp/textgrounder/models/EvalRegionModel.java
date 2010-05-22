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
import gnu.trove.TIntObjectIterator;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import opennlp.textgrounder.annealers.*;
import opennlp.textgrounder.geo.*;
import opennlp.textgrounder.models.callbacks.*;
import opennlp.textgrounder.ners.NullClassifier;
import opennlp.textgrounder.textstructs.EvalTokenArrayBuffer;
import opennlp.textgrounder.textstructs.StopwordList;
import opennlp.textgrounder.textstructs.TextProcessor;
import opennlp.textgrounder.textstructs.TextProcessorTR;
import opennlp.textgrounder.textstructs.TokenArrayBuffer;
import opennlp.textgrounder.topostructs.*;
import opennlp.textgrounder.util.Constants;
import opennlp.textgrounder.util.KMLUtil;

/**
 * 
 * @author tsmoon
 */
public class EvalRegionModel extends RegionModel {

    /**
     *
     */
    RegionModel rm;

    /**
     * 
     * @param rm
     */
    public EvalRegionModel(RegionModel rm) {
        this.rm = rm;
    }

    protected void initialize() throws
          FileNotFoundException, IOException, ClassNotFoundException,
          SQLException {

        stopwordList = rm.stopwordList;
        evalTokenArrayBuffer = new EvalTokenArrayBuffer(lexicon, new TrainingMaterialCallback(lexicon));
        processEvalInputPath(trainInputFile, textProcessor, evalTokenArrayBuffer, stopwordList);
        evalTokenArrayBuffer.convertToPrimitiveArrays();
        regionArray = rm.regionArray;
        locationSet = new TIntHashSet();
        setAllocateRegions(evalTokenArrayBuffer);
    }

    /**
     *
     */
    @Override
    protected void setAllocateRegions(TokenArrayBuffer evalTokenArrayBuffer) {
        N = evalTokenArrayBuffer.size();

        fW = lexicon.getDictionarySize();
        D = evalTokenArrayBuffer.getNumDocs();

        wordVector = evalTokenArrayBuffer.wordVector;
        documentVector = evalTokenArrayBuffer.documentVector;
        toponymVector = evalTokenArrayBuffer.toponymVector;
        stopwordVector = evalTokenArrayBuffer.stopwordVector;

        TIntHashSet uniqueTokes = new TIntHashSet();
        for (int i = 0; i < N; ++i) {
            if (stopwordVector[i] == 0) {
                int wordidx = wordVector[i];
                uniqueTokes.add(wordidx);
            }
        }

        W = uniqueTokes.size();
        betaW = beta * W;
        uniqueTokes.clear();

        /**
         * There is no need to initialize the topicVector. It will be randomly
         * initialized
         */
        topicVector = new int[N];

        TIntHashSet toponymsNotInGazetteer = buildTopoTable();

        T = regionMapperCallback.getNumRegions();
        topicCounts = new double[T];
        for (int i = 0; i < T; ++i) {
            topicCounts[i] = 0;
        }
        for (int i = 0; i < rm.topicCounts.length; ++i) {
            topicCounts[i] = rm.topicCounts[i];
        }

        topicByDocumentCounts = new int[D * T];
        for (int i = 0; i < D * T; ++i) {
            topicByDocumentCounts[i] = 0;
        }
        wordByTopicCounts = new double[fW * T];
        for (int i = 0; i < fW * T; ++i) {
            wordByTopicCounts[i] = 0;
        }
        for (int i = 0; i < rm.fW; ++i) {
            int ewordoff = i * T, twordoff = i * rm.T;
            for (int j = 0; j < rm.fW; ++j) {
                wordByTopicCounts[ewordoff + j] = rm.wordByTopicCounts[twordoff + j];
            }
        }

        regionByToponym = new int[fW * T];
        for (int i = 0; i < fW * T; ++i) {
            regionByToponym[i] = 0;
        }

        TIntObjectHashMap<TIntHashSet> nameToRegionIndex = regionMapperCallback.getNameToRegionIndex();
        for (TIntObjectIterator<TIntHashSet> it1 = nameToRegionIndex.iterator();
              it1.hasNext();) {
            it1.advance();
            int wordoff = it1.key() * T;
            for (TIntIterator it2 = it1.value().iterator(); it2.hasNext();) {
                int j = it2.next();
                regionByToponym[wordoff + j] = 1;
            }
        }

        for (TIntIterator it = toponymsNotInGazetteer.iterator(); it.hasNext();) {
            int topid = it.next();
            int topoff = topid * T;
            for (int i = 0; i < T; ++i) {
                regionByToponym[topoff + i] = 1;
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
                wordid = wordVector[i];
                docid = documentVector[i];
                istoponym = toponymVector[i];
                docoff = docid * T;
                wordoff = wordid * T;

                try {
                    if (istoponym == 1) {
                        for (int j = 0;; ++j) {
                            probs[j] = (wordByTopicCounts[wordoff + j] + beta)
                                  / (topicCounts[j] + betaW)
                                  * (topicByDocumentCounts[docoff + j] + alpha)
                                  * regionByToponym[wordoff + j];
                        }
                    } else {
                        for (int j = 0;; ++j) {
                            probs[j] = (wordByTopicCounts[wordoff + j] + beta)
                                  / (topicCounts[j] + betaW)
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
                                probs[j] = (wordByTopicCounts[wordoff + j] + beta)
                                      / (topicCounts[j] + betaW)
                                      * (topicByDocumentCounts[docoff + j] + alpha)
                                      * regionByToponym[wordoff + j];
                            }
                        } else {
                            for (int j = 0;; ++j) {
                                probs[j] = (wordByTopicCounts[wordoff + j] + beta)
                                      / (topicCounts[j] + betaW)
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
    protected void normalizeLocations() {
        for (TIntIterator it = locationSet.iterator(); it.hasNext();) {
            int locid = it.next();
            Location loc = gazetteer.getLocation(locid);
            loc.count += beta;
            loc.backPointers = new ArrayList<Integer>();
        }

        TIntObjectHashMap<TIntHashSet> nameToRegionIndex = regionMapperCallback.getNameToRegionIndex();
        TIntObjectHashMap<TIntHashSet> toponymRegionToLocations = regionMapperCallback.getToponymRegionToLocations();
        for (int wordid : nameToRegionIndex.keys()) {
            for (int regid : nameToRegionIndex.get(wordid).toArray()) {
                ToponymRegionPair trp = new ToponymRegionPair(wordid, regid);
                TIntHashSet locs = toponymRegionToLocations.get(trp.hashCode());
                for (TIntIterator it = locs.iterator(); it.hasNext();) {
                    int locid = it.next();
                    Location loc = gazetteer.getLocation(locid);
                    loc.count += wordByTopicCounts[wordid * T + regid];
                }
            }
        }

        int wordid, topicid;
        int istoponym, isstopword;

        int newlocid = gazetteer.getMaxLocId();
        int curlocid = newlocid + 1;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            if (isstopword == 0) {
                istoponym = toponymVector[i];
                if (istoponym == 1) {
                    wordid = wordVector[i];
                    topicid = topicVector[i];
                    ToponymRegionPair trp = new ToponymRegionPair(wordid, topicid);
                    TIntHashSet locs = toponymRegionToLocations.get(trp.hashCode());
                    try {
                        for (TIntIterator it = locs.iterator(); it.hasNext();) {
                            int locid = it.next();
                            Location loc = gazetteer.getLocation(locid);
                            loc.backPointers.add(i);
                            if (loc.id > newlocid) {
                                loc.count += 1;
                            }
                        }
                    } catch (NullPointerException e) {
                        locs = new TIntHashSet();
                        Region r = regionMapperCallback.getRegionMap().get(topicid);
                        Coordinate coord = new Coordinate(r.centLon, r.centLat);
                        Location loc = new Location(curlocid, lexicon.getWordForInt(wordid), null, coord, 0, null, 1);
                        gazetteer.putLocation(loc);
                        loc.backPointers = new ArrayList<Integer>();
                        loc.backPointers.add(i);
                        locs.add(loc.id);
                        toponymRegionToLocations.put(trp.hashCode(), locs);
                        curlocid += 1;
                    }
                }
            }
        }

        locations = new TIntHashSet();
        for (TIntObjectIterator<Location> it = gazetteer.getIdxToLocationMap().iterator();
              it.hasNext();) {
            it.advance();
            Location loc = it.value();
            try {
                if (loc.backPointers.size() != 0) {
                    locations.add(loc.id);
                }
            } catch (NullPointerException e) {
            }
        }
    }

    /**
     *
     */
    @Override
    public void evaluate() {

        evalTokenArrayBuffer = new EvalTokenArrayBuffer(lexicon);

        try {
            processEvalInputPath(evalInputFile, new TextProcessorTR(lexicon), evalTokenArrayBuffer, stopwordList);
        } catch (IOException ex) {
            Logger.getLogger(EvalRegionModel.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassCastException ex) {
            Logger.getLogger(EvalRegionModel.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(EvalRegionModel.class.getName()).log(Level.SEVERE, null, ex);
        }
        evalTokenArrayBuffer.convertToPrimitiveArrays();

        super.evaluate();
    }
}
