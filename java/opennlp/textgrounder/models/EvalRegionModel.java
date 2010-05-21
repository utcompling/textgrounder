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
 * Topic model with region awareness. Toponyms are all unigrams. Multiword
 * toponyms are split into space delimited tokens.
 * 
 * @author tsmoon
 */
public class EvalRegionModel extends RegionModel {

    /**
     * Default constructor. Take input from commandline and default options
     * and initialize class. Also, process input text and process so that
     * toponyms, stopwords and other words are identified and collected.
     *
     * @param options
     */
    public EvalRegionModel(CommandLineOptions options) {
        regionMapperCallback = new RegionMapperCallback();
        try {
            initialize(options);
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

    /**
     * 
     */
    public EvalRegionModel() {
    }

    /**
     *
     * @param options
     */
    @Override
    protected void initialize(CommandLineOptions options) throws
          FileNotFoundException, IOException, ClassNotFoundException,
          SQLException {
        super.initialize(options);
        initializeFromOptions(options);
        trainTokenArrayBuffer = new TokenArrayBuffer(lexicon, new TrainingMaterialCallback(lexicon));
        stopwordList = new StopwordList();
        processTrainInputPath(trainInputFile, textProcessor, trainTokenArrayBuffer, stopwordList);
        trainTokenArrayBuffer.convertToPrimitiveArrays();
        initializeRegionArray();
        locationSet = new TIntHashSet();
        setAllocateRegions(trainTokenArrayBuffer);
    }

    /**
     *
     */
    protected void setAllocateRegions(EvalTokenArrayBuffer evalTokenArrayBuffer) {
        N = evalTokenArrayBuffer.size();
        /**
         * Here we distinguish between the full dictionary size (fW) and the
         * dictionary size without stopwords (W). Normalization is conducted with
         * the dictionary size without stopwords, the size of which is sW.
         */
        sW = stopwordList.size();
        fW = lexicon.getDictionarySize();
        W = fW - sW;
        D = evalTokenArrayBuffer.getNumDocs();
        betaW = beta * W;

        wordVector = evalTokenArrayBuffer.wordVector;
        documentVector = evalTokenArrayBuffer.documentVector;
        toponymVector = evalTokenArrayBuffer.toponymVector;
        stopwordVector = evalTokenArrayBuffer.stopwordVector;

        /**
         * There is no need to initialize the topicVector. It will be randomly
         * initialized
         */
        topicVector = new int[N];

        System.err.println();
        System.err.print("Buildng lookup tables for locations, regions and toponyms for document: ");
        int curDoc = 0, prevDoc = -1;

        TIntHashSet toponymsNotInGazetteer = new TIntHashSet();
        for (int i = 0; i < N; i++) {
            curDoc = documentVector[i];
            if (curDoc != prevDoc) {
                System.err.print(curDoc + ",");
            }
            prevDoc = curDoc;
            if (toponymVector[i] == 1) {
                String placename = lexicon.getWordForInt(wordVector[i]);
                if (gazetteer.contains(placename)) {
                    TIntHashSet possibleLocations = gazetteer.get(placename);

                    TIntHashSet tempLocs = new TIntHashSet();
                    for (TIntIterator it = possibleLocations.iterator();
                          it.hasNext();) {
                        int locid = it.next();
                        Location loc = gazetteer.getLocation(locid);

                        if (Math.abs(loc.coord.latitude) > Constants.EPSILON && Math.abs(loc.coord.longitude) > Constants.EPSILON) {
                            tempLocs.add(loc.id);
                        }
                    }
                    possibleLocations = tempLocs;

                    addLocationsToRegionArray(possibleLocations, gazetteer, regionMapperCallback);
                    regionMapperCallback.addAll(placename, lexicon);
                    locationSet.addAll(possibleLocations.toArray());
                } else {
                    toponymsNotInGazetteer.add(wordVector[i]);
                }
            }
        }
        System.err.println();

        T = regionMapperCallback.getNumRegions();
        topicCounts = new double[T];
        for (int i = 0; i < T; ++i) {
            topicCounts[i] = 0;
        }
        topicByDocumentCounts = new int[D * T];
        for (int i = 0; i < D * T; ++i) {
            topicByDocumentCounts[i] = 0;
        }
        wordByTopicCounts = new double[fW * T];
        for (int i = 0; i < fW * T; ++i) {
            wordByTopicCounts[i] = 0;
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
        int wordoff;
        double[] probs = new double[T];
        double totalprob, max, r;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            if (isstopword == 0) {
                wordid = wordVector[i];
                docid = documentVector[i];
                istoponym = toponymVector[i];

                if (istoponym == 1) {
                    wordoff = wordid * T;
                    totalprob = 0;
                    try {
                        for (int j = 0;; ++j) {
                            totalprob += probs[j] = regionByToponym[wordoff + j];
                        }
                    } catch (ArrayIndexOutOfBoundsException e) {
                    }
                    r = rand.nextDouble() * totalprob;

                    max = probs[0];
                    topicid = 0;
                    while (r > max) {
                        topicid++;
                        max += probs[topicid];
                    }

                } else {
                    topicid = rand.nextInt(T);
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
