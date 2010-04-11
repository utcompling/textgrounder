///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
//
//  This library is free software; you can redistribute it and/or
//  modify it under the terms of the GNU Lesser General Public
//  License as published by the Free Software Foundation; either
//  version 3 of the License, or (at your option) any later version.
//
//  This library is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public
//  License along with this program; if not, write to the Free Software
//  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.models;

import opennlp.textgrounder.textstructs.StopwordList;
import opennlp.textgrounder.textstructs.TokenArrayBuffer;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import opennlp.textgrounder.annealers.*;
import opennlp.textgrounder.geo.*;
import opennlp.textgrounder.models.callbacks.*;
import opennlp.textgrounder.topostructs.*;
import opennlp.textgrounder.util.Constants;

/**
 * Topic model with region awareness. Toponyms are all unigrams. Multiword
 * toponyms are split into space delimited tokens.
 * 
 * @author tsmoon
 */
public class RegionModel extends TopicModel {

    /**
     * Callback class for handling mappings between regions, locations and placenames
     */
    protected RegionMapperCallback regionMapperCallback;
    /**
     * Vector of toponyms. If 0, the word is not a toponym. If 1, it is.
     */
    protected int[] toponymVector;
    /**
     * Vector of stopwords. If 0, the word is not a stopword. If 1, it is.
     */
    protected int[] stopwordVector;
    /**
     * An index of toponyms and possible regions. The goal is fast lookup and not
     * frugality with memory. The dimensions are equivalent to the wordByTopicCounts
     * array. Instead of counts, this array is populated with ones and zeros.
     * If a toponym occurs in a certain region, the cell value is one, zero if not.
     */
    protected int[] regionByToponym;
    /**
     * The set of locations that have been observed with the model.
     */
    protected HashSet<Location> locationSet;

    /**
     * Default constructor. Take input from commandline and default options
     * and initialize class. Also, process input text and process so that
     * toponyms, stopwords and other words are identified and collected.
     *
     * @param options
     */
    public RegionModel(CommandLineOptions options) {
        regionMapperCallback = new RegionMapperCallback();
        initialize(options);
    }

    /**
     *
     * @param options
     */
    protected void initialize(CommandLineOptions options) {

        initializeFromOptions(options);
        tokenArrayBuffer = new TokenArrayBuffer(null);
        BaselineModel baselineModel = null;

        try {
            baselineModel = new BaselineModel(options);
            StopwordList stopwordList = new StopwordList();
            sW = stopwordList.size();
            baselineModel.processPath(baselineModel.getInputFile(),
                  baselineModel.getTextProcessor(), tokenArrayBuffer,
                  stopwordList);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }

        baselineModel.initializeRegionArray();

        this.gazetteer = baselineModel.gazetteer;
        this.gazCache = baselineModel.gazCache;
        this.textProcessor = baselineModel.textProcessor;
        this.lexicon = baselineModel.lexicon;
        tokenArrayBuffer.setDocSet(this.lexicon);

        locationSet = new HashSet<Location>();

        setAllocateRegions(tokenArrayBuffer, baselineModel);
    }

    /**
     * 
     * @param options
     */
    @Override
    protected void initializeFromOptions(CommandLineOptions options) {
        super.initializeFromOptions(options);
        windowSize = options.getWindowSize();
        kmlOutputFilename = options.getKMLOutputFilename();
    }

    /**
     *
     */
    protected void setAllocateRegions(TokenArrayBuffer tokenArrayBuffer,
          BaselineModel baselineModel) {
        N = tokenArrayBuffer.size();
        /**
         * Here we distinguish between the full dictionary size (fW) and the
         * dictionary size without stopwords (W). Normalization is conducted with
         * the dictionary size without stopwords.
         */
        fW = lexicon.getDictionarySize();
        W = fW - sW;
        D = tokenArrayBuffer.getNumDocs();
        betaW = beta * W;

        wordVector = tokenArrayBuffer.wordVector;
        documentVector = tokenArrayBuffer.documentVector;
        toponymVector = tokenArrayBuffer.toponymVector;
        stopwordVector = tokenArrayBuffer.stopwordVector;

        /**
         * There is no need to initialize the topicVector. It will be randomly
         * initialized
         */
        topicVector = new int[N];

        System.err.println();
        System.err.print("Buildng lookup tables for locations, regions and toponyms for document: ");
        int curDoc = 0, prevDoc = -1;
        for (int i = 0; i < N; i++) {
            curDoc = documentVector[i];
            if (curDoc != prevDoc) {
                System.err.print(curDoc + ",");
            }
            prevDoc = curDoc;
            if (toponymVector[i] == 1) {
                String placename = lexicon.getWordForInt(wordVector[i]);
                if (gazetteer.contains(placename)) { // quick lookup to see if it has even 1 place by that name
                    // try the cache first. if not in there, do a full DB lookup and add that pair to the cache:
                    List<Location> possibleLocations = gazCache.get(placename);
                    if (possibleLocations == null) {
                        try {
                            possibleLocations = gazetteer.get(placename);
                            gazCache.put(placename, possibleLocations);
                        } catch (Exception ex) {
                        }
                    }

                    List<Location> tempLocs = new ArrayList<Location>();
                    for (Location loc : possibleLocations) {
                        if (Math.abs(loc.coord.latitude) > Constants.EPSILON && Math.abs(loc.coord.longitude) > Constants.EPSILON) {
                            tempLocs.add(loc);
                        }
                    }
                    possibleLocations = tempLocs;

                    baselineModel.addLocationsToRegionArray(possibleLocations, regionMapperCallback);
                    regionMapperCallback.addAll(placename, lexicon);
                    locationSet.addAll(possibleLocations);
                }
            }
        }
        System.err.println();

        T = regionMapperCallback.getNumRegions();
        topicCounts = new int[T];
        for (int i = 0; i < T; ++i) {
            topicCounts[i] = 0;
        }
        topicByDocumentCounts = new int[D * T];
        for (int i = 0; i < D * T; ++i) {
            topicByDocumentCounts[i] = 0;
        }
        wordByTopicCounts = new int[fW * T];
        for (int i = 0; i < fW * T; ++i) {
            wordByTopicCounts[i] = 0;
        }
        regionByToponym = new int[fW * T];
        for (int i = 0; i < fW * T; ++i) {
            regionByToponym[i] = 0;
        }

        Map<String, HashSet<Integer>> nameToRegionIndex = regionMapperCallback.getNameToRegionIndex();
        for (String placename : nameToRegionIndex.keySet()) {
            int wordoff = lexicon.getIntForWord(placename) * T;
            for (int j : nameToRegionIndex.get(placename)) {
                regionByToponym[wordoff + j] = 1;
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
        }
    }

    /**
     * Maximum posterior decoding of topic assignments. 
     */
    public void decode() {
        System.err.println(String.format("Decoding maximum posterior topics"));
        Annealer ann = new MaximumPosteriorDecoder();
        train(ann);
    }

    /**
     * 
     */
    protected void normalizeLocations() {
        Map<Integer, Location> locationIdToLocation = new HashMap<Integer, Location>();
        for (Location loc : locationSet) {
            loc.count += beta;
            loc.backPointers = new ArrayList<Integer>();
            locationIdToLocation.put(loc.id, loc);
        }

        Map<String, HashSet<Integer>> nameToRegionIndex = regionMapperCallback.getNameToRegionIndex();
        Map<ToponymRegionPair, HashSet<Location>> toponymRegionToLocations = regionMapperCallback.getToponymRegionToLocations();
        for (String name : nameToRegionIndex.keySet()) {
            int wordid = lexicon.getIntForWord(name);
            for (int regid : nameToRegionIndex.get(name)) {
                ToponymRegionPair trp = new ToponymRegionPair(wordid, regid);
                HashSet<Location> locs = toponymRegionToLocations.get(trp);
                for (Location loc : locs) {
                    Location tloc = locationIdToLocation.get(loc.id);
                    tloc.count += wordByTopicCounts[wordid * T + regid];
                }
            }
        }

        int wordid, topicid;
        int istoponym, isstopword;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            if (isstopword == 0) {
                istoponym = toponymVector[i];
                if (istoponym == 1) {
                    wordid = wordVector[i];
                    topicid = topicVector[i];
                    ToponymRegionPair trp = new ToponymRegionPair(wordid, topicid);
                    HashSet<Location> locs = toponymRegionToLocations.get(trp);
                    try {
                        for (Location loc : locs) {
                            Location tloc = locationIdToLocation.get(loc.id);
                            tloc.backPointers.add(i);
                        }
                    } catch (NullPointerException e) {
                    }
                }
            }
        }

        locations = new ArrayList<Location>();
        for (Location loc : locationIdToLocation.values()) {
            if (loc.backPointers.size() != 0) {
                locations.add(loc);
            }
//            locations = new ArrayList<Location>(locationIdToLocation.values());
        }
    }

    /**
     * Write assignments to Google Earth KML file.
     * 
     * @throws Exception
     */
    @Override
    public void writeXMLFile() throws Exception {
        System.err.println();
        System.err.println("Counting locations and smoothing");
        normalizeLocations();
        System.err.println("Writing output");
        writeXMLFile(inputPath, kmlOutputFilename, locations);
    }
}
