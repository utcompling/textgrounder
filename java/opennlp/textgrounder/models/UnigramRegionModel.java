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

import opennlp.textgrounder.io.DocumentSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;

import opennlp.textgrounder.annealers.*;
import opennlp.textgrounder.geo.*;
import opennlp.textgrounder.models.callbacks.*;
import opennlp.textgrounder.topostructs.*;
import opennlp.textgrounder.ners.*;

/**
 * Topic model with region awareness. Toponyms are all unigrams. Multiword
 * toponyms are split into space delimited tokens.
 * 
 * @author tsmoon
 */
public class UnigramRegionModel extends TopicModel {

    /**
     * Callback class for handling mappings between regions, locations and placenames
     */
    protected RegionMapperCallback regionMapperCallback;
    /**
     * Table from index to region
     */
    protected Hashtable<Integer, Region> regionMap;
    /**
     * Table from region to index. Reverse storage table for regionMap.
     */
    protected Hashtable<Region, Integer> reverseRegionMap;
    /**
     * Table from placename to set of region indexes. The indexes and their
     * referents are stored in regionMap.
     */
    protected Hashtable<String, HashSet<Integer>> nameToRegionIndex;
    /**
     * Vector of toponyms. If 0, the word is not a toponym. If 1, it is.
     */
    protected int[] toponymVector;
    /**
     * An index of toponyms and possible regions. The goal is fast lookup and not
     * frugality with memory. The dimensions are equivalent to the wordByTopicCounts
     * array. Instead of counts, this array is populated with ones and zeros.
     * If a toponym occurs in a certain region, the cell value is one, zero if not.
     */
    protected int[] regionByToponym;

    public UnigramRegionModel(CommandLineOptions options) {
        regionMapperCallback = new UnigramRegionMapperCallback();
        initialize(options, regionMapperCallback);
    }

    public UnigramRegionModel(CommandLineOptions options,
          RegionMapperCallback regionMapperCallback) {
        initialize(options, regionMapperCallback);
    }

    protected void initialize(CommandLineOptions options,
          RegionMapperCallback regionMapperCallback) {
        BaselineModel bm = null;
        try {
            bm = new BaselineModel(options);
            bm.processPath();
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }

        this.gazetteer = bm.gazetteer;
        this.gazCache = bm.gazCache;
        this.degreesPerRegion = bm.degreesPerRegion;
        this.pairListSet = bm.pairListSet;

        regionMap = new Hashtable<Integer, Region>();
        reverseRegionMap = new Hashtable<Region, Integer>();
        nameToRegionIndex = new Hashtable<String, HashSet<Integer>>();

        regionMapperCallback.setMaps(regionMap, reverseRegionMap, nameToRegionIndex);
        this.regionMapperCallback = regionMapperCallback;

        T = 0;
        setAllocateRegions();
    }

    /**
     * Allocate memory for fields
     *
     * @param docSet Container holding training data.
     * @param T Number of regions
     */
    @Override
    protected void allocateFields(DocumentSet docSet, int T) {
        N = 0;
        W = docSet.wordsToInts.size();
        D = docSet.size();
        betaW = beta * W;
        for (int i = 0; i < docSet.size(); i++) {
            N += docSet.get(i).size();
        }

        documentVector = new int[N];
        wordVector = new int[N];
        topicVector = new int[N];
        toponymVector = new int[N];

        for (int i = 0; i < N; ++i) {
            toponymVector[i] = documentVector[i] = wordVector[i] = topicVector[i] = 0;
        }

        int tcount = 0, docs = 0;
        for (ArrayList<Integer> doc : docSet) {
            int offset = 0;
            for (int idx : doc) {
                wordVector[tcount + offset] = idx;
                documentVector[tcount + offset] = docs;
                offset += 1;
            }
            tcount += doc.size();
            docs += 1;
        }

    }

    /**
     *
     * @param pairListSet
     */
    protected void setAllocateRegions() {
        int docoff = 0;
        for (int docIndex = 0; docIndex < docSet.size(); docIndex++) {
            ArrayList<ToponymSpan> curDocSpans = pairListSet.get(docIndex);

            for (int topSpanIndex = 0; topSpanIndex < curDocSpans.size();
                  topSpanIndex++) {
                ToponymSpan curTopSpan = curDocSpans.get(topSpanIndex);
                String placename = getPlacenameString(curTopSpan, docIndex).toLowerCase();

                for (int i = curTopSpan.begin; i < curTopSpan.end; ++i) {
                    toponymVector[docoff + i] = 1;
                }

                if (!gazetteer.contains(placename)) // quick lookup to see if it has even 1 place by that name
                {
                    continue;
                }

                // try the cache first. if not in there, do a full DB lookup and add that pair to the cache:
                List<Location> possibleLocations = gazCache.get(placename);
                if (possibleLocations == null) {
                    try {
                        possibleLocations = gazetteer.get(placename);
                    } catch (Exception ex) {
                    }
                }
                regionMapperCallback.setCurrentRegion(placename);
                addLocationsToRegionArray(possibleLocations, regionMapperCallback);
            }
            docoff += docSet.get(docIndex).size();
        }

        /**
         * Confirm and fill in dictionary if placenames do not exist
         */
        for (String placename : nameToRegionIndex.keySet()) {
            regionMapperCallback.confirmPlacenameTokens(placename, docSet);
        }

        W = docSet.getDictionarySize();
        T = activeRegions;

        topicCounts = new int[T];
        for (int i = 0; i < T; ++i) {
            topicCounts[i] = 0;
        }
        topicByDocumentCounts = new int[D * T];
        for (int i = 0; i < D * T; ++i) {
            topicByDocumentCounts[i] = 0;
        }
        wordByTopicCounts = new int[W * T];
        for (int i = 0; i < W * T; ++i) {
            wordByTopicCounts[i] = 0;
        }
        regionByToponym = new int[W * T];
        for (int i = 0; i < W * T; ++i) {
            regionByToponym[i] = 0;
        }

        /**
         * Could potentially blow up here if a token in a placename does not
         * exist in the dictionary
         */
        for (String placename : nameToRegionIndex.keySet()) {
            String[] names = placename.split(" ");
            for (String name : names) {
                if (!docSet.hasWord(name)) {
                    docSet.addWord(name);
                }
                int wordoff = docSet.getIntForWord(name) * T;
                for (int j : nameToRegionIndex.get(placename)) {
                    regionByToponym[wordoff + j] = 1;
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
        int istop;
        int wordoff;
        double[] probs = new double[T];
        double totalprob, max, r;

        for (int i = 0; i < N; ++i) {
            wordid = wordVector[i];
            docid = documentVector[i];
            istop = toponymVector[i];

            if (istop == 1) {
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

    /**
     * Train topics
     *
     * @param annealer Annealing scheme to use
     */
    @Override
    public void train(Annealer annealer) {
        int wordid, docid, topicid;
        int wordoff, docoff;
        int istop;
        double[] probs = new double[T];
        double totalprob, max, r;

        while (annealer.nextIter()) {
            for (int i = 0; i < N; ++i) {
                wordid = wordVector[i];
                docid = documentVector[i];
                topicid = topicVector[i];
                istop = toponymVector[i];
                docoff = docid * T;
                wordoff = wordid * T;

                topicCounts[topicid]--;
                topicByDocumentCounts[docoff + topicid]--;
                wordByTopicCounts[wordoff + topicid]--;

                try {
                    if (istop == 1) {
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
