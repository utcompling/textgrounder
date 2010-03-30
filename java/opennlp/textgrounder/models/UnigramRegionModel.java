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

import java.util.logging.Level;
import java.util.logging.Logger;
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

    protected UnigramRegionModel() {
    }

    public UnigramRegionModel(CommandLineOptions options) {
        regionMapperCallback = new UnigramRegionMapperCallback();
        initialize(options);
    }

    protected void initialize(CommandLineOptions options) {

        initializeFromOptions(options);

        BaselineModel bm = null;
        try {
            bm = new BaselineModel(options);
            bm.processPath();
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        try {
            bm.processPath();
        } catch (Exception ex) {
            Logger.getLogger(UnigramRegionModel.class.getName()).log(Level.SEVERE, null, ex);
        }
        bm.initializeRegionArray();

        this.gazetteer = bm.gazetteer;
        this.gazCache = bm.gazCache;
        this.pairListSet = bm.pairListSet;

        setAllocateRegions();
    }

    /**
     *
     */
    protected void setAllocateRegions() {
        ArrayList<Integer> wordVectorT = new ArrayList<Integer>(),
              docVectorT = new ArrayList<Integer>(),
              toponymVectorT = new ArrayList<Integer>();

        for (int docIndex = 0; docIndex < docSet.size(); docIndex++) {
            ArrayList<Integer> curDoc = docSet.get(docIndex);
            ArrayList<ToponymSpan> curDocSpans = pairListSet.get(docIndex);
            int topSpanIndex = 0;
            ToponymSpan curTopSpan = curDocSpans.get(topSpanIndex);
            for (int wordIndex = 0; wordIndex < curDoc.size(); wordIndex++) {

                if (wordIndex != curTopSpan.begin) {
                    wordVectorT.add(curDoc.get(wordIndex));
                    toponymVectorT.add(0);
                } else {
                    String placename = getPlacenameString(curTopSpan, docIndex).toLowerCase();

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
                    regionMapperCallback.addPlacenameTokens(placename, docSet, wordVectorT, toponymVectorT);

                    wordIndex = curTopSpan.end;
                    topSpanIndex += 1;
                }
                docVectorT.add(docIndex);
            }
        }

        N = regionMapperCallback.getNumRegions();
        W = docSet.getDictionarySize();
        T = activeRegions;
        D = docSet.size();
        betaW = beta * W;

        wordVector = new int[N];
        copyToArray(wordVector, wordVectorT);
        documentVector = new int[N];
        copyToArray(documentVector, docVectorT);
        toponymVector = new int[N];
        copyToArray(toponymVector, toponymVectorT);

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

        Hashtable<String, HashSet<Integer>> nameToRegionIndex = regionMapperCallback.getNameToRegionIndex();
        for (String placename : nameToRegionIndex.keySet()) {
            int wordoff = docSet.getIntForWord(placename) * T;
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

    public void decode() {
        Annealer ann = new MaximumPosteriorDecoder();
        train(ann);
    }

    /**
     * Copy a sequence of numbers from ta to array ia.
     *
     * @param <T>   Any number type
     * @param ia    Target array of integers to be copied to
     * @param ta    Source List<T> of numbers to be copied from
     */
    protected static <T extends Number> void copyToArray(int[] ia, List<T> ta) {
        for (int i = 0; i < ta.size(); ++i) {
            ia[i] = ta.get(i).intValue();
        }
    }
}
