///////////////////////////////////////////////////////////////////////////////
//  Copyright 2010 Taesun Moon <tsunmoon@gmail.com>.
// 
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  under the License.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.bayesian.spherical.models;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import opennlp.textgrounder.bayesian.apps.ExperimentParameters;
import opennlp.textgrounder.bayesian.mathutils.*;
import opennlp.textgrounder.bayesian.spherical.annealers.*;
import opennlp.textgrounder.bayesian.utils.TGArrays;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class SphericalModelV1 extends SphericalModelBase {

    /**
     * 
     */
    protected int[] topicCounts;
    /**
     *
     */
    protected int[] wordByTopicCounts;
    /**
     *
     */
    protected double[] averagedWordByTopicCounts;
    /**
     * 
     */
    protected double[] averagedTopicCounts;

    public SphericalModelV1(ExperimentParameters _parameters) {
        super(_parameters);
        emptyRSet = new HashSet<Integer>();
    }

    protected void baseSpecificInitialize() {
        topicVector = regionVector;

        topicByDocumentCounts = new int[D * Z];
        Arrays.fill(topicByDocumentCounts, 0);

        allWordsRegionCounts = new int[Z];
        Arrays.fill(allWordsRegionCounts, 0);
        topicCounts = allWordsRegionCounts;

        wordByRegionCounts = new int[W * Z];
        Arrays.fill(wordByRegionCounts, 0);
        wordByTopicCounts = wordByRegionCounts;
    }

    /**
     * Randomly initialize fields for training. If word is a toponym, choose
     * random region only from regions aligned to name.
     */
    @Override
    public void randomInitialize() {
        baseSpecificInitialize();

        currentR = 0;
        int wordid, docid, regionid, topicid;
        int istoponym, isstopword;
        int wordoff, docoff;
        double[] regionProbs = new double[expectedR];
        double[] topicProbs = new double[Z];
        double totalprob, max, r;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            istoponym = toponymVector[i];
            if (isstopword == 0 && istoponym == 1) {
                wordid = wordVector[i];
                docid = documentVector[i];
                docoff = docid * expectedR;
                wordoff = wordid * expectedR;

                totalprob = 0;
                for (int j = 0; j < currentR; ++j) {
                    totalprob += regionProbs[j] = 1;
                }

                r = rand.nextDouble() * totalprob + crpalpha;
                regionProbs[currentR] = crpalpha;

                max = regionProbs[0];
                regionid = 0;
                while (r > max) {
                    regionid++;
                    max += regionProbs[regionid];
                }
                if (regionid == currentR) {
                    currentR += 1;
                }

                regionVector[i] = regionid;
                toponymRegionCounts[regionid]++;
                regionByDocumentCounts[docoff + regionid]++;

                int coordinates = toponymCoordinateLexicon[wordid].length;
                int coordid = rand.nextInt(coordinates);
                regionToponymCoordinateCounts[regionid][wordid][coordid] += 1;
                coordinateVector[i] = coordid;
            }
        }

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            istoponym = toponymVector[i];
            if (isstopword == 0 && istoponym == 0) {
                wordid = wordVector[i];
                docid = documentVector[i];
                docoff = docid * Z;
                wordoff = wordid * Z;

                totalprob = 0;
                try {
                    for (int j = 0;; ++j) {
                        totalprob += topicProbs[j] = 1;
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                }

                r = rand.nextDouble() * totalprob;

                max = topicProbs[0];
                topicid = 0;
                while (r > max) {
                    topicid++;
                    max += topicProbs[topicid];
                }

                topicVector[i] = topicid;
                topicCounts[topicid]++;
                topicByDocumentCounts[docoff + topicid]++;
                wordByTopicCounts[wordoff + topicid]++;
            }
        }

        emptyRSet.add(currentR);

        for (int i = 0; i < currentR; ++i) {
            int[][] toponymCoordinateCounts = regionToponymCoordinateCounts[i];
            double[] mean = new double[3];
            Arrays.fill(mean, 0);

            for (int j = 0; j < T; ++j) {
                int[] coordcounts = toponymCoordinateCounts[j];
                double[][] coords = toponymCoordinateLexicon[j];
                for (int k = 0; k < coordcounts.length; ++k) {
                    int count = coordcounts[k];
                    if (count != 0) {
                        TGBLAS.daxpy(0, count, coords[k], 1, mean, 1);
                    }
                }
            }

            regionMeans[i] = mean;
        }

        for (int i = currentR; i < expectedR; ++i) {
            double[] mean = new double[3];
            Arrays.fill(mean, 0);
            regionMeans[i] = mean;
        }
    }

    /**
     * Train topics
     *
     * @param decoder Annealing scheme to use
     */
    @Override
    public void train(SphericalAnnealer _annealer) {
        int wordid, docid, regionid, topicid, coordid, emptyid = 0;
        int wordoff, docoff,regoff;
        int istoponym, isstopword;
        int curCoordCount;
        double[][] curCoords;
        double[] regionProbs = new double[expectedR * maxCoord];
        double[] topicProbs = new double[Z];
        double[] regionmean;
        double totalprob = 0, max, r;
        boolean addedEmptyR;

        while (_annealer.nextIter()) {
            for (int i = 0; i < N; ++i) {
                isstopword = stopwordVector[i];
                istoponym = toponymVector[i];
                addedEmptyR = false;
                if (isstopword == 0) {
                    if (istoponym == 1) {
                        wordid = wordVector[i];
                        docid = documentVector[i];
                        regionid = regionVector[i];
                        coordid = coordinateVector[i];
                        docoff = docid * expectedR;
                        wordoff = wordid * expectedR;

                        toponymRegionCounts[regionid]--;
                        regionByDocumentCounts[docoff + regionid]--;
                        regionToponymCoordinateCounts[regionid][wordid][coordid]--;
                        regionmean = regionMeans[regionid];
                        TGBLAS.daxpy(0, -1, toponymCoordinateLexicon[wordid][coordid], 1, regionmean, 1);
                        curCoordCount = toponymCoordinateLexicon[wordid].length;
                        curCoords = toponymCoordinateLexicon[wordid];

                        if (toponymRegionCounts[regionid] == 0) {
                            emptyRSet.add(regionid);
                            addedEmptyR = true;
                            emptyid = regionid;

                            regionmean = new double[3];
                            Arrays.fill(regionmean, 0);
                            regionMeans[regionid] = regionmean;
                        }

                        for (int j = 0; j < currentR; ++j) {
                            regoff = j * maxCoord;
                            if (emptyRSet.contains(j)) {
                                for (int k = 0; k < maxCoord; ++k) {
                                    regionProbs[regoff + k] = 0;
                                }
                            } else {
                                regionmean = regionMeans[j];
                                int doccount = regionByDocumentCounts[docoff + j];
                                for (int k = 0; k < curCoordCount; ++k) {
                                    regionProbs[regoff + k] =
                                          doccount
                                          * TGMath.unnormalizedProportionalSphericalDensity(curCoords[k], regionmean, kappa);
                                }
                                for (int k = curCoordCount; k < maxCoord; ++k) {
                                    regionProbs[regoff + k] = 0;
                                }
                            }
                        }

                        if (addedEmptyR) {
                            for (int j = 0; j < curCoordCount; ++j) {
                                regionProbs[emptyid * maxCoord + j] = crpalpha_mod / curCoordCount;
                            }
                        }

                        totalprob = annealer.annealProbs(0, currentR * maxCoord, regionProbs);

                        r = rand.nextDouble() * totalprob;

                        max = regionProbs[0];
                        regionid = 0;
                        coordid = 0;
                        while (r > max) {
                            coordid++;
                            if (coordid == curCoordCount) {
                                regionid++;
                                coordid = 0;
                            }
                            max += regionProbs[regionid * maxCoord + coordid];
                        }
                        regionVector[i] = regionid;
                        coordinateVector[i] = coordid;

                        toponymRegionCounts[regionid]++;
                        regionByDocumentCounts[docoff + regionid]++;
                        regionToponymCoordinateCounts[regionid][wordid][coordid]++;
                        regionmean = regionMeans[regionid];
                        TGBLAS.daxpy(0, 1, toponymCoordinateLexicon[wordid][coordid], 1, regionmean, 1);

                        if (emptyRSet.contains(regionid)) {
                            emptyRSet.remove(regionid);

                            if (emptyRSet.isEmpty()) {
                                currentR += 1;
                                emptyRSet.add(currentR);
                            }
                        }
                    } else {
                        wordid = wordVector[i];
                        docid = documentVector[i];
                        topicid = topicVector[i];
                        istoponym = toponymVector[i];
                        docoff = docid * Z;
                        wordoff = wordid * Z;

                        topicCounts[topicid]--;
                        topicByDocumentCounts[docoff + topicid]--;
                        wordByTopicCounts[wordoff + topicid]--;
                        try {
                            for (int j = 0;; ++j) {
                                topicProbs[j] = (wordByTopicCounts[wordoff + j] + beta)
                                      / (topicCounts[j] + betaW)
                                      * (topicByDocumentCounts[docoff + j] + alpha);
                            }
                        } catch (ArrayIndexOutOfBoundsException e) {
                        }

                        totalprob = _annealer.annealProbs(topicProbs);
                        r = rand.nextDouble() * totalprob;

                        max = topicProbs[0];
                        topicid = 0;
                        while (r > max) {
                            topicid++;
                            max += topicProbs[topicid];
                        }
                        topicVector[i] = topicid;

                        topicCounts[topicid]++;
                        topicByDocumentCounts[docoff + topicid]++;
                        wordByTopicCounts[wordoff + topicid]++;
                    }
                }
            }

            _annealer.collectSamples(wordByTopicCounts, regionByDocumentCounts, topicByDocumentCounts,
                  topicCounts, regionMeans, regionToponymCoordinateCounts);

            if (expectedR - currentR < EXPANSION_FACTOR / (1 + EXPANSION_FACTOR) * expectedR) {
                expandExpectedR();
                regionProbs = new double[expectedR * maxCoord];
            }
        }
    }

    @Override
    protected void expandExpectedR() {
        int newExpectedR = (int) Math.ceil(expectedR * (1 + EXPANSION_FACTOR));

        toponymRegionCounts = TGArrays.expandSingleTierC(toponymRegionCounts, newExpectedR, expectedR);
        regionByDocumentCounts = TGArrays.expandDoubleTierC(regionByDocumentCounts, D, newExpectedR, expectedR);

        regionMeans = TGArrays.expandSingleTierR(regionMeans, newExpectedR, currentR, coordParamLen);

        int[][][] newRegionToponymCoordinateCounts = new int[newExpectedR][][];
        for (int i = 0; i < expectedR; ++i) {
            int[][] toponymCoordinateCounts = new int[T][];
            for (int j = 0; j < T; ++j) {
                int coordinates = toponymCoordinateLexicon[j].length;
                int[] coordcounts = new int[coordinates];
                for (int k = 0; k < coordinates; ++k) {
                    coordcounts[k] = regionToponymCoordinateCounts[i][j][k];
                }
                toponymCoordinateCounts[j] = coordcounts;
            }
            newRegionToponymCoordinateCounts[i] = toponymCoordinateCounts;
        }

        for (int i = expectedR; i < newExpectedR; ++i) {
            int[][] toponymCoordinateCounts = new int[T][];
            for (int j = 0; j < T; ++j) {
                int coordinates = toponymCoordinateLexicon[j].length;
                int[] coordcounts = new int[coordinates];
                for (int k = 0; k < coordinates; ++k) {
                    coordcounts[k] = 0;
                }
                toponymCoordinateCounts[j] = coordcounts;
            }
            newRegionToponymCoordinateCounts[i] = toponymCoordinateCounts;
        }
        regionToponymCoordinateCounts = newRegionToponymCoordinateCounts;

        double[] sampleRegionByDocumentCounts = annealer.getRegionByDocumentCounts();
        sampleRegionByDocumentCounts = TGArrays.expandDoubleTierC(sampleRegionByDocumentCounts, D, newExpectedR, expectedR);
        annealer.setRegionByDocumentCounts(sampleRegionByDocumentCounts);

        double[] sampleWordByRegionCounts = annealer.getWordByRegionCounts();
        sampleWordByRegionCounts = TGArrays.expandDoubleTierC(sampleWordByRegionCounts, W, newExpectedR, expectedR);
        annealer.setWordByRegionCounts(sampleWordByRegionCounts);

        double[][][] sampleRegionToponymCoordinateCounts = annealer.getRegionToponymCoordinateCounts();
        double[][][] newSampleRegionToponymCoordinateCounts = new double[newExpectedR][][];
        for (int i = 0; i < expectedR; ++i) {
            newSampleRegionToponymCoordinateCounts[i] = sampleRegionToponymCoordinateCounts[i];
        }

        for (int i = expectedR; i < newExpectedR; ++i) {
            double[][] toponymCoordinateCounts = new double[T][];
            for (int j = 0; j < T; ++j) {
                int coordinates = toponymCoordinateLexicon[j].length;
                double[] coordcounts = new double[coordinates];
                for (int k = 0; k < coordinates; ++k) {
                    coordcounts[k] = 0;
                }
                toponymCoordinateCounts[j] = coordcounts;
            }
            newSampleRegionToponymCoordinateCounts[i] = toponymCoordinateCounts;
        }
        annealer.setRegionToponymCoordinateCounts(sampleRegionToponymCoordinateCounts);

        double[][] sampleRegionMeans = annealer.getRegionMeans();
        sampleRegionMeans = TGArrays.expandSingleTierR(sampleRegionMeans, newExpectedR, currentR, coordParamLen);
        annealer.setRegionMeans(sampleRegionMeans);

        expectedR = newExpectedR;
    }

    @Override
    public void decode() {
        SphericalAnnealer decoder = new SphericalMaximumPosteriorDecoder();
        int wordid, docid, regionid, topicid, coordid;
        int wordoff, docoff, regoff;
        int istoponym, isstopword;
        int curCoordCount;
        double[][] curCoords;
        double[] regionProbs = new double[expectedR * maxCoord];
        double[] topicProbs = new double[Z];
        double[] regionmean;
        double totalprob = 0, max, r;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            istoponym = toponymVector[i];
            if (isstopword == 0) {
                if (istoponym == 1) {
                    wordid = wordVector[i];
                    docid = documentVector[i];
                    regionid = regionVector[i];
                    coordid = coordinateVector[i];
                    docoff = docid * expectedR;
                    wordoff = wordid * expectedR;

                    regionmean = regionMeans[regionid];
                    curCoordCount = toponymCoordinateLexicon[wordid].length;
                    curCoords = toponymCoordinateLexicon[wordid];

                    for (int j = 0; j < currentR; ++j) {
                        regoff = j * maxCoord;
                        if (emptyRSet.contains(j)) {
                            for (int k = 0; k < maxCoord; ++k) {
                                regionProbs[regoff + k] = 0;
                            }
                        } else {

                            regionmean = averagedRegionMeans[j];
                            double doccount = averagedRegionByDocumentCounts[docoff + j];
                            for (int k = 0; k < curCoordCount; ++k) {
                                regionProbs[regoff + k] =
                                      doccount
                                      * TGMath.unnormalizedProportionalSphericalDensity(curCoords[k], regionmean, kappa);
                            }
                            for (int k = curCoordCount; k < maxCoord; ++k) {
                                regionProbs[regoff + k] = 0;
                            }
                        }
                    }

                    totalprob = decoder.annealProbs(0, currentR * maxCoord, regionProbs);

                    r = rand.nextDouble() * totalprob;

                    max = regionProbs[0];
                    regionid = 0;
                    coordid = 0;
                    while (r > max) {
                        coordid++;
                        if (coordid == curCoordCount) {
                            regionid++;
                            coordid = 0;
                        }
                        max += regionProbs[regionid * maxCoord + coordid];
                    }

                    regionVector[i] = regionid;
                    coordinateVector[i] = coordid;

                } else {
                    wordid = wordVector[i];
                    docid = documentVector[i];
                    topicid = topicVector[i];
                    istoponym = toponymVector[i];
                    docoff = docid * Z;
                    wordoff = wordid * Z;

                    try {
                        for (int j = 0;; ++j) {
                            topicProbs[j] = (averagedWordByTopicCounts[wordoff + j] + beta)
                                  / (averagedTopicCounts[j] + betaW)
                                  * (averagedTopicByDocumentCounts[docoff + j] + alpha);
                        }
                    } catch (ArrayIndexOutOfBoundsException e) {
                    }

                    totalprob = decoder.annealProbs(0, topicProbs);

                    r = rand.nextDouble() * totalprob;
                    max = topicProbs[0];
                    topicid = 0;
                    while (r > max) {
                        topicid++;
                        max += topicProbs[topicid];
                    }

                    topicVector[i] = topicid;
                }
            }
        }
    }

    /**
     *
     */
    @Override
    public void train() {
        super.train();
        if (annealer.getSamples() != 0) {
            averagedWordByTopicCounts = averagedWordByRegionCounts;
            averagedTopicCounts = averagedAllWordsRegionCounts;
            averagedTopicByDocumentCounts = annealer.getTopicByDocumentCounts();
        }
    }
}
