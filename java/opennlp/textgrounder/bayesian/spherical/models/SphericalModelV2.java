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
import java.util.Iterator;
import opennlp.textgrounder.bayesian.apps.ExperimentParameters;
import opennlp.textgrounder.bayesian.mathutils.*;
import opennlp.textgrounder.bayesian.spherical.annealers.*;
import opennlp.textgrounder.bayesian.utils.TGArrays;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class SphericalModelV2 extends SphericalModelBase {

    /**
     * Default constructor. Take input from commandline and default _options
     * and initialize class. Also, process input text and process so that
     * toponyms, stopwords and other words are identified and collected.
     *
     * @param _options
     */
    public SphericalModelV2(ExperimentParameters _parameters) {
        super(_parameters);
    }

    /**
     * Randomly initialize fields for training. If word is a toponym, choose
     * random region only from regions aligned to name.
     */
    @Override
    public void randomInitialize() {
        currentR = 0;
        int wordid, docid, regionid;
        int istoponym, isstopword;
        int wordoff, docoff;
        double[] probs = new double[expectedR];
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
                    totalprob += probs[j] = 1;
                }

                r = rand.nextDouble() * totalprob + crpalpha;
                probs[currentR] = crpalpha;

                max = probs[0];
                regionid = 0;
                while (r > max) {
                    regionid++;
                    max += probs[regionid];
                }
                if (regionid == currentR) {
                    currentR += 1;
                }

                regionVector[i] = regionid;
                regionCountsOfToponyms[regionid]++;
                regionCountsOfAllWords[regionid] = regionCountsOfToponyms[regionid];
                regionByDocumentCounts[docoff + regionid]++;
                wordByRegionCounts[wordoff + regionid]++;

//                toponymByRegionCounts[wordoff + regionid]++;
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
                docoff = docid * expectedR;
                wordoff = wordid * expectedR;

                totalprob = 0;
                for (int j = 0; j < currentR; ++j) {
                    totalprob += probs[j] = 1;
                }

                r = rand.nextDouble() * totalprob;

                max = probs[0];
                regionid = 0;
                while (r > max) {
                    regionid++;
                    max += probs[regionid];
                }

                regionVector[i] = regionid;
                regionCountsOfAllWords[regionid]++;
                regionByDocumentCounts[docoff + regionid]++;
                wordByRegionCounts[wordoff + regionid]++;
            }
        }

        for (int i = 0; i < currentR; ++i) {
            int[][] toponymCoordinateCounts = regionToponymCoordinateCounts[i];
            double[] mean = new double[3];
            for (int j = 0; j < 3; ++j) {
                mean[j] = 0;
            }

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
        int wordid, docid, regionid, coordid, emptyid = 0;
        int wordoff, docoff, regoff;
        int istoponym, isstopword;
        int curCoordCount;
        double[][] curCoords;
        double[] probs = new double[expectedR * maxCoord];
        double[] regionmean;
        double totalprob = 0, max, r;
        boolean addedEmptyR = false;

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

                        regionCountsOfToponyms[regionid]--;
                        regionCountsOfAllWords[regionid]--;
                        regionByDocumentCounts[docoff + regionid]--;
                        wordByRegionCounts[wordoff + regionid]--;
                        regionToponymCoordinateCounts[regionid][wordid][coordid]--;
                        regionmean = regionMeans[regionid];
                        TGBLAS.daxpy(0, -1, toponymCoordinateLexicon[wordid][coordid], 1, regionmean, 1);
                        curCoordCount = toponymCoordinateLexicon[wordid].length;
                        curCoords = toponymCoordinateLexicon[wordid];

                        if (regionCountsOfToponyms[regionid] == 0) {
                            emptyRSet.add(regionid);
                            addedEmptyR = true;
                            emptyid = regionid;

                            regionmean = new double[3];
                            Arrays.fill(regionmean, 0);
                            regionMeans[regionid] = regionmean;
                            resetRegionID(annealer, regionid, docid);
                        } else {
                            if (emptyRSet.isEmpty()) {
                                emptyid = currentR;
                            } else {
                                Iterator<Integer> it = emptyRSet.iterator();
                                emptyid = it.next();
                            }
                        }

                        for (int j = 0; j < currentR; ++j) {
                            regoff = j * maxCoord;
                            if (emptyRSet.contains(j)) {
                                for (int k = 0; k < curCoordCount; ++k) {
                                    probs[regoff + k] = 0;
                                }
                            } else {
                                regionmean = regionMeans[j];
                                int doccount = regionByDocumentCounts[docoff + j];
                                for (int k = 0; k < curCoordCount; ++k) {
                                    probs[regoff + k] =
                                          doccount
                                          * TGMath.unnormalizedProportionalSphericalDensity(curCoords[k], regionmean, kappa);
                                }
//                                for (int k = curCoordCount; k < maxCoord; ++k) {
//                                    probs[regoff + k] = 0;
//                                }
                            }
                        }

                        for (int j = 0; j < curCoordCount; ++j) {
                            probs[emptyid * maxCoord + j] = crpalpha_mod / curCoordCount;
                        }
//                        for (int j = curCoordCount; j < maxCoord; ++j) {
//                            probs[emptyid * maxCoord + j] = 0;
//                        }

                        if (emptyid == currentR) {
                            totalprob = annealer.annealProbs((currentR + 1), curCoordCount, maxCoord, probs);
                        } else {
                            totalprob = annealer.annealProbs(currentR, curCoordCount, maxCoord, probs);
                        }

                        r = rand.nextDouble() * totalprob;

                        max = probs[0];
                        regionid = 0;
                        coordid = 0;
                        while (r > max) {
                            coordid++;
                            if (coordid == curCoordCount) {
                                regionid++;
                                coordid = 0;
                            }
                            max += probs[regionid * maxCoord + coordid];
                        }
                        regionVector[i] = regionid;
                        coordinateVector[i] = coordid;

                        regionCountsOfToponyms[regionid]++;
                        regionCountsOfAllWords[regionid]++;
                        regionByDocumentCounts[docoff + regionid]++;
                        wordByRegionCounts[wordoff + regionid]++;
                        regionToponymCoordinateCounts[regionid][wordid][coordid]++;
                        regionmean = regionMeans[regionid];
                        TGBLAS.daxpy(0, 1, toponymCoordinateLexicon[wordid][coordid], 1, regionmean, 1);

                        if (emptyRSet.contains(regionid)) {
                            emptyRSet.remove(regionid);
                        } else if (regionid == currentR) {
                            currentR += 1;
                        }
                        while (emptyRSet.contains(currentR - 1)) {
                            currentR -= 1;
                            emptyRSet.remove(currentR);
                        }
                    } else {
                        wordid = wordVector[i];
                        docid = documentVector[i];
                        regionid = regionVector[i];
                        istoponym = toponymVector[i];
                        docoff = docid * expectedR;
                        wordoff = wordid * expectedR;

                        regionCountsOfAllWords[regionid]--;
                        regionByDocumentCounts[docoff + regionid]--;
                        wordByRegionCounts[wordoff + regionid]--;

                        for (int j = 0; j < currentR; ++j) {
                            probs[j] = (wordByRegionCounts[wordoff + j] + beta)
                                  / (regionCountsOfAllWords[j] + betaW)
                                  * regionByDocumentCounts[docoff + j];
                        }

                        totalprob = _annealer.annealProbs(0, currentR, probs);
                        r = rand.nextDouble() * totalprob;

                        max = probs[0];
                        regionid = 0;
                        while (r > max) {
                            regionid++;
                            max += probs[regionid];
                        }
                        regionVector[i] = regionid;

                        regionCountsOfAllWords[regionid]++;
                        regionByDocumentCounts[docoff + regionid]++;
                        wordByRegionCounts[wordoff + regionid]++;
                    }
                }
                if (expectedR - currentR < EXPANSION_FACTOR / (1 + EXPANSION_FACTOR) * expectedR) {
                    expandExpectedR();
                    probs = new double[expectedR * maxCoord];
                }
            }

            _annealer.collectSamples(wordByRegionCounts, regionByDocumentCounts,
                  regionCountsOfAllWords, regionMeans,/*toponymByRegionCounts, nonToponymRegionCounts, */
                  regionToponymCoordinateCounts);

        }
    }

    @Override
    protected void expandExpectedR() {
        int newExpectedR = (int) Math.ceil(expectedR * (1 + EXPANSION_FACTOR));
        System.err.print(String.format("(Expand Reg#:%d->%d,Current Reg#:%d)", expectedR, newExpectedR, currentR));

        regionCountsOfToponyms = TGArrays.expandSingleTierC(regionCountsOfToponyms, newExpectedR, expectedR);
        regionCountsOfAllWords = TGArrays.expandSingleTierC(regionCountsOfAllWords, newExpectedR, expectedR);
        regionByDocumentCounts = TGArrays.expandDoubleTierC(regionByDocumentCounts, D, newExpectedR, expectedR);
        wordByRegionCounts = TGArrays.expandDoubleTierC(wordByRegionCounts, W, newExpectedR, expectedR);

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
        if (sampleRegionByDocumentCounts != null) {
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
        }

        expectedR = newExpectedR;
    }

    @Override
    public void decode() {
        SphericalAnnealer decoder = new SphericalMaximumPosteriorDecoder();
        int wordid, docid, regionid, coordid;
        int wordoff, docoff, regoff;
        int istoponym, isstopword;
        int curCoordCount;
        double[][] curCoords;
        double[] probs = new double[expectedR * maxCoord];
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
                                probs[regoff + k] = 0;
                            }
                        } else {
                            regionmean = averagedRegionMeans[j];
                            double doccount = averagedRegionByDocumentCounts[docoff + j];
                            for (int k = 0; k < curCoordCount; ++k) {
                                probs[regoff + k] =
                                      doccount
                                      * TGMath.unnormalizedProportionalSphericalDensity(curCoords[k], regionmean, kappa);
                            }
                            for (int k = curCoordCount; k < maxCoord; ++k) {
                                probs[regoff + k] = 0;
                            }
                        }
                    }

                    totalprob = decoder.annealProbs(currentR, curCoordCount, maxCoord, probs);

                    r = rand.nextDouble() * totalprob;

                    max = probs[0];
                    regionid = 0;
                    coordid = 0;
                    while (r > max) {
                        coordid++;
                        if (coordid == curCoordCount) {
                            regionid++;
                            coordid = 0;
                        }
                        max += probs[regionid * maxCoord + coordid];
                    }

                    regionVector[i] = regionid;
                    coordinateVector[i] = coordid;

                } else {
                    wordid = wordVector[i];
                    docid = documentVector[i];
                    regionid = regionVector[i];
                    istoponym = toponymVector[i];
                    docoff = docid * expectedR;
                    wordoff = wordid * expectedR;

                    for (int j = 0; j < currentR; ++j) {
                        probs[j] = (averagedWordByRegionCounts[wordoff + j] + beta)
                              / (regionCountsOfAllWords[j] + betaW)
                              * (averagedRegionByDocumentCounts[docoff + j] + alpha);
                    }

                    totalprob = decoder.annealProbs(0, currentR, probs);

                    r = rand.nextDouble() * totalprob;
                    max = probs[0];
                    regionid = 0;
                    while (r > max) {
                        regionid++;
                        max += probs[regionid];
                    }

                    regionVector[i] = regionid;
                }
            }
        }
    }
}
