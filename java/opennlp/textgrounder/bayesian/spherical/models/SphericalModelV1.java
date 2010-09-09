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
import opennlp.textgrounder.bayesian.apps.ExperimentParameters;
import opennlp.textgrounder.bayesian.mathutils.*;
import opennlp.textgrounder.bayesian.spherical.annealers.*;
import opennlp.textgrounder.bayesian.utils.TGArrays;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class SphericalModelV1 extends SphericalModelBase {

    public SphericalModelV1(ExperimentParameters _parameters) {
        super(_parameters);
    }

    protected void baseSpecificInitialize() {
        topicVector = new int[N];
        Arrays.fill(topicVector, 0);

        topicByDocumentCounts = new int[D * Z];
        Arrays.fill(topicByDocumentCounts, 0);

        allWordsRegionCounts = new int[Z];
        Arrays.fill(allWordsRegionCounts, 0);
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
                wordByRegionCounts[wordoff + regionid]++;

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
                allWordsRegionCounts[topicid]++;
                topicByDocumentCounts[docoff + topicid]++;
                wordByRegionCounts[wordoff + topicid]++;
            }
        }

        emptyRSet.add(currentR);

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
    }

    /**
     * Train topics
     *
     * @param decoder Annealing scheme to use
     */
    @Override
    public void train(SphericalAnnealer _annealer) {
        int wordid, docid, regionid, coordid;
        int wordoff, docoff;
        int istoponym, isstopword;
        int curCoordCount;
        double[][] curCoords;
        double[] regionProbs = new double[expectedR * maxCoord];
        double[] topicProbs = new double[Z];
        double[] regionmean;
        double totalprob = 0, max, r;

        while (_annealer.nextIter()) {
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

                        toponymRegionCounts[regionid]--;
                        regionByDocumentCounts[docoff + regionid]--;
                        wordByRegionCounts[wordoff + regionid]--;
                        regionToponymCoordinateCounts[regionid][wordid][coordid]--;
                        regionmean = regionMeans[regionid];
                        TGBLAS.daxpy(0, -1, toponymCoordinateLexicon[wordid][coordid], 1, regionmean, 1);
                        curCoordCount = toponymCoordinateLexicon[wordid].length;
                        curCoords = toponymCoordinateLexicon[wordid];

                        if (toponymRegionCounts[regionid] == 0) {
                            emptyRSet.add(regionid);
                        }

                        for (int j = 0; j < currentR; ++j) {
                            regionmean = regionMeans[j];
                            int doccount = regionByDocumentCounts[docoff + j];
                            for (int k = 0; k < curCoordCount; ++k) {
                                regionProbs[j * maxCoord + k] =
                                      doccount
                                      * TGMath.unnormalizedProportionalSphericalDensity(curCoords[k], regionmean, kappa);
                            }
                        }

                        for (int emptyR : emptyRSet) {
                            for (int j = 0; j < curCoordCount; ++j) {
                                regionProbs[emptyR * maxCoord + j] = crpalpha_mod / curCoordCount;
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
                        wordByRegionCounts[wordoff + regionid]++;
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
                        regionid = regionVector[i];
                        istoponym = toponymVector[i];
                        docoff = docid * Z;
                        wordoff = wordid * Z;

                        allWordsRegionCounts[regionid]--;
                        topicByDocumentCounts[docoff + regionid]--;
                        wordByRegionCounts[wordoff + regionid]--;
                        try {
                            for (int j = 0;; ++j) {
                                topicProbs[j] = (wordByRegionCounts[wordoff + j] + beta)
                                      / (allWordsRegionCounts[j] + betaW)
                                      * (topicByDocumentCounts[docoff + j] + alpha);
                            }
                        } catch (ArrayIndexOutOfBoundsException e) {
                        }

                        totalprob = _annealer.annealProbs(topicProbs);
                        r = rand.nextDouble() * totalprob;

                        max = topicProbs[0];
                        regionid = 0;
                        while (r > max) {
                            regionid++;
                            max += topicProbs[regionid];
                        }
                        regionVector[i] = regionid;

                        allWordsRegionCounts[regionid]++;
                        topicByDocumentCounts[docoff + regionid]++;
                        wordByRegionCounts[wordoff + regionid]++;
                    }
                }
            }

            _annealer.collectSamples(wordByRegionCounts, regionByDocumentCounts, topicByDocumentCounts,
                  allWordsRegionCounts, regionMeans,/*toponymByRegionCounts, nonToponymRegionCounts, */
                  regionToponymCoordinateCounts);

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
        allWordsRegionCounts = TGArrays.expandSingleTierC(allWordsRegionCounts, newExpectedR, expectedR);
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
        int wordid, docid, regionid, coordid;
        int wordoff, docoff;
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
                        regionmean = averagedRegionMeans[j];
                        double doccount = averagedRegionByDocumentCounts[docoff + j];
                        for (int k = 0; k < curCoordCount; ++k) {
                            regionProbs[j * maxCoord + k] =
                                  doccount
                                  * TGMath.unnormalizedProportionalSphericalDensity(curCoords[k], regionmean, kappa);
                        }
                    }

                    totalprob = decoder.annealProbs(0, expectedR * maxCoord, regionProbs);

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
                    regionid = regionVector[i];
                    istoponym = toponymVector[i];
                    docoff = docid * Z;
                    wordoff = wordid * Z;

                    try {
                        for (int j = 0;; ++j) {
                            topicProbs[j] = (averagedWordByRegionCounts[wordoff + j] + beta)
                                  / (allWordsRegionCounts[j] + betaW)
                                  * (averagedTopicByDocumentCounts[docoff + j] + alpha);
                        }
                    } catch (ArrayIndexOutOfBoundsException e) {
                    }

                    totalprob = decoder.annealProbs(0, topicProbs);

                    r = rand.nextDouble() * totalprob;
                    max = regionProbs[0];
                    regionid = 0;
                    while (r > max) {
                        regionid++;
                        max += regionProbs[regionid];
                    }

                    regionVector[i] = regionid;
                }
            }
        }
    }
}
