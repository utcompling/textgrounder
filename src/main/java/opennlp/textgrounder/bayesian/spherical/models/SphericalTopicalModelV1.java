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

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class SphericalTopicalModelV1 extends SphericalModelBase {

    /**
     *
     */
    protected double[] averagedWordByTopicCounts;
    /**
     * 
     */
    protected double[] averagedTopicCounts;
    /**
     *
     */
    protected double[] wordByTopicDirichlet;
    /**
     * 
     */
    protected double[][] toponymCoordinateWeights;

    public SphericalTopicalModelV1(ExperimentParameters _parameters) {
        super(_parameters);
    }

    protected void baseSpecificInitialize() {
        topicVector = regionVector;

        dishByRestaurantCounts = new int[D * L];
        Arrays.fill(dishByRestaurantCounts, 0);

        regionCountsOfAllWords = new int[L];
        Arrays.fill(regionCountsOfAllWords, 0);
        globalDishCounts = regionCountsOfAllWords;

        wordByDishCounts = new int[W * L];
        Arrays.fill(wordByDishCounts, 0);
    }

    /**
     * Randomly initialize fields for training. If word is a toponym, choose
     * random region only from regions aligned to name.
     */
    @Override
    public void randomInitialize() {
        baseSpecificInitialize();

        /**
         * Sampling initial global
         */
        {
            double[] v = new double[L];
            double[] ivl = new double[L];
            double[] ilvl = new double[L];

            for (int l = 0; l < L - 1; ++l) {
                v[l] = RKRand.rk_beta(1, alpha_H);
            }

            v[L] = 1;
            for (int i = 0; i < L; ++i) {
                ilvl[i] = Math.log(1 - v[i]);
            }
            ivl = TGMath.cumSum(ilvl);

            globalDishWeights[0] = v[0];
            for (int l = 1; l < L; ++l) {
                globalDishWeights[l] = Math.exp(Math.log(v[l]) + ivl[l - 1]);
            }
        }

        {
            for (int d = 0; d < D; ++d) {
                for (int l = 0; l < L; ++l) {
                }
            }
        }

        int wordid, docid, regionid, topicid;
        int istoponym, isstopword;
        int wordoff, docoff;
        double[] regionProbs = new double[L];
        double[] topicProbs = new double[L];
        double totalprob, max, r;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            istoponym = toponymVector[i];
            if (isstopword == 0 && istoponym == 1) {
                wordid = wordVector[i];
                docid = documentVector[i];
                docoff = docid * L;
                wordoff = wordid * L;

                totalprob = 0;
                for (int j = 0; j < L; ++j) {
                    totalprob += regionProbs[j] = 1;
                }

                r = rand.nextDouble() * totalprob;

                max = regionProbs[0];
                regionid = 0;
                while (r > max) {
                    regionid++;
                    max += regionProbs[regionid];
                }

                regionVector[i] = regionid;
                dishCountsOfToponyms[regionid]++;
                regionByDocumentCounts[docoff + regionid]++;

                int coordinates = toponymCoordinateLexicon[wordid].length;
                int coordid = rand.nextInt(coordinates);
                dishToponymCoordinateCounts[regionid][wordid][coordid] += 1;
                coordinateVector[i] = coordid;
            }
        }

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            istoponym = toponymVector[i];
            if (isstopword == 0 && istoponym == 0) {
                wordid = wordVector[i];
                docid = documentVector[i];
                docoff = docid * L;
                wordoff = wordid * L;

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
                globalDishCounts[topicid]++;
                dishByRestaurantCounts[docoff + topicid]++;
                wordByDishCounts[wordoff + topicid]++;
            }
        }

        for (int i = 0; i < L; ++i) {
            int[][] toponymCoordinateCounts = dishToponymCoordinateCounts[i];
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

        for (int i = L; i < L; ++i) {
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
        int wordid, docid, dishid, coordid;
        int wordoff, docoff, regoff;
        int istoponym, isstopword;
        int curCoordCount;
        double[][] curCoords;
        double[] regionProbs = new double[L * maxCoord];
        double[] dishProbs = new double[L];
        double[] regionmean;
        double totalprob = 0, max, r;

        while (_annealer.nextIter()) {
            for (int i = 0; i < N; ++i) {
                isstopword = stopwordVector[i];
                istoponym = toponymVector[i];
                if (isstopword == 0) {
                    if (istoponym == 1) {
                        Arrays.fill(regionProbs, 0);

                        wordid = wordVector[i];
                        docid = documentVector[i];
                        dishid = regionVector[i];
                        coordid = coordinateVector[i];
                        docoff = docid * L;
                        wordoff = wordid * L;

                        globalDishCounts[dishid]--;
                        dishCountsOfToponyms[dishid]--;
                        dishByRestaurantCounts[docoff + dishid]--;
                        dishToponymCoordinateCounts[dishid][wordid][coordid]--;

                        curCoordCount = toponymCoordinateLexicon[wordid].length;
                        curCoords = toponymCoordinateLexicon[wordid];

                        double[] coordinateWeights = toponymCoordinateWeights[wordid];

                        for (int j = 0; j < L; ++j) {
                            regoff = j * maxCoord;
                            regionmean = regionMeans[j];
                            double ldw = localDishWeights[docoff + j];
                            for (int k = 0; k < curCoordCount; ++k) {
                                regionProbs[regoff + k] =
                                      ldw * coordinateWeights[k]
                                      * TGMath.sphericalDensity(curCoords[k], regionmean, kappa[k]);
                            }
                        }

                        totalprob = annealer.annealProbs(L, curCoordCount, maxCoord, regionProbs);

                        r = rand.nextDouble() * totalprob;

                        max = regionProbs[0];
                        coordid = 0;
                        while (r > max) {
                            coordid++;
                            if (coordid == curCoordCount) {
                                dishid++;
                                coordid = 0;
                            }
                            max += regionProbs[dishid * maxCoord + coordid];
                        }
                        regionVector[i] = dishid;
                        coordinateVector[i] = coordid;

                        dishCountsOfToponyms[dishid]++;
                        dishByRestaurantCounts[docoff + dishid]++;
                        dishToponymCoordinateCounts[dishid][wordid][coordid]++;
                        globalDishCounts[dishid]++;
                    } else {
                        wordid = wordVector[i];
                        docid = documentVector[i];
                        dishid = topicVector[i];
                        istoponym = toponymVector[i];
                        docoff = docid * L;
                        wordoff = wordid * L;

                        globalDishCounts[dishid]--;
                        dishByRestaurantCounts[docoff + dishid]--;
                        wordByDishCounts[wordoff + dishid]--;

                        try {
                            for (int j = 0;; ++j) {
                                dishProbs[j] = localDishWeights[docoff + j] * wordByTopicDirichlet[wordoff + j];
                            }
                        } catch (ArrayIndexOutOfBoundsException e) {
                        }

                        totalprob = _annealer.annealProbs(dishProbs);
                        r = rand.nextDouble() * totalprob;

                        max = dishProbs[0];
                        dishid = 0;
                        while (r > max) {
                            dishid++;
                            max += dishProbs[dishid];
                        }
                        topicVector[i] = dishid;

                        globalDishCounts[dishid]++;
                        dishByRestaurantCounts[docoff + dishid]++;
                        wordByDishCounts[wordoff + dishid]++;
                    }
                }
            }

            for (docid = 0; docid < D; ++docid) {
                docoff = docid * L;
                int l = 0;
                int t = 0;
                for (dishid = 0; dishid < L; ++dishid) {
                    l += t = (dishByRestaurantCounts[docoff + dishid] > 0 ? 1 : 0);
                    if (t == 0) {
                        break;
                    }
                }
                if (l == L) {
                    System.err.println("All tables were occupied in document " + docid);
                    System.exit(1);
                }
            }
        }
    }

    @Override
    public void decode() {
        SphericalAnnealer decoder = new SphericalMaximumPosteriorDecoder();
        int wordid, docid, regionid, topicid, coordid;
        int wordoff, docoff, regoff;
        int istoponym, isstopword;
        int curCoordCount;
        double[][] curCoords;
        double[] regionProbs = new double[L * maxCoord];
        double[] topicProbs = new double[L];
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
                    docoff = docid * L;
                    wordoff = wordid * L;

                    regionmean = regionMeans[regionid];
                    curCoordCount = toponymCoordinateLexicon[wordid].length;
                    curCoords = toponymCoordinateLexicon[wordid];

                    for (int j = 0; j < L; ++j) {
                        regoff = j * maxCoord;
                        regionmean = averagedRegionMeans[j];
                        double doccount = averagedRegionByDocumentCounts[docoff + j];
                        for (int k = 0; k < curCoordCount; ++k) {
                            regionProbs[regoff + k] =
                                  doccount
                                  * TGMath.unnormalizedProportionalSphericalDensity(curCoords[k], regionmean, kappa[k]);
                        }
                    }

                    totalprob = decoder.annealProbs(0, regionProbs);

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
                    docoff = docid * L;
                    wordoff = wordid * L;

                    try {
                        for (int j = 0;; ++j) {
                            topicProbs[j] = (averagedWordByTopicCounts[wordoff + j] + beta)
                                  / (averagedTopicCounts[j] + betaW)
                                  * (averagedTopicByDocumentCounts[docoff + j] + alpha_H);
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
            averagedTopicCounts = averagedRegionCountsOfAllWords;
            averagedTopicByDocumentCounts = annealer.getTopicByDocumentCounts();
        }
    }
}
