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
    protected int[] globalDishCounts;
    /**
     *
     */
    protected int[] wordByDishCounts;
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
    protected double[] globalDishWeights;
    /**
     *
     */
    protected double[] localDishWeights;
    /**
     *
     */
    protected double[] wordByTopicDirichlet;
    /**
     * 
     */
    protected double[][] toponymCoordinateWeights;
    /**
     * 
     */
    protected double[] Kappa;

    public SphericalTopicalModelV1(ExperimentParameters _parameters) {
        super(_parameters);
    }

    protected void baseSpecificInitialize() {
        topicVector = regionVector;

        dishByRestaurantCounts = new int[D * Z];
        Arrays.fill(dishByRestaurantCounts, 0);

        regionCountsOfAllWords = new int[Z];
        Arrays.fill(regionCountsOfAllWords, 0);
        globalDishCounts = regionCountsOfAllWords;

        wordByDishCounts = new int[W * Z];
        Arrays.fill(wordByDishCounts, 0);
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
        double[] regionProbs = new double[L];
        double[] topicProbs = new double[Z];
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
                globalDishCounts[topicid]++;
                dishByRestaurantCounts[docoff + topicid]++;
                wordByDishCounts[wordoff + topicid]++;
            }
        }

        for (int i = 0; i < currentR; ++i) {
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

        for (int i = currentR; i < L; ++i) {
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
        int wordid, docid, regionid, dishid, coordid;
        int wordoff, docoff, regoff;
        int istoponym, isstopword;
        int curCoordCount;
        double[][] curCoords;
        double[] regionProbs = new double[L * maxCoord];
        double[] dishProbs = new double[Z];
        double[] regionmean;
        double kp;
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
                        docoff = docid * L;
                        wordoff = wordid * L;

                        dishCountsOfToponyms[regionid]--;
                        dishByRestaurantCounts[docoff + regionid]--;
                        dishToponymCoordinateCounts[regionid][wordid][coordid]--;

                        curCoordCount = toponymCoordinateLexicon[wordid].length;
                        curCoords = toponymCoordinateLexicon[wordid];

                        double[] coordinateWeights = toponymCoordinateWeights[wordid];

                        for (int j = 0; j < currentR; ++j) {
                            regoff = j * maxCoord;
                            regionmean = regionMeans[j];
                            kp = Kappa[j];
                            double ldw = localDishWeights[docoff + j];
                            for (int k = 0; k < curCoordCount; ++k) {
                                regionProbs[regoff + k] =
                                      ldw * coordinateWeights[k]
                                      *  TGMath.sphericalDensity(curCoords[k], regionmean, kp);
                            }
                        }

                        totalprob = annealer.annealProbs(currentR, curCoordCount, maxCoord, regionProbs);

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

                        dishCountsOfToponyms[regionid]++;
                        dishByRestaurantCounts[docoff + regionid]++;
                        dishToponymCoordinateCounts[regionid][wordid][coordid]++;

                    } else {
                        wordid = wordVector[i];
                        docid = documentVector[i];
                        dishid = topicVector[i];
                        istoponym = toponymVector[i];
                        docoff = docid * Z;
                        wordoff = wordid * Z;

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
                    docoff = docid * L;
                    wordoff = wordid * L;

                    regionmean = regionMeans[regionid];
                    curCoordCount = toponymCoordinateLexicon[wordid].length;
                    curCoords = toponymCoordinateLexicon[wordid];

                    for (int j = 0; j < currentR; ++j) {
                        regoff = j * maxCoord;
                        if (emptyRSet.contains(j)) {
                            for (int k = 0; k < curCoordCount; ++k) {
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
                        }
                    }

                    totalprob = decoder.annealProbs(currentR, curCoordCount, maxCoord, regionProbs);

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
            averagedTopicCounts = averagedRegionCountsOfAllWords;
            averagedTopicByDocumentCounts = annealer.getTopicByDocumentCounts();
        }
    }
}
