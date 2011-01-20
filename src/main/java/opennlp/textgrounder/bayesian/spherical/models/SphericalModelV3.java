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

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class SphericalModelV3 extends SphericalModelV2 {

    /**
     * Default constructor. Take input from commandline and default _options
     * and initialize class. Also, process input text and process so that
     * toponyms, stopwords and other words are identified and collected.
     *
     * @param _options
     */
    public SphericalModelV3(ExperimentParameters _parameters) {
        super(_parameters);
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
                                    probs[regoff + k] = doccount
                                          * TGMath.unnormalizedProportionalSphericalDensity(curCoords[k], regionmean, kappa)
                                          * (wordByRegionCounts[wordoff + j] + beta)
                                          / (regionCountsOfAllWords[j] + betaW);
                                }
                            }
                        }

                        for (int j = 0; j < curCoordCount; ++j) {
                            probs[emptyid * maxCoord + j] = crpalpha_mod / curCoordCount
                                  * beta / betaW;
                        }

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
                  regionCountsOfAllWords, regionMeans, regionToponymCoordinateCounts);
        }
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
                                probs[regoff + k] = doccount
                                      * TGMath.unnormalizedProportionalSphericalDensity(curCoords[k], regionmean, kappa)
                                      * (wordByRegionCounts[wordoff + j] + beta)
                                      / (regionCountsOfAllWords[j] + betaW);
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
