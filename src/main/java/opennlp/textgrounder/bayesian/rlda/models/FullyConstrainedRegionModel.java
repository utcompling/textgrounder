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
package opennlp.textgrounder.bayesian.rlda.models;

import opennlp.textgrounder.bayesian.rlda.annealers.*;
import opennlp.textgrounder.bayesian.apps.ExperimentParameters;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class FullyConstrainedRegionModel extends RegionModel {

    private static final long serialVersionUID = 42L;

    public FullyConstrainedRegionModel(ExperimentParameters _parameters) {
        super(_parameters);
    }

    /**
     * Randomly initialize fields for training. If word is a toponym, choose
     * random region only from regions aligned to name.
     */
    @Override
    public void randomInitialize() {
        int wordid, docid, regionid;
        int istoponym, isstopword;
        int wordoff, docoff;
        double[] probs = new double[R];
        double totalprob, max, r;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            if (isstopword == 0) {
                wordid = wordVector[i];
                docid = documentVector[i];
                docoff = docid * R;
                istoponym = toponymVector[i];

                totalprob = 0;
                if (istoponym == 1) {
                    wordoff = wordid * R;
                    try {
                        for (int j = 0;; ++j) {
                            totalprob += probs[j] =
                                  regionByToponymFilter[wordoff + j]
                                  * activeRegionByDocumentFilter[docoff + j];
                        }
                    } catch (ArrayIndexOutOfBoundsException e) {
                    }
                } else {
                    try {
                        for (int j = 0;; ++j) {
                            totalprob += probs[j] =
                                  activeRegionByDocumentFilter[docoff + j];
                        }
                    } catch (ArrayIndexOutOfBoundsException e) {
                    }
                }

                r = rand.nextDouble() * totalprob;

                max = probs[0];
                regionid = 0;
                while (r > max) {
                    regionid++;
                    max += probs[regionid];
                }

                regionVector[i] = regionid;
                regionCounts[regionid]++;
                regionByDocumentCounts[docid * R + regionid]++;
                wordByRegionCounts[wordid * R + regionid]++;
            }
        }
    }

    /**
     * Train topics
     *
     * @param decoder Annealing scheme to use
     */
    @Override
    public void train(RLDAAnnealer _annealer) {
        int wordid, docid, regionid;
        int wordoff, docoff;
        int istoponym, isstopword;
        double[] probs = new double[R];
        double totalprob, max, r;

        while (_annealer.nextIter()) {
            for (int i = 0; i < N; ++i) {
                isstopword = stopwordVector[i];
                if (isstopword == 0) {
                    wordid = wordVector[i];
                    docid = documentVector[i];
                    regionid = regionVector[i];
                    istoponym = toponymVector[i];
                    docoff = docid * R;
                    wordoff = wordid * R;

                    regionCounts[regionid]--;
                    regionByDocumentCounts[docoff + regionid]--;
                    wordByRegionCounts[wordoff + regionid]--;

                    try {
                        if (istoponym == 1) {
                            for (int j = 0;; ++j) {
                                probs[j] = (wordByRegionCounts[wordoff + j] + beta)
                                      / (regionCounts[j] + betaW)
                                      * (regionByDocumentCounts[docoff + j] + alpha)
                                      * regionByToponymFilter[wordoff + j];
                            }
                        } else {
                            for (int j = 0;; ++j) {
                                probs[j] = (wordByRegionCounts[wordoff + j] + beta)
                                      / (regionCounts[j] + betaW)
                                      * (regionByDocumentCounts[docoff + j] + alpha)
                                      * activeRegionByDocumentFilter[docoff + j];
                            }
                        }
                    } catch (ArrayIndexOutOfBoundsException e) {
                    }
                    totalprob = _annealer.annealProbs(probs);
                    r = rand.nextDouble() * totalprob;

                    max = probs[0];
                    regionid = 0;
                    while (r > max) {
                        regionid++;
                        max += probs[regionid];
                    }
                    regionVector[i] = regionid;

                    regionCounts[regionid]++;
                    regionByDocumentCounts[docoff + regionid]++;
                    wordByRegionCounts[wordoff + regionid]++;
                }
            }

            _annealer.collectSamples(regionCounts, wordByRegionCounts, regionByDocumentCounts);
        }
    }

    @Override
    public void decode() {
        System.err.println(String.format("Decoding maximum posterior topics"));
        RLDAAnnealer decoder = new RLDAMaximumPosteriorDecoder();
        int wordid, docid, regionid;
        int wordoff, docoff;
        int istoponym, isstopword;
        double[] probs = new double[R];
        double totalprob, max, r;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            if (isstopword == 0) {
                wordid = wordVector[i];
                docid = documentVector[i];
                regionid = regionVector[i];
                istoponym = toponymVector[i];
                docoff = docid * R;
                wordoff = wordid * R;

                try {
                    if (istoponym == 1) {
                        for (int j = 0;; ++j) {
                            probs[j] = (normalizedWordByRegionCounts[wordoff + j] + beta)
                                  / (normalizedRegionCounts[j] + betaW)
                                  * (normalizedRegionByDocumentCounts[docoff + j] + alpha)
                                  * regionByToponymFilter[wordoff + j];
                        }
                    } else {
                        for (int j = 0;; ++j) {
                            probs[j] = (normalizedWordByRegionCounts[wordoff + j] + beta)
                                  / (normalizedRegionCounts[j] + betaW)
                                  * (normalizedRegionByDocumentCounts[docoff + j] + alpha)
                                  * activeRegionByDocumentFilter[docoff + j];
                        }
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                }
                totalprob = decoder.annealProbs(probs);
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
