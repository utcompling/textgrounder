///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.models;

import java.util.ArrayList;

import opennlp.textgrounder.annealers.*;
import opennlp.textgrounder.geo.CommandLineOptions;
import opennlp.textgrounder.textstructs.*;
import opennlp.textgrounder.topostructs.*;

/**
 * IGNORE THIS CLASS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * IGNORE THIS CLASS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * IGNORE THIS CLASS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * IGNORE THIS CLASS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * IGNORE THIS CLASS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * IGNORE THIS CLASS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * IGNORE THIS CLASS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * IGNORE THIS CLASS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * IGNORE THIS CLASS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * IGNORE THIS CLASS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * IGNORE THIS CLASS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * IGNORE THIS CLASS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * IGNORE THIS CLASS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * IGNORE THIS CLASS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * IGNORE THIS CLASS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 *
 * @author tsmoon
 */
public class OracleRegionModel extends NonRandomStartRegionModel<SmallLocation> {

    public OracleRegionModel(CommandLineOptions _options,
          SmallLocation _genericsKludgeFactor) {
        super(_options, _genericsKludgeFactor);
    }

    /**
     * Randomly initialize fields for training. If word is a toponym, choose
     * random region only from regions aligned to name.
     */
    @Override
    public void randomInitialize() {
        int wordid, docid, topicid;
        int istoponym, isstopword;
        int wordoff, docoff;
        double[] probs = new double[T];
        double totalprob, max, r;

        ArrayList<Location> goldLocationArray = weightedEvalTokenArrayBuffer.goldLocationArrayList;

        for (int i = 0; i < N; ++i) {
            if (goldLocationArray.get(i) != null) {
                toponymVector[i] = 1;
            } else {
                toponymVector[i] = 0;
            }
        }

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            if (isstopword == 0) {
                wordid = wordIdMapper[wordVector[i]];
                docid = documentVector[i];
                docoff = docid * T;
                istoponym = toponymVector[i];

                totalprob = 0;
                if (istoponym == 1) {
                    Location goldLoc = goldLocationArray.get(i);
                    int regid = getRegionForLocationOracle(goldLoc);
                    if (regid > -1) {
                        try {
                            for (int j = 0;; ++j) {
                                totalprob = probs[j] = 0;
                            }
                        } catch (ArrayIndexOutOfBoundsException e) {
                        }
                        totalprob += probs[regid] = 1;
                    } else {
                        wordoff = wordid * T;
                        try {
                            for (int j = 0;; ++j) {
                                totalprob += probs[j] =
                                      regionByToponymFilter[wordoff + j]
                                      * activeRegionByDocumentFilter[docoff + j];
                            }
                        } catch (ArrayIndexOutOfBoundsException e) {
                        }
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
                topicid = 0;
                while (r > max) {
                    topicid++;
                    max += probs[topicid];
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
                    wordid = wordIdMapper[wordVector[i]];
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
                                      * regionByToponymFilter[wordoff + j]
                                      * activeRegionByDocumentFilter[docoff + j];
                            }
                        } else {
                            for (int j = 0;; ++j) {
                                probs[j] = (wordByTopicCounts[wordoff + j] + beta)
                                      / (topicCounts[j] + betaW)
                                      * (topicByDocumentCounts[docoff + j] + alpha)
                                      * activeRegionByDocumentFilter[docoff + j];
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

            normalize();
            evaluate(evalTokenArrayBuffer);
            evalTokenArrayBuffer.modelLocationArrayList.clear();
        }
    }
    
    protected int getRegionForLocationOracle(SmallLocation loc) {
        if (loc == null) {
            return -1;
        }
        int curX = (int) (loc.getCoord().latitude + 180) / (int) degreesPerRegion;
        int curY = (int) (loc.getCoord().longitude + 90) / (int) degreesPerRegion;

        try {
            if (regionArray[curX][curY] == null) {
                /**
                 * means this location has no resolution in current model. return -1 one and let model initialize randomly
                 */
                return -1;
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            return -1;
        }
        return regionMapperCallback.getRegionToIdxMap().get(regionArray[curX][curY]);
    }

    @Override
    public void evaluate() {
        evaluate(evalTokenArrayBuffer);
    }

    /**
     *
     * @param evalTokenArrayBuffer
     */
    @Override
    public void evaluate(EvalTokenArrayBuffer<SmallLocation> evalTokenArrayBuffer) {
        if (evalTokenArrayBuffer.modelLocationArrayList.size() != evalTokenArrayBuffer.goldLocationArrayList.size()) {
            System.out.println("MISMATCH: model: " + evalTokenArrayBuffer.modelLocationArrayList.size() + "; gold: " + evalTokenArrayBuffer.goldLocationArrayList.size());
            System.exit(1);
        }

        int tp = 0;
        int fp = 0;
        int fn = 0;

        int goldLocationCount = 0;
        int modelLocationCount = 0;

        int noModelGuessCount = 0;

        // these correspond exactly with Jochen's thesis:
        int t_n = 0;
        int t_c = 0;
        int t_i = 0;
        int t_u = 0;

        for (int i = 0; i < evalTokenArrayBuffer.size(); i++) {
            SmallLocation curModelLoc = evalTokenArrayBuffer.modelLocationArrayList.get(i);
            SmallLocation curGoldLoc = evalTokenArrayBuffer.goldLocationArrayList.get(i);

            if (curGoldLoc != null) {
                int goldregid = getRegionForLocationOracle(curGoldLoc);
                goldLocationCount++;
                t_n++;
                if (goldregid != -1 && curModelLoc != null) {
                    int modelregid = getRegionForLocationOracle(curModelLoc);

                    modelLocationCount++;
                    if (goldregid == modelregid) {
                        tp++;
                        t_c++;
                    } else {
                        fp++;
                        fn++; // reinstated
                        t_i++;
                    }
                } else {
                    fn++;
                    t_u++;
                    noModelGuessCount++;
                }
            }
        }

        // in jochen's terms:
        double precision = (double) t_c / (t_c + t_i);
        double recall = (double) t_c / (t_n);

        double f1 = 2 * ((precision * recall) / (precision + recall));

        System.out.println(String.format("%f\t%f\t%f", precision, recall, f1));
    }
}
