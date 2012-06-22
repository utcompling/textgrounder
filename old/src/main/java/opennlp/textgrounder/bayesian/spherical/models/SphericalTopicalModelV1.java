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
import opennlp.textgrounder.bayesian.mathutils.RKRand.BetaEdgeException;
import opennlp.textgrounder.bayesian.spherical.annealers.*;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class SphericalTopicalModelV1 extends SphericalModelBase {

    private static final long serialVersionUID = 42L;

    public SphericalTopicalModelV1(ExperimentParameters _parameters) {
        super(_parameters);
    }

    /**
     * Randomly initialize fields for training. If word is a toponym, choose
     * random region only from regions aligned to name.
     */
    @Override
    public void randomInitialize() {

        /**
         * Sampling initial global
         */
        {
            double[] v = new double[L];
            double[] ivl = new double[L];
            double[] ilvl = new double[L];

            int l = 0;
            try {
                for (; l < L - 1; ++l) {
                    v[l] = RKRand.rk_beta(1, alpha_H);
                }
            } catch (BetaEdgeException ex) {
                v[l] = 1;
            }

            v[L - 1] = 1;
            for (int i = 0; i < L; ++i) {
                ilvl[i] = Math.log(1 - v[i]);
            }
            ivl = TGMath.cumSum(ilvl);

            globalDishWeights[0] = v[0];
            for (l = 1; l < L; ++l) {
                globalDishWeights[l] = Math.exp(Math.log(v[l]) + ivl[l - 1]);
            }
        }

        {
            double[] v = new double[L];
            double[] wcs = new double[L];
            wcs = TGMath.stableCumProb(globalDishWeights);

            double[] vl = new double[L];
            double[] ivl = new double[L];
            double[] ilvl = new double[L];

            for (int d = 0; d < D; ++d) {

                int docoff = d * L;
                double ai = alpha[d];

                double val = 1;
                int l = 0;
                try {
                    for (; l < L - 1; ++l) {
                        val = RKRand.rk_beta(ai * globalDishWeights[l], ai * (1 - wcs[l]));
                        if (Double.isNaN(val)) {
                            vl[l] = 0;
                        } else {
                            vl[l] = val;
                        }
                    }
                } catch (BetaEdgeException ex) {
                    vl[l] = 1;
//                        System.err.println(ex.getMessage());
//                        System.err.println("This happened at iteration " + l + " of randomInitialize");
                }

                vl[L - 1] = 1;
                for (int i = 0; i < L; ++i) {
                    ilvl[i] = Math.log(1 - vl[i]);
                }
                ivl = TGMath.cumSum(ilvl);

                localDishWeights[docoff] = vl[0];
                for (l = 1; l < L; ++l) {
                    localDishWeights[docoff + l] = Math.exp(Math.log(vl[l]) + ivl[l - 1]);
                }
            }
        }

        for (int l = 0; l < L; ++l) {
            regionMeans[l] = TGRand.uniVMFRnd();
            kappa[l] = RKRand.rk_gamma(kappa_hyper_shape, kappa_hyper_scale);
        }

        for (int l = 0; l < L; ++l) {
            double[] dir = TGRand.dirichletRnd(phi_dirichlet_hyper, W);
            for (int i = 0; i < W; ++i) {
                nonToponymByDishDirichlet[i * L + l] = dir[i];
            }
        }

        for (int i = 0; i < T; ++i) {
            int len = toponymCoordinateLexicon[i].length;
            double[] dir = TGRand.dirichletRnd(ehta_dirichlet_hyper, len);
            toponymCoordinateDirichlet[i] = dir;
        }

        {
            int wordid, docid, dishid, coordid;
            int wordoff, docoff, regoff;
            int istoponym, isstopword;
            int curCoordCount;
            double[][] curCoords;
            double[] regionProbs = new double[L * maxCoord];
            double[] dishProbs = new double[L];
            double[] regionmean;
            double totalprob = 0, max, r;

            SphericalEmptyAnnealer sea = new SphericalEmptyAnnealer(experimentParameters);

            for (int i = 0; i < N; ++i) {
                isstopword = stopwordVector[i];
                istoponym = toponymVector[i];
                if (isstopword == 0) {
                    if (istoponym == 1) {
                        Arrays.fill(regionProbs, 0);

                        wordid = wordVector[i];
                        docid = documentVector[i];
                        docoff = docid * L;
                        wordoff = wordid * L;

                        curCoordCount = toponymCoordinateLexicon[wordid].length;
                        curCoords = toponymCoordinateLexicon[wordid];

                        double[] coordinateWeights = toponymCoordinateDirichlet[wordid];

                        for (int j = 0; j < L; ++j) {
                            regoff = j * maxCoord;
                            regionmean = regionMeans[j];
                            double ldw = localDishWeights[docoff + j];
                            for (int k = 0; k < curCoordCount; ++k) {
                                regionProbs[regoff + k] =
                                      TGMath.stableProd(ldw, coordinateWeights[k],
                                      TGMath.sphericalDensity(curCoords[k], regionmean, kappa[k]));
                            }
                        }

                        totalprob = sea.annealProbs(L, curCoordCount, maxCoord, regionProbs);

                        r = TGMath.stableProd(rand.nextDouble(), totalprob);

                        max = regionProbs[0];
                        coordid = 0;
                        dishid = 0;
                        while (r > max) {
                            coordid++;
                            if (coordid == curCoordCount) {
                                dishid++;
                                coordid = 0;
                            }
                            max = TGMath.stableSum(max, regionProbs[dishid * maxCoord + coordid]);
                        }
                        dishVector[i] = dishid;
                        coordinateVector[i] = coordid;

                        toponymByDishCounts[dishid]++;
                        dishByRestaurantCounts[docoff + dishid]++;
                        toponymCoordinateCounts[wordid][coordid]++;
                        globalDishCounts[dishid]++;
                    } else {
                        wordid = wordVector[i];
                        docid = documentVector[i];
                        docoff = docid * L;
                        wordoff = wordid * L;

                        try {
                            for (int j = 0;; ++j) {
                                dishProbs[j] = TGMath.stableProd(localDishWeights[docoff + j], nonToponymByDishDirichlet[wordoff + j]);
                            }
                        } catch (ArrayIndexOutOfBoundsException e) {
                        }

                        totalprob = sea.annealProbs(dishProbs);
                        r = TGMath.stableProd(rand.nextDouble(), totalprob);

                        max = dishProbs[0];
                        dishid = 0;
                        while (r > max) {
                            dishid++;
                            max = TGMath.stableSum(max, dishProbs[dishid]);
                        }
                        dishVector[i] = dishid;

                        globalDishCounts[dishid]++;
                        dishByRestaurantCounts[docoff + dishid]++;
                        nonToponymByDishCounts[wordoff + dishid]++;
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

    public void resample() {
        /**
         * careful, N needs to revised to only non-stopwords
         */
        alpha_H = globalAlphaUpdate();
        alpha = alphaUpdate();
        globalDishWeights = globalStickBreakingWeightsUpdate();
        localDishWeights = restaurantStickBreakingWeightsUpdate();
        regionMeans = regionMeansUpdate();
        kappa = kappaUpdate();

        double[] dsum = new double[W];
        Arrays.fill(dsum, 0);
        for (int w = 0; w < W; ++w) {
            int wordoff = w * L;
            for (int l = 0; l < L; ++l) {
                dsum[w] += nonToponymByDishDirichlet[wordoff + l] =
                      RKRand.rk_gamma(phi_dirichlet_hyper
                      + nonToponymByDishCounts[wordoff + l], 1);
            }
        }

        for (int w = 0; w < W; ++w) {
            dsum[w] = Math.log(dsum[w]);
        }

        for (int w = 0; w < W; ++w) {
            int wordoff = w * L;
            for (int l = 0; l < L; ++l) {
                double val = nonToponymByDishDirichlet[wordoff + l];
                nonToponymByDishDirichlet[wordoff + l] =
                      Math.exp(Math.log(val) - dsum[w]);
            }
        }

        for (int i = 0; i < T; ++i) {
            int len = toponymCoordinateLexicon[i].length;
            int[] coordCounts = toponymCoordinateCounts[i];
            for (int j = 0; j < len; ++j) {
                double[] dir = TGRand.dirichletRnd(ehta_dirichlet_hyper, coordCounts);
                toponymCoordinateDirichlet[i] = dir;
            }
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
                        dishid = dishVector[i];
                        coordid = coordinateVector[i];
                        docoff = docid * L;
                        wordoff = wordid * L;

                        globalDishCounts[dishid]--;
                        toponymByDishCounts[dishid]--;
                        dishByRestaurantCounts[docoff + dishid]--;
                        toponymCoordinateCounts[wordid][coordid]--;

                        curCoordCount = toponymCoordinateLexicon[wordid].length;
                        curCoords = toponymCoordinateLexicon[wordid];

                        double[] coordinateWeights = toponymCoordinateDirichlet[wordid];

                        for (int j = 0; j < L; ++j) {
                            regoff = j * maxCoord;
                            regionmean = regionMeans[j];
                            double ldw = localDishWeights[docoff + j];
                            for (int k = 0; k < curCoordCount; ++k) {
                                regionProbs[regoff + k] =
                                      TGMath.stableProd(ldw, coordinateWeights[k],
                                      TGMath.sphericalDensity(curCoords[k], regionmean, kappa[k]));
                            }
                        }

                        totalprob = _annealer.annealProbs(L, curCoordCount, maxCoord, regionProbs);

                        r = TGMath.stableProd(rand.nextDouble(), totalprob);

                        max = regionProbs[0];
                        dishid = 0;
                        coordid = 0;
                        while (r > max) {
                            coordid++;
                            if (coordid == curCoordCount) {
                                dishid++;
                                coordid = 0;
                            }
                            max = TGMath.stableSum(max, regionProbs[dishid * maxCoord + coordid]);
                        }
                        dishVector[i] = dishid;
                        coordinateVector[i] = coordid;

                        toponymByDishCounts[dishid]++;
                        dishByRestaurantCounts[docoff + dishid]++;
                        toponymCoordinateCounts[wordid][coordid]++;
                        globalDishCounts[dishid]++;
                    } else {
                        wordid = wordVector[i];
                        docid = documentVector[i];
                        dishid = dishVector[i];
                        docoff = docid * L;
                        wordoff = wordid * L;

                        globalDishCounts[dishid]--;
                        dishByRestaurantCounts[docoff + dishid]--;
                        nonToponymByDishCounts[wordoff + dishid]--;

                        try {
                            for (int j = 0;; ++j) {
                                dishProbs[j] = TGMath.stableProd(localDishWeights[docoff + j], nonToponymByDishDirichlet[wordoff + j]);
                            }
                        } catch (ArrayIndexOutOfBoundsException e) {
                        }

                        totalprob = _annealer.annealProbs(dishProbs);
                        r = TGMath.stableProd(rand.nextDouble(), totalprob);

                        max = dishProbs[0];
                        dishid = 0;
                        while (r > max) {
                            dishid++;
                            max = TGMath.stableSum(max, dishProbs[dishid]);
                        }
                        dishVector[i] = dishid;

                        globalDishCounts[dishid]++;
                        dishByRestaurantCounts[docoff + dishid]++;
                        nonToponymByDishCounts[wordoff + dishid]++;
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

            annealer.collectSamples(globalDishWeights, localDishWeights, regionMeans, kappa, nonToponymByDishDirichlet, toponymCoordinateDirichlet);
            resample();
        }
    }

    @Override
    public void decode() {
        SphericalAnnealer decoder = new SphericalMaximumPosteriorDecoder();
        int wordid, docid, dishid, topicid, coordid;
        int wordoff, docoff, regoff;
        int istoponym, isstopword;
        int curCoordCount;
        double[][] curCoords;
        double[] regionProbs = new double[L * maxCoord];
        double[] dishProbs = new double[L];
        double[] regionmean;
        double totalprob = 0, max, r;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            istoponym = toponymVector[i];
            if (isstopword == 0) {
                if (istoponym == 1) {
                    Arrays.fill(regionProbs, 0);

                    wordid = wordVector[i];
                    docid = documentVector[i];
                    docoff = docid * L;
                    wordoff = wordid * L;

                    curCoordCount = toponymCoordinateLexicon[wordid].length;
                    curCoords = toponymCoordinateLexicon[wordid];

                    double[] coordinateWeights = toponymCoordinateDirichletFM[wordid];

                    for (int j = 0; j < L; ++j) {
                        regoff = j * maxCoord;
                        regionmean = regionMeansFM[j];
                        double ldw = localDishWeightsFM[docoff + j];
                        for (int k = 0; k < curCoordCount; ++k) {
                            regionProbs[regoff + k] =
                                  TGMath.stableProd(ldw, coordinateWeights[k],
                                  TGMath.sphericalDensity(curCoords[k], regionmean, kappaFM[k]));
                        }
                    }

                    totalprob = decoder.annealProbs(L, curCoordCount, maxCoord, regionProbs);

                    r = TGMath.stableProd(rand.nextDouble(), totalprob);

                    max = regionProbs[0];
                    dishid = 0;
                    coordid = 0;
                    while (r > max) {
                        coordid++;
                        if (coordid == curCoordCount) {
                            dishid++;
                            coordid = 0;
                        }
                        max = TGMath.stableSum(max, regionProbs[dishid * maxCoord + coordid]);
                    }
                    dishVector[i] = dishid;
                    coordinateVector[i] = coordid;
                } else {
                    wordid = wordVector[i];
                    docid = documentVector[i];
                    docoff = docid * L;
                    wordoff = wordid * L;

                    try {
                        for (int j = 0;; ++j) {
                            dishProbs[j] = TGMath.stableProd(localDishWeightsFM[docoff + j], nonToponymByDishDirichletFM[wordoff + j]);
                        }
                    } catch (ArrayIndexOutOfBoundsException e) {
                    }

                    totalprob = decoder.annealProbs(dishProbs);
                    r = TGMath.stableProd(rand.nextDouble(), totalprob);

                    max = dishProbs[0];
                    dishid = 0;
                    while (r > max) {
                        dishid++;
                        max = TGMath.stableSum(max, dishProbs[dishid]);
                    }
                    dishVector[i] = dishid;
                }
            }
        }
    }
}
