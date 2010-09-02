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
package opennlp.textgrounder.bayesian.spherical.annealers;

import opennlp.textgrounder.bayesian.annealers.Annealer;
import opennlp.textgrounder.bayesian.apps.ExperimentParameters;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public abstract class SphericalAnnealer extends Annealer {

    /**
     * Counts of tcount per topic. However, since access more often occurs in
     * terms of the tcount, it will be a topic by word matrix.
     */
    protected double[] wordByRegionCounts;
    /**
     *
     */
    protected double[] toponymByRegionCounts;
    /**
     *
     */
    protected double[][][] regionToponymCoordinateCounts;
    /**
     *
     */
    protected double[] nonToponymRegionCounts;

    protected SphericalAnnealer() {
    }

    public SphericalAnnealer(ExperimentParameters _experimentParameters) {
        super(_experimentParameters);
    }

    public void collectSamples(double[] _wordByRegionCounts,
          double[] _toponymByRegionCounts, double[] _nonToponymRegionCounts,
          double[][][] _regionToponymCoordinateCounts) {

        if (sampleCount < samples) {
            if (sampleiteration && (innerIter % lag == 0)) {
                sampleCount += 1;
                if (samples == sampleCount) {
                    finishedCollection = true;
                }

                System.err.print("(sample:" + (innerIter + 1) / lag + ")");
                if (wordByRegionCounts == null) {
                    wordByRegionCounts = new double[_wordByRegionCounts.length];
                    toponymByRegionCounts = new double[_toponymByRegionCounts.length];
                    nonToponymRegionCounts = new double[_nonToponymRegionCounts.length];
                    for (int i = 0; i < _wordByRegionCounts.length; ++i) {
                        wordByRegionCounts[i] = 0;
                    }
                    for (int i = 0; i < _toponymByRegionCounts.length; ++i) {
                        toponymByRegionCounts[i] = 0;
                    }
                    for (int i = 0; i < _nonToponymRegionCounts.length; ++i) {
                        _nonToponymRegionCounts[i] = 0;
                    }

                    regionToponymCoordinateCounts = new double[_regionToponymCoordinateCounts.length][][];
                    for (int i = 0; i < _regionToponymCoordinateCounts.length;
                          ++i) {
                        regionToponymCoordinateCounts[i] = new double[_regionToponymCoordinateCounts[i].length][];
                        for (int j = 0;
                              j < _regionToponymCoordinateCounts[i].length; ++j) {
                            regionToponymCoordinateCounts[i][j] = new double[_regionToponymCoordinateCounts[i][j].length];
                            for (int k = 0;
                                  k < _regionToponymCoordinateCounts[i][j].length;
                                  ++k) {
                                regionToponymCoordinateCounts[i][j][k] = 0;
                            }
                        }
                    }
                }

                for (int i = 0; i < wordByRegionCounts.length; ++i) {
                    wordByRegionCounts[i] += _wordByRegionCounts[i];
                }

                for (int i = 0; i < toponymByRegionCounts.length; ++i) {
                    toponymByRegionCounts[i] += _toponymByRegionCounts[i];
                }

                for (int i = 0; i < nonToponymRegionCounts.length; ++i) {
                    nonToponymRegionCounts[i] += _nonToponymRegionCounts[i];
                }

                for (int i = 0; i < regionToponymCoordinateCounts.length; ++i) {
                    for (int j = 0; j < regionToponymCoordinateCounts[i].length;
                          ++j) {
                        for (int k = 0;
                              k < regionToponymCoordinateCounts[i][j].length;
                              ++k) {
                            regionToponymCoordinateCounts[i][j][k] += _regionToponymCoordinateCounts[i][j][k];
                        }
                    }
                }
            }
            if (finishedCollection) {
                normalizeSamples();
            }
        }
    }

    protected void normalizeSamples() {
        for (int i = 0; i < wordByRegionCounts.length; ++i) {
            wordByRegionCounts[i] /= sampleCount;
        }

        for (int i = 0; i < toponymByRegionCounts.length; ++i) {
            toponymByRegionCounts[i] /= sampleCount;
        }

        for (int i = 0; i < nonToponymRegionCounts.length; ++i) {
            nonToponymRegionCounts[i] /= sampleCount;
        }

        for (int i = 0; i < regionToponymCoordinateCounts.length;
              ++i) {
            for (int j = 0;
                  j < regionToponymCoordinateCounts[i].length; ++j) {
                for (int k = 0;
                      k < regionToponymCoordinateCounts[i][j].length;
                      ++k) {
                    regionToponymCoordinateCounts[i][j][k] /= sampleCount;
                }
            }
        }

    }
}
