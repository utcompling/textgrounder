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
package opennlp.textgrounder.annealers;

import opennlp.textgrounder.geo.CommandLineOptions;

/**
 * A full simulated annealer
 * 
 * @author tsmoon
 */
public class SimulatedAnnealer extends Annealer {

    /**
     * Counts of topics
     */
    protected double[] topicCounts;
    /**
     * Counts of tcount per topic. However, since access more often occurs in
     * terms of the tcount, it will be a topic by word matrix.
     */
    protected double[] wordByTopicCounts;
    protected double beta;
    protected double betaW;

    public SimulatedAnnealer(CommandLineOptions options) {
        super(options);
    }

    @Override
    public double annealProbs(int starti, double[] classes) {
        double sum = 0, sumw = 0;
        try {
            for (int i = starti;; ++i) {
                sum += classes[i];
            }
        } catch (ArrayIndexOutOfBoundsException e) {
        }
        if (temperatureReciprocal != 1) {
            try {
                for (int i = starti;; ++i) {
                    classes[i] /= sum;
                    sumw += classes[i] = Math.pow(classes[i],
                          temperatureReciprocal);
                }
            } catch (ArrayIndexOutOfBoundsException e) {
            }
        } else {
            sumw = sum;
        }
        try {
            for (int i = starti;; ++i) {
                classes[i] /= sumw;
            }
        } catch (ArrayIndexOutOfBoundsException e) {
        }
        /**
         * For now, we set everything so that it sums to one.
         */
        return 1;
    }

    @Override
    public void collectSamples(int[] tc, int[] wbtc, double b) {
        if (sampleiteration && ((innerIter + 1) % lag == 0)) {
            if (topicCounts == null) {
                topicCounts = new double[tc.length];
                wordByTopicCounts = new double[wbtc.length];
                for (int i = 0; i < tc.length; ++i) {
                    topicCounts[i] = 0;
                }
                for (int i = 0; i < wbtc.length; ++i) {
                    wordByTopicCounts[i] = 0;
                }
                beta = b;
                betaW = beta * wbtc.length / tc.length;
            }

            for (int i = 0; i < tc.length; ++i) {
                topicCounts[i] += tc[i] + beta;
            }
            for (int i = 0; i < wbtc.length; ++i) {
                wordByTopicCounts[i] += wbtc[i] + betaW;
            }
        }
    }
}
