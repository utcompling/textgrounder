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
package opennlp.textgrounder.bayesian.rlda.annealers;

import opennlp.textgrounder.bayesian.annealers.*;
import opennlp.textgrounder.bayesian.apps.ExperimentParameters;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public abstract class RLDAAnnealer extends Annealer {

    public RLDAAnnealer() {
    }

    public RLDAAnnealer(ExperimentParameters _experimentParameters) {
        super(_experimentParameters);
    }
    /**
     * Counts of topics
     */
    protected double[] topicSampleCounts = null;
    /**
     * Counts of tcount per topic. However, since access more often occurs in
     * terms of the tcount, it will be a topic by word matrix.
     */
    protected double[] wordByTopicSampledCounts = null;
    /**
     *
     */
    protected double[] regionByDocumentSampledCounts = null;

    /**
     *
     * @param _topicCounts
     * @param _wordByTopicCounts
     * @param b
     */
    public void collectSamples(int[] _topicCounts, int[] _wordByTopicCounts, int[] _regionByDocumentCounts) {
        if (sampleCount < samples) {
            if (sampleiteration && (innerIter % lag == 0)) {
                sampleCount += 1;
                if (samples == sampleCount) {
                    finishedCollection = true;
                }

                System.err.print("(sample:" + (innerIter + 1) / lag + ")");
                if (topicSampleCounts == null) {
                    topicSampleCounts = new double[_topicCounts.length];
                    wordByTopicSampledCounts = new double[_wordByTopicCounts.length];
                    regionByDocumentSampledCounts = new double[_regionByDocumentCounts.length];
                    for (int i = 0; i < _topicCounts.length; ++i) {
                        topicSampleCounts[i] = 0;
                    }
                    for (int i = 0; i < _wordByTopicCounts.length; ++i) {
                        wordByTopicSampledCounts[i] = 0;
                    }
                    for (int i = 0; i < _regionByDocumentCounts.length; ++i) {
                        regionByDocumentSampledCounts[i] = 0;
                    }
                }

                for (int i = 0; i < _topicCounts.length; ++i) {
                    topicSampleCounts[i] += _topicCounts[i];
                }
                for (int i = 0; i < _wordByTopicCounts.length; ++i) {
                    wordByTopicSampledCounts[i] += _wordByTopicCounts[i];
                }
                for (int i = 0; i < _regionByDocumentCounts.length; ++i) {
                    regionByDocumentSampledCounts[i] += _regionByDocumentCounts[i];
                }
            }

            if (finishedCollection) {
                averageSamples(topicSampleCounts);
                averageSamples(wordByTopicSampledCounts);
                averageSamples(regionByDocumentSampledCounts);
            }
        }
    }

    /**
     * @return the topicSampleCounts
     */
    public double[] getAveragedTopicSampleCounts() {
        return topicSampleCounts;
    }

    /**
     * @return the wordByTopicSampledCounts
     */
    public double[] getAveragedWordByTopicSampledProbs() {
        return wordByTopicSampledCounts;
    }

    public double[] getAveragedRegionByDocumentSampledCounts() {
        return regionByDocumentSampledCounts;
    }
}
