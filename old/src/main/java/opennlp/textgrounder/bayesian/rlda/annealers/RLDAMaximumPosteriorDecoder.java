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
package opennlp.textgrounder.bayesian.rlda.annealers;

import opennlp.textgrounder.bayesian.apps.ExperimentParameters;

/**
 * Maximum posterior decoder. Simply returns the largest value among the arrays.
 * 
 * @author tsmoon
 */
public class RLDAMaximumPosteriorDecoder extends RLDAAnnealer {

    private int count = 1;

    public RLDAMaximumPosteriorDecoder() {
    }

    public RLDAMaximumPosteriorDecoder(ExperimentParameters _experimentParameters) {
        super(_experimentParameters);
    }

    @Override
    public double annealProbs(int starti, double[] classes) {
        double max = 0;
        int maxid = 0;
        try {
            for (int i = starti;; ++i) {
                if (classes[i] > max) {
                    max = classes[i];
                    maxid = i;
                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {
        }
        try {
            for (int i = starti;; ++i) {
                classes[i] = 0;
            }
        } catch (ArrayIndexOutOfBoundsException e) {
        }
        classes[maxid] = 1;
        return 1;
    }

    @Override
    public boolean nextIter() {
        if (count != 0) {
            count = 0;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void collectSamples(int[] topicCounts, int[] wordByTopicCounts,
          int[] regionByDocumentCounts) {
        return;
    }
}
