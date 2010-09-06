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
 * Class for no annealing regime. If the initial and target temperature are
 * equal, this class is called. In this case, the outer iteration is set to one
 * and the inner iterations are all the iterations there are.
 * 
 * @author tsmoon
 */
public class RLDAEmptyAnnealer extends RLDAAnnealer {

    protected RLDAEmptyAnnealer() {
    }

    public RLDAEmptyAnnealer(ExperimentParameters _experimentParameters) {
        super(_experimentParameters);
    }

    @Override
    public double annealProbs(int starti, double[] classes) {
        double sum = 0;
        try {
            for (int i = starti;; ++i) {
                sum += classes[i];
            }
        } catch (ArrayIndexOutOfBoundsException e) {
        }
        return sum;
    }
}
