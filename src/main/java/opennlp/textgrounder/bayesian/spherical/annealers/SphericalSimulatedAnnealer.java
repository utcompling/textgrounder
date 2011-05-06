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

import opennlp.textgrounder.bayesian.apps.ExperimentParameters;
import opennlp.textgrounder.bayesian.mathutils.TGMath;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class SphericalSimulatedAnnealer extends SphericalAnnealer {

    protected SphericalSimulatedAnnealer() {
    }

    public SphericalSimulatedAnnealer(ExperimentParameters _experimentParameters) {
        super(_experimentParameters);
    }

    @Override
    public double annealProbs(int _starti, double[] _classes) {
        return annealProbs(_starti, _classes.length, _classes);
    }

    @Override
    public double annealProbs(int _starti, int _endi, double[] _classes) {
        double sum = 0, sumw = 0;
        sum = TGMath.stableSum(_classes, _starti, _endi);

        if (temperatureReciprocal != 1) {
            for (int i = _starti; i < _endi; ++i) {
                _classes[i] = TGMath.stableDiv(_classes[i], sum);
                sumw += _classes[i] = Math.pow(_classes[i], temperatureReciprocal);
            }
        } else {
            sumw = sum;
        }
        return sumw;
    }

    @Override
    public double annealProbs(int _R, int _subC, int _C, double[] _classes) {
        double sum = 0, sumw = 0;
        for (int i = 0; i < _R; ++i) {
            int off = i * _C;
            for (int j = 0; j < _subC; ++j) {
                sum += _classes[off + j];
            }
        }
        if (temperatureReciprocal != 1) {
            for (int i = 0; i < _R; ++i) {
                int off = i * _C;
                for (int j = 0; j < _subC; ++j) {
                    _classes[off + j] /= sum;
                    sumw += _classes[off + j] = Math.pow(_classes[off + j], temperatureReciprocal);
                }
            }
        } else {
            sumw = sum;
        }

        return sumw;
    }
}
