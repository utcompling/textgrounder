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
public class SphericalEmptyAnnealer extends SphericalAnnealer {

    protected SphericalEmptyAnnealer() {
    }

    public SphericalEmptyAnnealer(ExperimentParameters _experimentParameters) {
        super(_experimentParameters);
    }

    @Override
    public double annealProbs(int starti, double[] classes) {
        int l = classes.length;
        return TGMath.stableSum(classes, starti, l);
    }

    @Override
    public double annealProbs(int _starti, int _endi, double[] _classes) {
        return TGMath.stableSum(_classes, _starti, _endi);
    }

    @Override
    public double annealProbs(int _R, int _subC, int _C, double[] _classes) {
        double max = _classes[0];

        for (int i = 0; i < _R; ++i) {
            int off = i * _C;
            for (int j = 0; j < _subC; ++j) {
                if (_classes[off + j] > max) {
                    max = _classes[off + j];
                }
            }
        }

        if (max == 0) {
            return 0;
        }

        max = Math.log(max);
        double p = 0;

        for (int i = 0; i < _R; ++i) {
            int off = i * _C;
            for (int j = 0; j < _subC; ++j) {
                p += Math.exp(Math.log(_classes[off + j]) - max);
            }
        }

        return Math.exp(max + Math.log(p));
    }
}
