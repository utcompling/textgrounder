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

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class SphericalMaximumPosteriorDecoder extends SphericalAnnealer {

    private int count = 1;

    public SphericalMaximumPosteriorDecoder() {
    }

    public SphericalMaximumPosteriorDecoder(ExperimentParameters _experimentParameters) {
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
    public double annealProbs(int _starti, int _endi, double[] _classes) {
        double max = 0;
        int maxid = 0;
        for (int i = _starti; i < _endi; ++i) {
            if (_classes[i] > max) {
                max = _classes[i];
                maxid = i;
            }
        }
        for (int i = _starti; i < _endi; ++i) {
            _classes[i] = 0;
        }
        _classes[maxid] = 1;
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
    public double annealProbs(int _R, int _subC, int _C, double[] _classes) {
        double max = 0;
        int maxid = 0;
        for (int i = 0; i < _R; ++i) {
            int off = i * _C;
            for (int j = 0; j < _subC; ++j) {
                if (_classes[off + j] > max) {
                    max = _classes[off + j];
                    maxid = off + j;
                }
            }
        }

        for (int i = 0; i < _R; ++i) {
            int off = i * _C;
            for (int j = 0; j < _subC; ++j) {
                _classes[off + j] = 0;
            }
        }

        _classes[maxid] = 1;
        return 1;
    }
}
