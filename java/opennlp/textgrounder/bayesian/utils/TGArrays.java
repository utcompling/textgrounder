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
package opennlp.textgrounder.bayesian.utils;

import java.util.Arrays;
import java.util.HashSet;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class TGArrays {

    public static int[] expandSingleTierC(int[] _source, int _C, int _prevC) {
        int[] target = new int[_C];
        for (int i = 0; i < _prevC; ++i) {
            target[i] = _source[i];
        }
        for (int i = _prevC; i < _C; ++i) {
            target[i] = 0;
        }
        return target;
    }

    public static double[] expandSingleTierC(double[] _source, int _C,
          int _prevC) {
        double[] target = new double[_C];
        for (int i = 0; i < _prevC; ++i) {
            target[i] = _source[i];
        }
        for (int i = _prevC; i < _C; ++i) {
            target[i] = 0;
        }
        return target;
    }

    public static int[] expandDoubleTierC(int[] _source, int _R,
          int _C, int _prevC) {
        int[] target = new int[_R * _C];
        for (int i = 0; i < _R; ++i) {
            int off = i * _C;
            int prevoff = i * _prevC;
            for (int j = 0; j < _prevC; ++j) {
                target[off + j] = _source[prevoff + j];
            }
            for (int j = _prevC; j < _C; ++j) {
                target[off + j] = 0;
            }
        }
        return target;
    }

    public static double[] expandDoubleTierC(double[] _source, int _R,
          int _C, int _prevC) {
        double[] target = new double[_R * _C];
        for (int i = 0; i < _R; ++i) {
            int off = i * _C;
            int prevoff = i * _prevC;
            try {
                /**
                 * for cases where _C < _prevC
                 */
                for (int j = 0; j < _prevC; ++j) {
                    target[off + j] = _source[prevoff + j];
                }
            } catch (ArrayIndexOutOfBoundsException e) {
            }
            for (int j = _prevC; j < _C; ++j) {
                target[off + j] = 0;
            }
        }
        return target;
    }

    public static double[][] expandSingleTierR(double[][] _source, int _R,
          int _prevR, int _C) {
        double[][] target = new double[_R][];
        try {
            /**
             * for cases where _R < _prevR
             */
            for (int i = 0; i < _prevR; ++i) {
                target[i] = _source[i];
            }
        } catch (ArrayIndexOutOfBoundsException e) {
        }
        for (int i = _prevR; i < _R; ++i) {
            double[] sub = new double[_C];
            Arrays.fill(sub, 0);
            target[i] = sub;
        }
        return target;
    }

    public static double[] expandDoubleTierR(double[] _source, int _R,
          int _prevR, int _C) {
        double[] target = new double[_R * _C];
        for (int i = 0; i < _C; ++i) {
            for (int j = 0; j < _prevR; ++j) {
                target[j * _C + i] = _source[j * _C + i];
            }
            for (int j = _prevR; j < _R; ++j) {
                target[j * _C + i] = 0;
            }
        }
        return target;
    }
}
