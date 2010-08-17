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
package opennlp.textgrounder.bayesian.mathutils;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class TGBLAS {

    /**
     * compute dot product of two vectors
     *
     * @param N length of vectors
     * @param X vector of doubles
     * @param incX step size between elements in X. in our case, just default this to one
     * @param Y vector of doubles
     * @param incY step size between elements in Y. in our case, just default this to one
     * @return dot product of two vectors
     */
    public static double ddot(int N, double[] X, int incX, double[] Y,
          int incY) {
        double sum = 0;
        try {
            for (int i = 0;; ++i) {
                sum += X[i] * Y[i];
            }
        } catch (ArrayIndexOutOfBoundsException e) {
        }
        return sum;
    }

    /**
     * scale vector, then add to another vector
     *
     * @param N length of vectors
     * @param alpha scaling factor for X
     * @param X vector of doubles
     * @param incX step size between elements in X. in our case, just default this to one
     * @param Y vector of doubles
     * @param incY step size between elements in Y. in our case, just default this to one
     */
    public static void daxpy(int N, double alpha, double[] X, int incX,
          double[] Y, int incY) {
        try {
            for (int i = 0;; ++i) {
                Y[i] += alpha * X[i];
            }
        } catch (ArrayIndexOutOfBoundsException e) {
        }
    }

    public static double dnrm2(int N, double[] X, int incX) {
        double sum = 0;
        try {
            for (int i = 0;; ++i) {
                sum += Math.pow(X[i], 2);
            }
        } catch (ArrayIndexOutOfBoundsException e) {
        }
        return Math.sqrt(sum);
    }
}
