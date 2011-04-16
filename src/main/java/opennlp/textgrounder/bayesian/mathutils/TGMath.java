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

import java.util.Arrays;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class TGMath {

    public static double sphericalDensity(double[] _x, double[] _mu,
          double _kappa) {
        double d = 0;
        if (_kappa > 5) {
            d = 0.5 * _kappa / Math.PI * Math.exp(_kappa * TGBLAS.ddot(0, _x, 1, _mu, 1)
                  - _kappa);
        } else {
            d = _kappa * Math.exp(_kappa * TGBLAS.ddot(0, _x, 1, _mu, 1))
                  / (4 * Math.PI * Math.sinh(_kappa));
        }
        return d;
    }

    public static double logSphericalDensity(double[] _x, double[] _mu,
          double _kappa) {
        double d = 0;
        if (_kappa > 5) {
            d = Math.log(0.5 * _kappa / Math.PI) + _kappa * TGBLAS.ddot(0, _x, 1, _mu, 1) - _kappa;
        } else {
            d = Math.log(sphericalDensity(_x, _mu, _kappa));
        }
        return d;
    }

    public static double sphericalDensity(double _alpha, double _beta,
          double _theta, double _phi, double _kappa) {
        double d = 0;
        if (_kappa > 5) {
            d = 0.5 * _kappa / Math.PI * Math.exp(_kappa
                  * (Math.cos(_alpha) * Math.cos(_theta)
                  + Math.sin(_alpha) * Math.sin(_theta) * Math.cos(_phi - _beta)) - _kappa);
        } else {
            d = _kappa * Math.exp(_kappa
                  * (Math.cos(_alpha) * Math.cos(_theta)
                  + Math.sin(_alpha) * Math.sin(_theta) * Math.cos(_phi - _beta)))
                  / (4 * Math.PI * Math.sinh(_kappa));
        }
        return d;
    }

    public static double logSphericalDensity(double _alpha, double _beta,
          double _theta, double _phi, double _kappa) {
        double d = 0;
        if (_kappa > 5) {
            d = Math.log(0.5 * _kappa / Math.PI) + _kappa
                  * (Math.cos(_alpha) * Math.cos(_theta)
                  + Math.sin(_alpha) * Math.sin(_theta) * Math.cos(_phi - _beta))
                  - _kappa;
        } else {
            d = sphericalDensity(_alpha, _beta, _theta, _phi, _kappa);
        }
        return d;
    }

    /**
     * vMF density without constant normalization factor
     *
     * @param _x
     * @param _mu
     * @param _kappa
     * @return
     */
    public static double proportionalSphericalDensity(double[] _x, double[] _mu,
          double _kappa) {
        return Math.exp(_kappa * TGBLAS.ddot(0, _x, 1, _mu, 1));
    }

    public static double logProportionalSphericalDensity(double[] _x, double[] _mu,
          double _kappa) {
        return _kappa * TGBLAS.ddot(0, _x, 1, _mu, 1);
    }

    public static double normalizedProportionalSphericalDensity(double[] _x,
          double[] _mu, double _kappa) {
        double[] nrmmean = new double[3];
        Arrays.fill(nrmmean, 0);

        TGBLAS.daxpy(0, 1 / TGBLAS.dnrm2(0, _mu, 1), _mu, 1, nrmmean, 1);
        return Math.exp(_kappa * TGBLAS.ddot(0, _x, 1, nrmmean, 1));
    }

    public static double logNormalizedProportionalSphericalDensity(double[] _x,
          double[] _mu, double _kappa) {
        double[] nrmmean = new double[3];
        Arrays.fill(nrmmean, 0);

        TGBLAS.daxpy(0, 1 / TGBLAS.dnrm2(0, _mu, 1), _mu, 1, nrmmean, 1);
        return _kappa * TGBLAS.ddot(0, _x, 1, nrmmean, 1);
    }

    public static double[] normalizeVector(double[] _mu) {
        double[] nrmmean = new double[_mu.length];
        for (int i = 0; i < _mu.length; ++i) {
            nrmmean[i] = 0;
        }

        TGBLAS.daxpy(0, 1 / TGBLAS.dnrm2(0, _mu, 1), _mu, 1, nrmmean, 1);
        return nrmmean;
    }

    public static double proportionalSphericalDensity(double _alpha,
          double _beta,
          double _theta, double _phi, double _kappa) {
        return Math.exp(_kappa
              * (Math.cos(_alpha) * Math.cos(_theta)
              + Math.sin(_alpha) * Math.sin(_theta) * Math.cos(_phi - _beta)));
    }

    public static double[] sphericalToCartesian(double _theta, double _phi) {
        double[] cart = new double[3];
        cart[0] = Math.sin(_theta) * Math.cos(_phi);
        cart[1] = Math.sin(_theta) * Math.sin(_phi);
        cart[2] = Math.cos(_theta);
        return cart;
    }

    public static double[] sphericalToCartesian(double[] _coord) {
        return sphericalToCartesian(_coord[0], _coord[1]);
    }

    public static double[] geographicToSpherical(double _lat, double _long) {
        double[] geo = new double[2];
        double theta = latToRadians(_lat);
        double phi = longToRadians(_long);
        geo[0] = theta;
        geo[1] = phi;
        return geo;
    }

    /**
     * convert cartesian coordinate to spherical. first element is azimuthal,
     * second element is radial
     *
     * @param _x
     * @return
     */
    public static double[] cartesianToSpherical(double[] _x) {
        double[] spher = new double[2];
        spher[0] = Math.acos(_x[2]);
        spher[1] = Math.atan2(_x[1], _x[0]);
        return spher;
    }

    public static double[] sphericalToGeographic(double[] _spher) {
        double[] geo = new double[2];
        geo[0] = (_spher[0] / -Math.PI) * 180 + 90;
        geo[1] = (_spher[1] / Math.PI) * 180;
        return geo;
    }

    public static double[] cartesianToGeographic(double[] _x) {
        return sphericalToGeographic(cartesianToSpherical(_x));
    }

    public static double latToRadians(double _lat) {
        return (_lat / -180 + 0.5) * Math.PI;
    }

    public static double longToRadians(double _long) {
        return (_long / 180) * Math.PI;
    }

    public static double stableSum(double _prob1, double _prob2) {
        return Math.exp(stableLogSum(Math.log(_prob1), Math.log(_prob2)));
    }

    public static double stableSum(double[] _vals) {
        return stableSum(_vals, 0, _vals.length);
    }

    public static double stableSum(double[] _vals, int _n) {
        return stableSum(_vals, 0, _n);
    }

    public static double stableSum(double[] _vals, int _starti, int _endi) {
        double max = _vals[_starti];
        for (int i = _starti; i < _endi; ++i) {
            if (_vals[i] > max) {
                max = _vals[i];
            }
        }

        if (max == 0) {
            return 0;
        }

        max = Math.log(max);
        double p = 0;
        for (int i = _starti; i < _endi; ++i) {
            p += Math.exp(Math.log(_vals[i]) - max);
        }
        return Math.exp(max + Math.log(p));
    }

    public static double stableProd(double _val1, double _val2) {
        return Math.exp(Math.log(_val1) + Math.log(_val2));
    }

    public static double stableProd(double... _vals) {
        double sum = 0;
        for (double v : _vals) {
            sum += Math.log(v);
        }
        return Math.exp(sum);
    }

    public static double stableDiv(double _val1, double _val2) {
        return Math.exp(Math.log(_val1) - Math.log(_val2));
    }

    public static double stableLogSum(double _logprob1, double _logprob2) {
        if (Double.isInfinite(_logprob1) && Double.isInfinite(_logprob2)) {
            return _logprob1; // both prob1 and prob2 are 0, return log 0.
        }
        if (_logprob1 > _logprob2) {
            return _logprob1 + Math.log(1 + Math.exp(_logprob2 - _logprob1));
        } else {
            return _logprob2 + Math.log(1 + Math.exp(_logprob1 - _logprob2));
        }
    }

    public static double[] cumSum(double[] _vec) {
        double[] cs = new double[_vec.length];
        Arrays.fill(cs, 0);
        cs[0] = _vec[0];
        for (int i = 1; i < _vec.length; ++i) {
            cs[i] = cs[i - 1] + _vec[i];
        }
        return cs;
    }

    public static double[] stableCumProb(double[] _vec) {
        double[] cs = new double[_vec.length];
        Arrays.fill(cs, 0);
        cs[0] = _vec[0];
        for (int i = 1; i < _vec.length; ++i) {
            double val = stableSum(cs[i - 1], _vec[i]);
            if (val > 1) {
                cs[i] = 1;
            } else {
                cs[i] = val;
            }
        }
        return cs;
    }

    public static double[] stableInverseCumProb(double[] _vec) {
        double[] cs = new double[_vec.length];
        Arrays.fill(cs, 0);
        cs[_vec.length - 1] = _vec[_vec.length - 1];
        for (int i = _vec.length - 2; i >= 0; --i) {
            double val = stableSum(cs[i + 1], _vec[i]);
            if (val > 1) {
                cs[i] = 1;
            } else {
                cs[i] = val;
            }
        }
        return cs;
    }

    public static int[] inverseCumSum(int[] _vec) {
        int[] cs = new int[_vec.length];
        Arrays.fill(cs, 0);
        cs[_vec.length - 1] = _vec[_vec.length - 1];
        for (int i = _vec.length - 2; i >= 0; --i) {
            cs[i] = cs[i + 1] + _vec[i];
        }
        return cs;
    }

    /**
     * accumulate numbers in an array backwards. Not inclusive of _end. Inclusive
     * of _begin. _end must be bigger than _begin.
     *
     * @param _vec
     * @param _end
     * @param _begin
     * @return
     */
    public static int[] inverseCumSum(int[] _vec, int _begin, int _end) {
        int len = _end - _begin;
        int[] cs = new int[len];
        Arrays.fill(cs, 0);
        cs[len - 1] = _vec[_begin + len - 1];
        for (int i = len - 2; i >= 0; --i) {
            cs[i] = cs[i + 1] + _vec[_begin + i];
        }
        return cs;
    }
}
