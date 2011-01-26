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
        return _kappa * Math.exp(_kappa * TGBLAS.ddot(0, _x, 1, _mu, 1)) / (4 * Math.PI * Math.sinh(_kappa));
    }

    public static double sphericalDensity(double _alpha, double _beta,
          double _theta, double _phi, double _kappa) {
        return _kappa * Math.exp(_kappa
              * (Math.cos(_alpha) * Math.cos(_theta)
              + Math.sin(_alpha) * Math.sin(_theta) * Math.cos(_phi - _beta)))
              / (4 * Math.PI * Math.sinh(_kappa));
    }

    public static double proportionalSphericalDensity(double[] _x, double[] _mu,
          double _kappa) {
        return Math.exp(_kappa * TGBLAS.ddot(0, _x, 1, _mu, 1));
    }

    public static double unnormalizedProportionalSphericalDensity(double[] _x,
          double[] _mu, double _kappa) {
        double[] nrmmean = new double[3];
        Arrays.fill(nrmmean, 0);

        TGBLAS.daxpy(0, 1 / TGBLAS.dnrm2(0, _mu, 1), _mu, 1, nrmmean, 1);
        return Math.exp(_kappa * TGBLAS.ddot(0, _x, 1, nrmmean, 1));
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

    public static double safeLogSum(double[] _vals, int _n) {
        double max = _vals[0];
        for (int i = 0; i < _n; ++i) {
            if (_vals[i] > max) {
                max = _vals[i];
            }
        }

        if (Double.isInfinite(max)) // the largest probability is 0 (log prob= -inf)
        {
            return max;   // return log 0
        }

        double p = 0;
        for (int i = 0; i < _n; ++i) {
            p += Math.exp(_vals[i] - max);
        }
        return max + Math.log(p);
    }

    public static double[] cumLogSum(double[] _vec) {
        double[] lvec = new double[_vec.length];
        for (int i = 0; i < _vec.length; ++i) {
            lvec[i] = Math.log(_vec[i]);
        }

        double[] cs = new double[_vec.length];
        for (int i = 1; i < _vec.length + 1; ++i) {
            cs[i - 1] = Math.exp(safeLogSum(lvec, i));
        }

        return cs;
    }

    public static double[] inverseCumLogSum(double[] _vec) {
        double[] lvec = new double[_vec.length];
        for (int i = 0; i < _vec.length; ++i) {
            lvec[i] = Math.log(_vec[i]);
        }

        double[] cs = new double[_vec.length];
        cs[_vec.length - 1] = _vec[_vec.length - 1];
        for (int i = _vec.length - 2; i >= 0; --i) {
            cs[i] = cs[i + 1] + _vec[i];
        }
        return cs;
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

    public static double[] inverseCumSum(double[] _vec) {
        double[] cs = new double[_vec.length];
        Arrays.fill(cs, 0);
        cs[_vec.length - 1] = _vec[_vec.length - 1];
        for (int i = _vec.length - 2; i >= 0; --i) {
            cs[i] = cs[i + 1] + _vec[i];
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
}
