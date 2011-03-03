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

import opennlp.textgrounder.bayesian.ec.util.MersenneTwisterFast;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class TGRand {

    protected static MersenneTwisterFast mtfRand;

    public static void setMtfRand(MersenneTwisterFast mtfRand) {
        TGRand.mtfRand = mtfRand;
    }

    public static double[] dirichletRnd(double[] _hyper, int[] _n) {
        double[] vals = new double[_hyper.length];
        double s = 0;
        for (int i = 0; i < _hyper.length; ++i) {
            s += vals[i] = RKRand.rk_gamma(_hyper[i] + _n[i], 1);
        }
        s = Math.log(s);
        for (int i = 0; i < _n.length; ++i) {
            vals[i] = Math.exp(Math.log(vals[i]) - s);
        }
        return vals;
    }

    public static double[] dirichletRnd(double _hyper, int[] _n) {
        double[] vals = new double[_n.length];
        double s = 0;
        for (int i = 0; i < _n.length; ++i) {
            s += vals[i] = RKRand.rk_gamma(_hyper + _n[i], 1);
        }
        s = Math.log(s);
        for (int i = 0; i < _n.length; ++i) {
            vals[i] = Math.exp(Math.log(vals[i]) - s);
        }
        return vals;
    }

    public static double[] dirichletRnd(double[] _hyper) {
        double[] vals = new double[_hyper.length];
        double s = 0;
        for (int i = 0; i < _hyper.length; ++i) {
            s = vals[i] = RKRand.rk_gamma(_hyper[i], 1);
        }
        s = Math.log(s);
        for (int i = 0; i < _hyper.length; ++i) {
            vals[i] = Math.exp(Math.log(vals[i]) - s);
        }
        return vals;
    }

    public static double[] dirichletRnd(double _hyper, int _n) {
        double[] vals = new double[_n];
        double s = 0;
        for (int i = 0; i < _n; ++i) {
            s += vals[i] = RKRand.rk_gamma(_hyper, 1);
        }
        s = Math.log(s);
        for (int i = 0; i < _n; ++i) {
            vals[i] = Math.exp(Math.log(vals[i]) - s);
        }
        return vals;
    }

    public static double[] uniVMFRnd() {
        double z = mtfRand.nextDouble() * 2 - 1;
        double t = mtfRand.nextDouble() * 2 * Math.PI;
        double z2 = Math.sqrt(1 - z * z);
        double x = z2 * Math.cos(t);
        double y = z2 * Math.sin(t);

        return new double[]{x, y, z};
    }

    public static double[] vmfRnd(double[] _mu, double _kappa) {
        double[] smu = TGMath.cartesianToSpherical(_mu);

        double y = mtfRand.nextDouble();
        double c = 2 / Math.sinh(_kappa);
        double W = 1 / _kappa * Math.log(Math.exp(-_kappa) + c * y);
        double v = 2 * Math.PI * mtfRand.nextDouble();

        double[] untranslated = new double[]{Math.cos(v), Math.sin(v), W};
        double[] translated = rotateVector(untranslated, smu[0], smu[1]);

        return TGMath.sphericalToCartesian(translated);
    }

    /**
     * Using a right handed coordinate system, rotate a vector in 3d euclidian
     * coordinates by y axis (clockwise) and then z axis (clockwise)
     *
     * @param _vec
     * @return
     */
    protected static double[] rotateVector(double[] _vec, double _theta, double _phi) {
        double[] r = new double[3];
        double st = Math.sin(_theta);
        double ct = Math.cos(_theta);
        double sp = Math.sin(_phi);
        double cp = Math.cos(_phi);

        double x = _vec[0];
        double y = _vec[1];
        double z = _vec[2];
        r[0] = x * ct * cp - y * st + z * ct * sp;
        r[1] = x * st * cp + y * ct + z * st * sp;
        r[2] = -x * sp + z * cp;

        /**
         * The following is an alternate formulation where the axes
         * are rotated along the x axis (counter clockwise) then the z axis (clockwise)
         */
//        r[0] = x * ct + y * st * cp + z * st * sp;
//        r[1] = -x * st + y * ct * cp + z * ct * sp;
//        r[2] = -y * sp + z * sp;
        return r;
    }
}
