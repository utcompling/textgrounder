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

import opennlp.textgrounder.bayesian.ec.util.MersenneTwisterFast;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class TGRand {

    protected static MersenneTwisterFast mtfRand;

    /**
     * 
     * @param _mtfRand
     */
    public TGRand(MersenneTwisterFast _mtfRand) {
        mtfRand = _mtfRand;
    }

    public static double evalMuLikelihood(double[] _mu) {
        return 0;
    }

    public static double evalKappaLikelihood(double _kappa) {
        return 0;
    }

    public static double alphaUpdate(double _L, double _N, double _prevAlpha, double _d, double _f) {
        double q = RKRand.rk_beta(_prevAlpha + 1, _N);
        double pq = (_d + _L - 1) / (_N * (_f - Math.log(q)));
        int s = 0;
        if (mtfRand.nextDouble() < pq) {
            s = 1;
        }
        double alpha = RKRand.rk_gamma(_d + _L + s - 1, _f - Math.log(q));

        return alpha;
    }

    public static double[] sampleDirichlet(double[] _c0, int[] _n) {
        double[] hyp = new double[_n.length];
        for (int i = 0; i < _n.length; ++i) {
            hyp[i] = _c0[i] + _n[i];
        }
        double[] phi = dirichletRnd(hyp);
        return phi;
    }

    public static double[] sampleDirichlet(double _c0, int[] _n) {
        double[] hyp = new double[_n.length];
        for (int i = 0; i < _n.length; ++i) {
            hyp[i] = _c0 + _n[i];
        }
        double[] phi = dirichletRnd(hyp);
        return phi;
    }

    public static double[] sampleVMFMeans(double[] _mu, double _k) {
        double[] newmean = sampleVMF(_mu, _k);
        double u = evalMuLikelihood(newmean) / evalMuLikelihood(_mu);
        if (u > 1) {
            return newmean;
        } else {
            if (mtfRand.nextDouble() < u) {
                return newmean;
            } else {
                return _mu;
            }
        }
    }

    public static double sampleKappa(double _k, double _var) {
        double newk = _var * mtfRand.nextGaussian() + _k;
        double u = evalKappaLikelihood(newk) / evalKappaLikelihood(_k);
        if (u > 1) {
            return newk;
        } else {
            if (mtfRand.nextDouble() < u) {
                return newk;
            } else {
                return _k;
            }
        }
    }

    public static double[] sampleRestaurantStickBreakingWeights(double[] _alpha, double[] _wglob, double[] _nl) {
        double[] weights = new double[_wglob.length];
        double[] vl = new double[_wglob.length];
        double[] ivl = new double[_wglob.length];
        double[] ilvl = new double[_wglob.length];
        double[] wcs = TGMath.cumSum(_wglob);
        double[] incs = TGMath.inverseCumSum(_nl);

        for (int i = 0; i < _wglob.length - 1; ++i) {
            double a = _alpha[i] * _wglob[i] + _nl[i];
            double b = _alpha[i] * (1 - wcs[i]) + incs[i + 1];
            vl[i] = RKRand.rk_beta(a, b);
        }

        vl[_wglob.length] = 1;
        for (int i = 0; i < _wglob.length; ++i) {
            ilvl[i] = Math.log(1 - vl[i]);
        }
        ivl = TGMath.cumSum(ilvl);

        weights[0] = vl[0];
        for (int i = 1; i < _wglob.length; ++i) {
            weights[i] = Math.exp(Math.log(vl[i]) + ivl[i - 1]);
        }

        return weights;
    }

    public static double[] sampleGlobalStickBreakingWeights(double _alpha, double[] _wglob, double[] _n) {
        double[] weights = new double[_wglob.length];
        double[] v = new double[_wglob.length];
        double[] ivl = new double[_wglob.length];
        double[] ilvl = new double[_wglob.length];
        double[] incs = TGMath.inverseCumSum(_n);

        for (int i = 0; i < _wglob.length - 1; ++i) {
            double a = 1 + _n[i];
            double b = _alpha + incs[i + 1];
            v[i] = RKRand.rk_beta(a, b);
        }

        v[_wglob.length] = 1;
        for (int i = 0; i < _wglob.length; ++i) {
            ilvl[i] = Math.log(1 - v[i]);
        }
        ivl = TGMath.cumSum(ilvl);

        weights[0] = v[0];
        for (int i = 1; i < _wglob.length; ++i) {
            weights[i] = Math.exp(Math.log(v[i]) + ivl[i - 1]);
        }

        return weights;
    }

    public static double[] dirichletRnd(double[] _hyper) {
        double[] vals = new double[_hyper.length];
        for (int i = 0; i < _hyper.length; ++i) {
            vals[i] = RKRand.rk_gamma(_hyper[i], 1);
        }
        return vals;
    }

    public static double[] sampleUniformVMF() {
        double z = mtfRand.nextDouble() * 2 - 1;
        double t = mtfRand.nextDouble() * 2 * Math.PI;
        double z2 = 1 - z * z;
        double x = Math.sqrt(z2) * Math.cos(t);
        double y = Math.sqrt(z2) * Math.sin(t);

        return new double[]{x, y, z};
    }

    public static double[] sampleVMF(double[] _mu, double _kappa) {
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
