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
//import DistLib.uniform;
//import DistLib.beta;
//import DistLib.gamma;
import java.util.Arrays;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class TGRand {

    public static double evalMuLikelihood(double[] _mu) {
        return 0;
    }

    public static double evalKappaLikelihood(double _kappa) {
        return 0;
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

    public static double alphaUpdate(double _L, double _N, double _prevAlpha, double _d, double _f) {
        double q = betaRnd(_prevAlpha + 1, _N);
        double pq = (_d + _L - 1) / (_N * (_f - Math.log(q)));
        int s = 0;
        if (mtfRand.nextDouble() < pq) {
            s = 1;
        }
        double alpha = gammaRnd(_d + _L + s - 1, _f - Math.log(q));

        return alpha;
    }

    public static double[] sampleDirichlet(double[] _c0, int[] _n) {
        double[] hyp = new double[_n.length];
        for(int i = 0; i < _n.length; ++i) {
            hyp[i] = _c0[i] + _n[i];
        }
        double[] phi = dirichletRnd(hyp);
        return phi;
    }

    public static double[] sampleDirichlet(double _c0, int[] _n) {
        double[] hyp = new double[_n.length];
        for(int i = 0; i < _n.length; ++i) {
            hyp[i] = _c0 + _n[i];
        }
        double[] phi = dirichletRnd(hyp);
        return phi;
    }

    public static double[] sampleVMFMeans(double[] _mu, double _k) {
        double[] newmean = vmfRnd(_mu, _k);
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
        double[] wcs = cumSum(_wglob);
        double[] incs = inverseCumSum(_nl);

        for (int i = 0; i < _wglob.length - 1; ++i) {
            double a = _alpha[i] * _wglob[i] + _nl[i];
            double b = _alpha[i] * (1 - wcs[i]) + incs[i + 1];
            vl[i] = betaRnd(a, b);
        }

        vl[_wglob.length] = 1;
        for (int i = 0; i < _wglob.length; ++i) {
            ilvl[i] = Math.log(1 - vl[i]);
        }
        ivl = cumSum(ilvl);

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
        double[] incs = inverseCumSum(_n);

        for (int i = 0; i < _wglob.length - 1; ++i) {
            double a = 1 + _n[i];
            double b = _alpha + incs[i + 1];
            v[i] = betaRnd(a, b);
        }

        v[_wglob.length] = 1;
        for (int i = 0; i < _wglob.length; ++i) {
            ilvl[i] = Math.log(1 - v[i]);
        }
        ivl = cumSum(ilvl);

        weights[0] = v[0];
        for (int i = 1; i < _wglob.length; ++i) {
            weights[i] = Math.exp(Math.log(v[i]) + ivl[i - 1]);
        }

        return weights;
    }
    protected static MersenneTwisterFast mtfRand;
//    protected static DistLib.uniform dlRand = new uniform();

    public static double[] vmfRnd(double[] _mu, double _k) {
        return null;
    }

    public static double[] dirichletRnd(double[] _hyper) {
        double[] vals = new double[_hyper.length];
        for (int i = 0; i < _hyper.length; ++i) {
            vals[i] = gammaRnd(_hyper[i], 1);
        }
        return vals;
    }

    /** Generate a random number from a beta random variable.
     ** @param a    First parameter of the Beta random variable.
     ** @param b    Second parameter of the Beta random variable.
     ** @return     A double.
     */
    public static double betaRnd(double _alpha, double _beta) {

        double try_x;
        double try_y;
        do {
            try_x = Math.pow(mtfRand.nextDouble(), 1 / _alpha);
            try_y = Math.pow(mtfRand.nextDouble(), 1 / _beta);
        } while ((try_x + try_y) > 1);
        return try_x / (try_x + try_y);

        //        return beta.random(_alpha, _beta, dlRand);
    }

    /**
     * defined for gamma dist where _scale is denominator
     * @param _shape
     * @param _scale
     * @return
     */
    public static double gammaRnd(double _shape, double _scale) {

        boolean accept = false;
        if (_shape < 1) {
            // Weibull algorithm
            double c = (1 / _shape);
            double d = ((1 - _shape) * Math.pow(_shape, (_shape / (1 - _shape))));
            double u, v, z, e, x;
            do {
                u = mtfRand.nextDouble();
                v = mtfRand.nextDouble();
                z = -Math.log(u);
                e = -Math.log(v);
                x = Math.pow(z, c);
                if ((z + e) >= (d + x)) {
                    accept = true;
                }
            } while (!accept);
            return (x * _scale);
        } else {
            // Cheng's algorithm
            double b = (_shape - Math.log(4));
            double c = (_shape + Math.sqrt(2 * _shape - 1));
            double lam = Math.sqrt(2 * _shape - 1);
            double cheng = (1 + Math.log(4.5));
            double u, v, x, y, z, r;
            do {
                u = mtfRand.nextDouble();
                v = mtfRand.nextDouble();
                y = ((1 / lam) * Math.log(v / (1 - v)));
                x = (_shape * Math.exp(y));
                z = (u * v * v);
                r = (b + (c * y) - x);
                if ((r >= ((4.5 * z) - cheng))
                      || (r >= Math.log(z))) {
                    accept = true;
                }
            } while (!accept);
            return (x * _scale);
        }
    }
}
