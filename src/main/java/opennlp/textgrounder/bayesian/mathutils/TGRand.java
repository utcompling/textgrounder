/* Copyright 2005 Robert Kern (robert.kern@gmail.com)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

/* The implementations of rk_hypergeometric_hyp(), rk_hypergeometric_hrua(),
 * and rk_triangular() were adapted from Ivan Frohne's rv.py which has this
 * license:
 *
 * Copyright 1998 by Ivan Frohne; Wasilla, Alaska, U.S.A.
 * All Rights Reserved
 *
 * Permission to use, copy, modify and distribute this software and its
 * documentation for any purpose, free of charge, is granted subject to the
 * following conditions:
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the software.
 *
 * THE SOFTWARE AND DOCUMENTATION IS PROVIDED WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR
 * OR COPYRIGHT HOLDER BE LIABLE FOR ANY CLAIM OR DAMAGES IN A CONTRACT
 * ACTION, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR ITS DOCUMENTATION.
 */
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

    public static void hypUpdate() {
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

    public static double[] dirichletRnd(double[] _hyper) {
        double[] vals = new double[_hyper.length];
        for (int i = 0; i < _hyper.length; ++i) {
            vals[i] = gammaRnd(_hyper[i], 1);
        }
        return vals;
    }

    /**
     * defined for where scale parameter beta is denominator, i.e.
     * <pr>
     * 1/gamma(a) * 1/b^a * x^(a-1) * e^(-x/b)
     * </pr>
     * where a=_alpha (shape parameter), b=_beta (scale parameter)
     * 
     * @param _alpha
     * @param _beta
     * @return
     */
    public static double gammaRnd(double _alpha, double _beta) {
        return 0;
//        return gamma.random(_alpha, _beta, dlRand);
    }

    public static double betaRnd(double _alpha, double _beta) {
        return 0;
//        return beta.random(_alpha, _beta, dlRand);
    }

    public static double gammaRnd2(double k, double theta) {

        boolean accept = false;
        if (k < 1) {
            // Weibull algorithm
            double c = (1 / k);
            double d = ((1 - k) * Math.pow(k, (k / (1 - k))));
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
            return (x * theta);
        } else {
            // Cheng's algorithm
            double b = (k - Math.log(4));
            double c = (k + Math.sqrt(2 * k - 1));
            double lam = Math.sqrt(2 * k - 1);
            double cheng = (1 + Math.log(4.5));
            double u, v, x, y, z, r;
            do {
                u = mtfRand.nextDouble();
                v = mtfRand.nextDouble();
                y = ((1 / lam) * Math.log(v / (1 - v)));
                x = (k * Math.exp(y));
                z = (u * v * v);
                r = (b + (c * y) - x);
                if ((r >= ((4.5 * z) - cheng))
                      || (r >= Math.log(z))) {
                    accept = true;
                }
            } while (!accept);
            return (x * theta);
        }
    }
}
