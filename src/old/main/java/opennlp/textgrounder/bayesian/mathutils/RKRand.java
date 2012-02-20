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
///////////////////////////////////////////////////////////////////////////////
//  
//  Conversion of all rk functions to java, with attendant modifications to
//  work with MersenneTwisterFast
//
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
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class RKRand {

    public static class BetaEdgeException extends Exception {

        private static final long serialVersionUID = 42L;

        public BetaEdgeException() {
            super("The beta random number generator drew a one");
        }
    }
    protected static double M_PI = 3.14159265358979323846264338328;
    protected static MersenneTwisterFast mtfRand;

    public static void setMtfRand(MersenneTwisterFast mtfRand) {
        RKRand.mtfRand = mtfRand;
    }

    /* log-gamma function to support some of these distributions. The
     * algorithm comes from SPECFUN by Shanjie Zhang and Jianming Jin and their
     * book "Computation of Special Functions", 1996, John Wiley & Sons, Inc.
     */
    public static double loggam(double x) {
        double x0, x2, xp, gl, gl0;
        int k, n;

        double[] a = new double[]{8.333333333333333e-02, -2.777777777777778e-03,
            7.936507936507937e-04, -5.952380952380952e-04,
            8.417508417508418e-04, -1.917526917526918e-03,
            6.410256410256410e-03, -2.955065359477124e-02,
            1.796443723688307e-01, -1.39243221690590e+00};
        x0 = x;
        n = 0;
        if ((x == 1.0) || (x == 2.0)) {
            return 0.0;
        } else if (x <= 7.0) {
            n = (int) (7 - x);
            x0 = x + n;
        }
        x2 = 1.0 / (x0 * x0);
        xp = 2 * M_PI;
        gl0 = a[9];
        for (k = 8; k >= 0; k--) {
            gl0 *= x2;
            gl0 += a[k];
        }
        gl = gl0 / x0 + 0.5 * Math.log(xp) + (x0 - 0.5) * Math.log(x0) - x0;
        if (x <= 7.0) {
            for (k = 1; k <= n; k++) {
                gl -= Math.log(x0 - 1.0);
                x0 -= 1.0;
            }
        }
        return gl;
    }

    public static double rk_normal(double loc, double scale) {
        return loc + scale * mtfRand.nextGaussian();
    }

    public static double rk_standard_exponential() {
        /* We use -log(1-U) since U is [0, 1) */
        return -Math.log(1.0 - mtfRand.nextDouble());
    }

    public static double rk_exponential(double scale) {
        return scale * rk_standard_exponential();
    }

    public static double rk_uniform(double loc, double scale) {
        return loc + scale * mtfRand.nextDouble();
    }

    public static double rk_standard_gamma(double shape) {
        double b, c;
        double U, V, X, Y;

        if (shape == 1.0) {
            return rk_standard_exponential();
        } else if (shape < 1.0) {
            for (;;) {
                U = mtfRand.nextDouble();
                V = rk_standard_exponential();
                if (U <= 1.0 - shape) {
                    X = Math.pow(U, 1. / shape);
                    if (X <= V) {
                        return X;
                    }
                } else {
                    Y = -Math.log((1 - U) / shape);
                    X = Math.pow(1.0 - shape + shape * Y, 1. / shape);
                    if (X <= (V + Y)) {
                        return X;
                    }
                }
            }
        } else {
            b = shape - 1. / 3.;
            c = 1. / Math.sqrt(9 * b);
            for (;;) {
                do {
                    X = mtfRand.nextGaussian();
                    V = 1.0 + c * X;
                } while (V <= 0.0);

                V = V * V * V;
                U = mtfRand.nextDouble();
                if (U < 1.0 - 0.0331 * (X * X) * (X * X)) {
                    return (b * V);
                }
                if (Math.log(U) < 0.5 * X * X + b * (1. - V + Math.log(V))) {
                    return (b * V);
                }
            }
        }
    }

    public static double rk_gamma(double shape, double scale) {
        return scale * rk_standard_gamma(shape);
    }

    public static double rk_beta(double a, double b) throws BetaEdgeException {
        double Ga, Gb;

        if ((a <= 0.1) || (b <= 0.1)) {
            double U, V, X, Y, Z;
            /* Use Jonk's algorithm */

            while (true) {
                U = mtfRand.nextDouble();
                V = mtfRand.nextDouble();
                X = 1 / a * Math.log(U);
                Y = 1 / b * Math.log(V);
                Z = TGMath.stableLogSum(X, Y);

                if (Math.exp(Z) <= 1.0) {
                    double val = Math.exp(X - Z);
                    if (1 - val < 1e-10) {
                        throw new BetaEdgeException();
                    }
                    return val;
                }
            }
        } else if ((a <= 1.0) && (b <= 1.0)) {
            double U, V, X, Y;
            /* Use Jonk's algorithm */

            while (true) {
                U = mtfRand.nextDouble();
                V = mtfRand.nextDouble();
                X = Math.pow(U, 1.0 / a);
                Y = Math.pow(V, 1.0 / b);

                if ((X + Y) <= 1.0) {
                    return X / (X + Y);
                }
            }
        } else {
            Ga = rk_standard_gamma(a);
            Gb = rk_standard_gamma(b);
            return Ga / (Ga + Gb);
        }
    }

    public static double rk_chisquare(double df) {
        return 2.0 * rk_standard_gamma(df / 2.0);
    }

    public static double rk_noncentral_chisquare(double df, double nonc) {
        double Chi2, N;

        Chi2 = rk_chisquare(df - 1);
        N = mtfRand.nextGaussian() + Math.sqrt(nonc);
        return Chi2 + N * N;
    }

    public static double rk_f(double dfnum, double dfden) {
        return ((rk_chisquare(dfnum) * dfden)
              / (rk_chisquare(dfden) * dfnum));
    }

    public static double rk_noncentral_f(double dfnum, double dfden, double nonc) {
        return ((rk_noncentral_chisquare(dfnum, nonc) * dfden)
              / (rk_chisquare(dfden) * dfnum));
    }

    public static long rk_negative_binomial(double n, double p) {
        double Y;

        Y = rk_gamma(n, (1 - p) / p);
        return rk_poisson(Y);
    }

    public static long rk_poisson_mult(double lam) {
        long X;
        double prod, U, enlam;

        enlam = Math.exp(-lam);
        X = 0;
        prod = 1.0;
        while (true) {
            U = mtfRand.nextDouble();
            prod *= U;
            if (prod > enlam) {
                X += 1;
            } else {
                return X;
            }
        }
    }
    //////////////////////////////////////////////////////////////////
    /**
     * 
     */
    protected static double LS2PI = 0.91893853320467267;
    protected static double TWELFTH = 0.083333333333333333333333;

    public static long rk_poisson_ptrs(double lam) {
        long k;
        double U, V, slam, loglam, a, b, invalpha, vr, us;

        slam = Math.sqrt(lam);
        loglam = Math.log(lam);
        b = 0.931 + 2.53 * slam;
        a = -0.059 + 0.02483 * b;
        invalpha = 1.1239 + 1.1328 / (b - 3.4);
        vr = 0.9277 - 3.6224 / (b - 2);

        while (true) {
            U = mtfRand.nextDouble() - 0.5;
            V = mtfRand.nextDouble();
            us = 0.5 - Math.abs(U);
            k = (long) Math.floor((2 * a / us + b) * U + lam + 0.43);
            if ((us >= 0.07) && (V <= vr)) {
                return k;
            }
            if ((k < 0)
                  || ((us < 0.013) && (V > us))) {
                continue;
            }
            if ((Math.log(V) + Math.log(invalpha) - Math.log(a / (us * us) + b))
                  <= (-lam + k * loglam - loggam(k + 1))) {
                return k;
            }


        }

    }

    public static long rk_poisson(double lam) {
        if (lam >= 10) {
            return rk_poisson_ptrs(lam);
        } else if (lam == 0) {
            return 0;
        } else {
            return rk_poisson_mult(lam);
        }
    }

    public static double rk_standard_cauchy() {
        return mtfRand.nextGaussian() / mtfRand.nextGaussian();
    }

    public static double rk_standard_t(double df) {
        double N, G, X;

        N = mtfRand.nextGaussian();
        G = rk_standard_gamma(df / 2);
        X = Math.sqrt(df / 2) * N / Math.sqrt(G);
        return X;
    }

    /* Uses the rejection algorithm compared against the wrapped Cauchy
    distribution suggested by Best and Fisher and documented in
    Chapter 9 of Luc's Non-Uniform Random Variate Generation.
    http://cg.scs.carleton.ca/~luc/rnbookindex.html
    (but corrected to match the algorithm in R and Python)
     */
    public static double rk_vonmises(double mu, double kappa) {
        double r, rho, s;
        double U, V, W, Y, Z;
        double result, mod;
        boolean neg;

        if (kappa < 1e-8) {
            return M_PI * (2 * mtfRand.nextDouble() - 1);
        } else {
            r = 1 + Math.sqrt(1 + 4 * kappa * kappa);
            rho = (r - Math.sqrt(2 * r)) / (2 * kappa);
            s = (1 + rho * rho) / (2 * rho);

            while (true) {
                U = mtfRand.nextDouble();
                Z = Math.cos(M_PI * U);
                W = (1 + s * Z) / (s + Z);
                Y = kappa * (s - W);
                V = mtfRand.nextDouble();
                if ((Y * (2 - Y) - V >= 0) || (Math.log(Y / V) + 1 - Y >= 0)) {
                    break;
                }
            }

            U = mtfRand.nextDouble();

            result = Math.acos(W);
            if (U < 0.5) {
                result = -result;
            }
            result += mu;
            neg = (result < 0);
            mod = Math.abs(result);
            mod = ((mod + M_PI) % (2 * M_PI)) - M_PI;
            if (neg) {
                mod *= -1;
            }

            return mod;
        }
    }

    public static double rk_pareto(double a) {
        return Math.exp(rk_standard_exponential() / a) - 1;
    }

    public static double rk_weibull(double a) {
        return Math.pow(rk_standard_exponential(), 1. / a);
    }

    public static double rk_power(double a) {
        return Math.pow(1 - Math.exp(-rk_standard_exponential()), 1. / a);
    }

    public static double rk_laplace(double loc, double scale) {
        double U;

        U = mtfRand.nextDouble();
        if (U < 0.5) {
            U = loc + scale * Math.log(U + U);
        } else {
            U = loc - scale * Math.log(2.0 - U - U);
        }
        return U;
    }

    public static double rk_gumbel(double loc, double scale) {
        double U;

        U = 1.0 - mtfRand.nextDouble();
        return loc - scale * Math.log(-Math.log(U));
    }

    public static double rk_logistic(double loc, double scale) {
        double U;

        U = mtfRand.nextDouble();
        return loc + scale * Math.log(U / (1.0 - U));
    }

    public static double rk_lognormal(double mean, double sigma) {
        return Math.exp(rk_normal(mean, sigma));
    }

    public static double rk_rayleigh(double mode) {
        return mode * Math.sqrt(-2.0 * Math.log(1.0 - mtfRand.nextDouble()));
    }

    public static double rk_wald(double mean, double scale) {
        double U, X, Y;
        double mu_2l;

        mu_2l = mean / (2 * scale);
        Y = mtfRand.nextGaussian();
        Y = mean * Y * Y;
        X = mean + mu_2l * (Y - Math.sqrt(4 * scale * Y + Y * Y));
        U = mtfRand.nextDouble();
        if (U <= mean / (mean + X)) {
            return X;
        } else {
            return mean * mean / X;
        }
    }

    public static long rk_zipf(double a) {
        double T, U, V;
        long X;
        double am1, b;

        am1 = a - 1.0;
        b = Math.pow(2.0, am1);
        do {
            U = 1.0 - mtfRand.nextDouble();
            V = mtfRand.nextDouble();
            X = (long) Math.floor(Math.pow(U, -1.0 / am1));
            /* The real result may be above what can be represented in a signed
             * long. It will get casted to -sys.maxint-1. Since this is
             * a straightforward rejection algorithm, we can just reject this value
             * in the rejection condition below. This function then models a Zipf
             * distribution truncated to sys.maxint.
             */
            T = Math.pow(1.0 + 1.0 / X, am1);
        } while (((V * X * (T - 1.0) / (b - 1.0)) > (T / b)) || X < 1);
        return X;
    }

    public static int rk_geometric_search(double p) {
        double U;
        int X;
        double sum, prod, q;

        X = 1;
        sum = prod = p;
        q = 1.0 - p;
        U = mtfRand.nextDouble();
        while (U > sum) {
            prod *= q;
            sum += prod;
            X++;
        }
        return X;
    }

    public static int rk_geometric_inversion(double p) {
        return (int) Math.ceil(Math.log(1.0 - mtfRand.nextDouble()) / Math.log(1.0 - p));
    }

    public static int rk_geometric(double p) {
        if (p >= 0.333333333333333333333333) {
            return rk_geometric_search(p);
        } else {
            return rk_geometric_inversion(p);
        }
    }

    public static long rk_hypergeometric_hyp(long good, long bad, long sample) {
        long d1, K, Z;
        double d2, U, Y;

        d1 = bad + good - sample;
        d2 = (double) Math.min(bad, good);

        Y = d2;
        K = sample;
        while (Y > 0.0) {
            U = mtfRand.nextDouble();
            Y -= (long) Math.floor(U + Y / (d1 + K));
            K--;
            if (K == 0) {
                break;
            }
        }
        Z = (long) (d2 - Y);
        if (good > bad) {
            Z = sample - Z;
        }
        return Z;
    }

    /* D1 = 2*Math.sqrt(2/e) */
    /* D2 = 3 - 2*Math.sqrt(3/e) */
    protected static double D1 = 1.7155277699214135;
    protected static double D2 = 0.8989161620588988;

    public static long rk_hypergeometric_hrua(long good, long bad, long sample) {
        long mingoodbad, maxgoodbad, popsize, m, d9;
        double d4, d5, d6, d7, d8, d10, d11;
        long Z;
        double T, W, X, Y;

        mingoodbad = Math.min(good, bad);
        popsize = good + bad;
        maxgoodbad = Math.max(good, bad);
        m = Math.min(sample, popsize - sample);
        d4 = ((double) mingoodbad) / popsize;
        d5 = 1.0 - d4;
        d6 = m * d4 + 0.5;
        d7 = Math.sqrt((popsize - m) * sample * d4 * d5 / (popsize - 1) + 0.5);
        d8 = D1 * d7 + D2;
        d9 = (long) Math.floor((double) ((m + 1) * (mingoodbad + 1)) / (popsize + 2));
        d10 = (loggam(d9 + 1) + loggam(mingoodbad - d9 + 1) + loggam(m - d9 + 1)
              + loggam(maxgoodbad - m + d9 + 1));
        d11 = Math.min(Math.min(m, mingoodbad) + 1.0, Math.floor(d6 + 16 * d7));
        /* 16 for 16-decimal-digit precision in D1 and D2 */

        while (true) {
            X = mtfRand.nextDouble();
            Y = mtfRand.nextDouble();
            W = d6 + d8 * (Y - 0.5) / X;

            /* fast rejection: */
            if ((W < 0.0) || (W >= d11)) {
                continue;
            }

            Z = (long) Math.floor(W);
            T = d10 - (loggam(Z + 1) + loggam(mingoodbad - Z + 1) + loggam(m - Z + 1)
                  + loggam(maxgoodbad - m + Z + 1));

            /* fast acceptance: */
            if ((X * (4.0 - X) - 3.0) <= T) {
                break;
            }

            /* fast rejection: */
            if (X * (X - T) >= 1) {
                continue;
            }

            if (2.0 * Math.log(X) <= T) {
                break; /* acceptance */
            }
        }

        /* this is a correction to HRUA* by Ivan Frohne in rv.py */
        if (good > bad) {
            Z = m - Z;
        }

        /* another fix from rv.py to allow sample to exceed popsize/2 */
        if (m < sample) {
            Z = good - Z;
        }

        return Z;
    }

    public static long rk_hypergeometric(long good, long bad, long sample) {
        if (sample > 10) {
            return rk_hypergeometric_hrua(good, bad, sample);
        } else {
            return rk_hypergeometric_hyp(good, bad, sample);
        }
    }

    public static double rk_triangular(double left, double mode, double right) {
        double base, leftbase, ratio, leftprod, rightprod;
        double U;

        base = right - left;
        leftbase = mode - left;
        ratio = leftbase / base;
        leftprod = leftbase * base;
        rightprod = (right - mode) * base;

        U = mtfRand.nextDouble();
        if (U <= ratio) {
            return left + Math.sqrt(U * leftprod);
        } else {
            return right - Math.sqrt((1.0 - U) * rightprod);
        }
    }

    public static long rk_logseries(double p) {
        double q, r, U, V;
        long result;

        r = Math.log(1.0 - p);

        while (true) {
            V = mtfRand.nextDouble();
            if (V >= p) {
                return 1;
            }
            U = mtfRand.nextDouble();
            q = 1.0 - Math.exp(r * U);
            if (V <= q * q) {
                result = (long) Math.floor(1 + Math.log(V) / Math.log(q));
                if (result < 1) {
                    continue;
                } else {
                    return result;
                }
            }
            if (V >= q) {
                return 1;
            }
            return 2;
        }
    }
}
