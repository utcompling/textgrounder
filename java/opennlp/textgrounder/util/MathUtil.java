///////////////////////////////////////////////////////////////////////////////
// Copyright (C) 2007 Jason Baldridge, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.util;

import java.util.Arrays;

/**
 * Handy functions for common mathematical operations on data
 * structures, plus computations in log-space.
 *
 * @author  Jason Baldridge
 * @version $Revision: 1.53 $, $Date: 2006/10/12 21:20:44 $
 */
public class MathUtil {

    public static double LOG_ZERO = Double.NEGATIVE_INFINITY;

    public static double eexp (double x) { 
	if (Double.isInfinite(x))
	    return 0.0;
	else
	    return Math.exp(x);
    }

    public static double elog (double x) { 
	if (x == 0.0)
	    return LOG_ZERO;
	else
	    return Math.log(x);
    }

    public static double elogProduct(double ... values) {
	double sum = 0.0;
	for (double x: values) {
	    if (Double.isInfinite(x))
		return LOG_ZERO;
	    else
		sum += x;
	}
	return sum;
    }

    public static double elogSum(double x, double y) {
	if (Double.isInfinite(x))
	    return y;
	else if (Double.isInfinite(y))
	    return x;
	else {
	    if (x > y)
		return x + Math.log1p(Math.exp(y-x));
//		return x + Math.log(1 + Math.exp(y-x));
	    else
		return x + Math.log1p(Math.exp(x-y));
//		return y + Math.log(1 + Math.exp(x-y));
	}
    }

    public static void logNormalize(double[] vals) {
	double total = sum(vals);
	for (int i=0; i<vals.length; i++)
	    vals[i] = elog(vals[i]/total);
    }

    public static double sum (double[] values) {
	double total = 0.0;
	for (int i=0; i<values.length; i++)
	    total += values[i];
	return total;
    }

    public static double entropy(double[] vals) {
	double entropy = 0.0;
	for (int i=0; i<vals.length; i++)
	    entropy -= vals[i]*Math.log(vals[i]);
	return entropy;
    }

    public static double entropyOfLogDistribution(double[] vals) {
	double entropy = 0.0;
	for (int i=0; i<vals.length; i++)
	    entropy -= vals[i]*Math.exp(vals[i]);
	return entropy;
    }

    public static final int getIndexOfHighest (double[] vals) {
	int highestIndex = 0;
	double highestVal = vals[0];
	for (int i=1; i<vals.length; i++) {
	    if (vals[i] > highestVal) {
		highestVal = vals[i];
		highestIndex = i;
	    }
	}
	return highestIndex;
    }

    public static final int[] rankScores (double[] vals) {
	IntDoublePair[] paired = new IntDoublePair[vals.length];
	for (int i=0; i<vals.length; i++)
	    paired[i] = new IntDoublePair(i, vals[i]);

	Arrays.sort(paired);

	int[] ranked = new int[vals.length];
	for (int i=0; i<vals.length; i++)
	    ranked[i] = paired[i].intValue;

	return ranked;
    }

    public static final void exponentiate (double[] vals) {
	for (int i=0; i<vals.length; i++)
	    vals[i] = Math.exp(vals[i]);
    }


    public static final void takeLogarithm (double[] vals) {
	for (int i=0; i<vals.length; i++)
	    vals[i] = Math.log(vals[i]);
    }

    public static final double[] uniformLogDistribution (int numValues) {
	double[] probs = new double[numValues];
	Arrays.fill(probs, Math.log(1.0/numValues));
	return probs;
    }

    public static final void normalize (double[] vals) {
	double total = 0.0;
	for (int i=0; i<vals.length; i++)
	    total += vals[i];
	for (int i=0; i<vals.length; i++)
	    vals[i] = vals[i]/total;
    }

    public static final String printProbabilities (double[] vals) {
	StringBuffer sb = new StringBuffer();
	sb.append('[');
	sb.append(Constants.PERCENT_FORMAT.format(vals[0]));
	for (int i=1; i<vals.length; i++)
	    sb.append(',').append(Constants.PERCENT_FORMAT.format(vals[i]));
	sb.append(']');
	return sb.toString();
    }


}
