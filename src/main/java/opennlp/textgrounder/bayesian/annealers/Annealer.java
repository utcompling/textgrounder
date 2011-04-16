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
package opennlp.textgrounder.bayesian.annealers;

import opennlp.textgrounder.bayesian.apps.ExperimentParameters;
import opennlp.textgrounder.bayesian.mathutils.TGMath;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public abstract class Annealer {

    /**
     * epsilon for comparing equality in floating point numbers.
     */
    public static final double EPSILON = 1e-6;
    /**
     * Exponent in the annealing process. Is the reciprocal of the temperature.
     */
    protected double temperatureReciprocal = 1;
    /**
     * Temperature at which to start annealing process
     */
    protected double initialTemperature = 1;
    /**
     * Decrement at which to reduce the temperature in annealing process
     */
    protected double temperatureDecrement = 0.1;
    /**
     * Stop changing temperature after the following temp has been reached.
     */
    protected double targetTemperature = 1;
    /**
     * Current inner iteration count
     */
    protected int innerIter = 0;
    /**
     * Current outer iteration count
     */
    protected int outerIter = 0;
    /**
     * Number of inner innerIter. This corresponds to the number of innerIter
     * per temperature decrement
     */
    protected int innerIterationsMax;
    /**
     * Number of outer innerIter. This is calculated based on the start
     * temperature minus the target temperature divided by the temperature
     * decrement.
     */
    protected int outerIterationsMax;
    /**
     * Number of samples to take after burn-in
     */
    protected int samples = 0;
    /**
     * Number of innerIter between samples
     */
    protected int lag = 0;
    /**
     * The current temperature
     */
    protected double temperature = 1;
    /**
     * To sample or not to sample
     */
    protected boolean sampleiteration = false;
    /**
     * Indicates whether sample collection is done or not
     */
    protected boolean finishedCollection = false;
    /**
     * Number of samples collected
     */
    protected int sampleCount = 0;

    protected Annealer() {
    }

    public Annealer(ExperimentParameters _experimentParameters) {
        initialTemperature = _experimentParameters.getInitialTemperature();
        temperature = initialTemperature;
        temperatureReciprocal = 1 / temperature;
        temperatureDecrement = _experimentParameters.getTemperatureDecrement();
        targetTemperature = _experimentParameters.getTargetTemperature();
        innerIterationsMax = _experimentParameters.getBurnInIterations();
        outerIterationsMax =
              (int) Math.round((initialTemperature - targetTemperature)
              / temperatureDecrement) + 1;
        samples = _experimentParameters.getSamples();
        lag = _experimentParameters.getLag();
    }

    /**
     * The temperature changes in floating point increments. There is a later
     * need to check whether the temperature is equal to one or not during
     * the training process. If the temperature is close enough to one,
     * this will set the temperature to one.
     *
     * @return Whether temperature has been set to one
     */
    public boolean stabilizeTemperature() {
        if (Math.abs(temperatureReciprocal - 1) < EPSILON) {
            System.err.println("Temperature stabilized to 1!");
            temperatureReciprocal = 1;
            return true;
        } else {
            return false;
        }
    }

    /**
     * Calculate annealed probabilities. Take an array of numbers, normalize
     * into probabilities, raise to the power of the current annealing temperature,
     * then renormalize. Return the sum of this array. Generally, it will be
     * one, but might not be if the annealing temperature is one.
     *
     * @param starti Offset of first populated array
     * @param classes Array of class likelihoods (unnormalized)
     * @return Sum of array values
     */
    public abstract double annealProbs(int starti, double[] classes);

    /**
     * Counts the number of innerIter. It decrements the number of innerIter
     * after each iteration. It maintains both the inner and outer loop
     *
     * @return boolean of whether there is a next iteration or not
     */
    public boolean nextIter() {
        if (outerIter == outerIterationsMax) {
            System.err.println("");
            System.err.println("Burn in complete!");
            if (samples != 0 && !finishedCollection) {
                System.err.println("Beginning sampling!");

                outerIter = 0;
                innerIter = 1;

                temperatureReciprocal = 1. / targetTemperature;
                innerIterationsMax = samples * lag;
                outerIterationsMax = 1;
                sampleiteration = true;

                return true;
            } else {
                return false;
            }
        } else {
            if (innerIter == innerIterationsMax) {
                outerIter++;
                if (outerIter == outerIterationsMax) {
                    System.err.print("\n");
                    return nextIter();
                }
                innerIter = 0;
                temperature -= temperatureDecrement;
                temperatureReciprocal = 1 / temperature;
                stabilizeTemperature();
                System.err.println(String.format("Outer iteration: %d (temperature: %.2f)", outerIter, temperature));
                System.err.print("Inner iteration: ");
                innerIter += 1;
            } else {
                if (innerIter == 0) {
                    System.err.println(String.format("Outer iteration: %d (temperature: %.2f)", outerIter, temperature));
                    System.err.print("Inner iteration: ");
                }
                System.err.print(innerIter + ",");
                innerIter += 1;
            }
            return true;
        }
    }

    /**
     * Anneal an array of probabilities. For use when every array is
     * meaningfully populated. Discards with bounds checking.
     *
     * @param classes   Array of probabilities
     * @return  Sum of annealed probabilities. Is not 1.
     */
    public double annealProbs(double[] classes) {
        return annealProbs(0, classes);
    }

    protected void averageSamples(double[] _v) {
        for (int i = 0; i < _v.length; ++i) {
            _v[i] /= sampleCount;
        }
    }

    protected void addToFirstMoment(double[] _target, double[] _source) {
        try {
            for (int i = 0;; ++i) {
                _target[i] = TGMath.stableSum(_target[i], _source[i]);
            }
        } catch (ArrayIndexOutOfBoundsException e) {
        }
    }

    protected void addToSecondMoment(double[] _target, double[] _source) {
        try {
            for (int i = 0;; ++i) {
                _target[i] = TGMath.stableSum(_target[i], Math.pow(_source[i], 2));
            }
        } catch (ArrayIndexOutOfBoundsException e) {
        }
    }

    protected void addToCovariance(double[] _target, double[] _source) {
        try {
            int c = _source.length;
            for (int i = 0; i < c; ++i) {
                int off = i * (2 * c - i + 1) / 2 - i;
                for (int j = i; j < c; ++j) {
                    _target[off + j] = TGMath.stableSum(_target[i], TGMath.stableProd(_source[i], _source[j]));
                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {
        }
    }

    /**
     * @return the samples
     */
    public int getSamples() {
        return samples;
    }

    public boolean isFinishedCollection() {
        return finishedCollection;
    }

    public void setFinishedCollection(boolean finishedCollection) {
        this.finishedCollection = finishedCollection;
    }

    public double getInitialTemperature() {
        return initialTemperature;
    }

    public void setInitialTemperature(double initialTemperature) {
        this.initialTemperature = initialTemperature;
    }

    public int getInnerIter() {
        return innerIter;
    }

    public void setInnerIter(int innerIter) {
        this.innerIter = innerIter;
    }

    public int getInnerIterationsMax() {
        return innerIterationsMax;
    }

    public void setInnerIterationsMax(int innerIterationsMax) {
        this.innerIterationsMax = innerIterationsMax;
    }

    public int getLag() {
        return lag;
    }

    public void setLag(int lag) {
        this.lag = lag;
    }

    public int getOuterIter() {
        return outerIter;
    }

    public void setOuterIter(int outerIter) {
        this.outerIter = outerIter;
    }

    public int getOuterIterationsMax() {
        return outerIterationsMax;
    }

    public void setOuterIterationsMax(int outerIterationsMax) {
        this.outerIterationsMax = outerIterationsMax;
    }

    public int getSampleCount() {
        return sampleCount;
    }

    public void setSampleCount(int sampleCount) {
        this.sampleCount = sampleCount;
    }

    public boolean isSampleiteration() {
        return sampleiteration;
    }

    public void setSampleiteration(boolean sampleiteration) {
        this.sampleiteration = sampleiteration;
    }

    public double getTargetTemperature() {
        return targetTemperature;
    }

    public void setTargetTemperature(double targetTemperature) {
        this.targetTemperature = targetTemperature;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public double getTemperatureDecrement() {
        return temperatureDecrement;
    }

    public void setTemperatureDecrement(double temperatureDecrement) {
        this.temperatureDecrement = temperatureDecrement;
    }

    public double getTemperatureReciprocal() {
        return temperatureReciprocal;
    }

    public void setTemperatureReciprocal(double temperatureReciprocal) {
        this.temperatureReciprocal = temperatureReciprocal;
    }
}
