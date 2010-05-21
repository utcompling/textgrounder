///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
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
package opennlp.textgrounder.annealers;

import opennlp.textgrounder.geo.CommandLineOptions;
import opennlp.textgrounder.util.Constants;

/**
 * Class for conducting simulated annealing. This class also controls the burn-in
 * and the number of samples taken.
 * 
 * @author tsmoon
 */
public abstract class Annealer {

    /**
     * Exponent in the annealing process. Is the reciprocal of the temperature.
     */
    protected double temperatureReciprocal;
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
    protected int samples;
    /**
     * Number of innerIter between samples
     */
    protected int lag;
    /**
     * The current temperature
     */
    protected double temperature;
    /**
     * Counts of topics
     */
    protected double[] topicCounts;
    /**
     * Counts of tcount per topic. However, since access more often occurs in
     * terms of the tcount, it will be a topic by word matrix.
     */
    protected double[] wordByTopicCounts;
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

    public Annealer(CommandLineOptions options) {
        initialTemperature = options.getInitialTemperature();
        temperature = initialTemperature;
        temperatureReciprocal = 1 / temperature;
        temperatureDecrement = options.getTemperatureDecrement();
        targetTemperature = options.getTargetTemperature();
        innerIterationsMax = options.getIterations();
        outerIterationsMax =
              (int) Math.round((initialTemperature - targetTemperature)
              / temperatureDecrement) + 1;
        samples = options.getSamples();
        lag = options.getLag();
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
        if (Math.abs(temperatureReciprocal - 1) < Constants.EPSILON) {
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
                innerIter = 0;

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

    /**
     * 
     * @param tc
     * @param wbtc
     * @param b
     */
    public void collectSamples(double[] tc, double[] wbtc) {
        if (sampleiteration && (innerIter % lag == 0)) {
            sampleCount += 1;
            if (samples == sampleCount) {
                finishedCollection = true;
            }

            System.err.print("(sample:" + (innerIter + 1) / lag + ")");
            if (topicCounts == null) {
                topicCounts = new double[tc.length];
                wordByTopicCounts = new double[wbtc.length];
                for (int i = 0; i < tc.length; ++i) {
                    topicCounts[i] = 0;
                }
                for (int i = 0; i < wbtc.length; ++i) {
                    wordByTopicCounts[i] = 0;
                }
            }

            for (int i = 0; i < tc.length; ++i) {
                topicCounts[i] += tc[i];
            }
            for (int i = 0; i < wbtc.length; ++i) {
                wordByTopicCounts[i] += wbtc[i];
            }
        }

        if(finishedCollection) {
            normalizeSamples();
        }
    }

    /**
     * 
     */
    protected void normalizeSamples() {
        for (int i = 0; i < topicCounts.length; ++i) {
            topicCounts[i] /= sampleCount;
        }
        for (int i = 0; i < wordByTopicCounts.length; ++i) {
            wordByTopicCounts[i] /= sampleCount;
        }
    }

    /**
     * @return the samples
     */
    public int getSamples() {
        return samples;
    }

    /**
     * @return the topicCounts
     */
    public double[] getTopicCounts() {
        return topicCounts;
    }

    /**
     * @return the wordByTopicCounts
     */
    public double[] getWordByTopicCounts() {
        return wordByTopicCounts;
    }
}
