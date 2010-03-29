///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
//
//  This library is free software; you can redistribute it and/or
//  modify it under the terms of the GNU Lesser General Public
//  License as published by the Free Software Foundation; either
//  version 3 of the License, or (at your option) any later version.
//
//  This library is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public
//  License along with this program; if not, write to the Free Software
//  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
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
            return false;
        } else {
            if (innerIter == innerIterationsMax) {
                innerIter = 0;
                outerIter++;
                temperature -= temperatureDecrement;
                temperatureReciprocal = 1 / temperature;
                stabilizeTemperature();
            } else {
                innerIter++;
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
}
