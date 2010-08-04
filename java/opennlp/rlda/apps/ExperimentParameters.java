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
package opennlp.rlda.apps;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class ExperimentParameters {

    public static enum INPUT_FORMAT {

        BINARY,
        TEXT,
        XML
    }
    /**
     * Hyperparameter of topic prior
     */
    protected double alpha = 1;
    /**
     * Hyperparameter for word/topic prior
     */
    protected double beta = 0.1;
    /**
     * Number of training iterations
     */
    protected int trainIterations = 100;
    /**
     * Number to seed random number generator. If 0 is passed from the commandline,
     * it means that a true random seed will be used (i.e. one based on the current time).
     * Otherwise, any value that has been passed to it from the command line be
     * used. If nothing is passed from the commandline, the random number
     * generator will be seeded with 1.
     */
    protected int randomSeed = 1;
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
     * Number of regions
     */
    protected int regions = 50;
    /**
     * Number of samples to take
     */
    protected int samples = 100;
    /**
     * Number of trainIterations between samples
     */
    protected int lag = 10;
    /**
     * 
     */
    protected int countCutoff = 5;
    /**
     *
     */
    protected String corpusPath = null;
    /**
     *
     */
    protected int degreesPerRegion = 3;
    /**
     *
     */
    protected String inputPath = null;
    /**
     * 
     */
    protected String outputPath = null;
    /**
     *
     */
    protected String corpusFileName = "input.xml";
    /**
     * Path to array of tokens, toponym status. Should be a file.
     */
    protected String tokenArrayFileName = "token-array.dat";
    /**
     * Path to array of tokens, toponym status. Should be a file.
     */
    protected String tokenArrayOutputPath = null;
    /**
     * 
     */
    protected String toponymRegionFileName = "toponym-region.dat";
    /**
     * Path of model that has been saved from previous training runs
     */
    protected String trainedModelPath = "trained-model.model";
    /**
     *
     */
    protected INPUT_FORMAT inputFormat = INPUT_FORMAT.BINARY;

    /**
     * Default constructor
     */
    public ExperimentParameters() {
    }

    public double getAlpha() {
        return alpha;
    }

    public double getBeta() {
        return beta;
    }

    public int getIterations() {
        return trainIterations;
    }

    public int getRandomSeed() {
        return randomSeed;
    }

    public double getInitialTemperature() {
        return initialTemperature;
    }

    public double getTargetTemperature() {
        return targetTemperature;
    }

    public double getTemperatureDecrement() {
        return temperatureDecrement;
    }

    public int getRegions() {
        return regions;
    }

    public int getSamples() {
        return samples;
    }

    public int getLag() {
        return lag;
    }

    public String getCorpusFileName() {
        return corpusFileName;
    }

    public String getCorpusPath() {
        return corpusPath;
    }

    public String getTokenArrayOutputPath() {
        return tokenArrayOutputPath;
    }

    public String getTokenArrayPath() {
        return tokenArrayFileName;
    }

    public String getTrainedModelPath() {
        return trainedModelPath;
    }

    public String getToponymRegionPath() {
        return toponymRegionFileName;
    }

    public int getTrainIterations() {
        return trainIterations;
    }

    public INPUT_FORMAT getInputFormat() {
        return inputFormat;
    }
}
