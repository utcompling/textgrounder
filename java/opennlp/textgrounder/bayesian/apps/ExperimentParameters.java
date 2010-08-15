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
package opennlp.textgrounder.bayesian.apps;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    protected int degreesPerRegion = 3;
    /**
     *
     */
    protected String projectRoot = ".";
    /**
     *
     */
    protected String corpusFilename = "input.xml";
    /**
     * 
     */
    protected String processedCorpusFilename = "output.xml";
    /**
     * Path to array of tokens, toponym status. Should be a file.
     */
    protected String tokenArrayInputFilename = "token-array-input.dat.gz";
    /**
     * Path to array of tokens, toponym status. Should be a file.
     */
    protected String tokenArrayOutputPath = "token-array-output.dat.gz";
    /**
     * 
     */
    protected String toponymRegionFilename = "toponym-region.dat.gz";
    /**
     * Path of model that has been saved from previous training runs
     */
    protected String trainedModelFilename = "trained-model.model.gz";
    /**
     * 
     */
    protected String sampledProbabilitiesFilename = "probabilities.dat.gz";
    /**
     *
     */
    protected String wordByRegionProbsFilename = "word-by-region-probabilities.gz";
    /**
     * 
     */
    protected String regionByWordProbsFilename = "region-by-word-probabilities.gz";
    /**
     *
     */
    protected String regionByDocumentProbsFilename = "region-by-document-probabilities.gz";
    /**
     * 
     */
    protected String regionFilename = "region.dat.gz";
    /**
     * 
     */
    protected String lexiconFilename = "lexicon.dat.gz";
    /**
     *
     */
    protected INPUT_FORMAT inputFormat = INPUT_FORMAT.BINARY;

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

    public String getTokenArrayOutputPath() {
        return joinPath(projectRoot, tokenArrayOutputPath);
    }

    public String getTokenArrayInputPath() {
        return joinPath(projectRoot, tokenArrayInputFilename);
    }

    public String getSampledProbabilitiesPath() {
        return joinPath(projectRoot, sampledProbabilitiesFilename);
    }

    public String getRegionByWordProbabilitiesPath() {
        return joinPath(projectRoot, regionByWordProbsFilename);
    }

    public String getWordByRegionProbabilitiesPath() {
        return joinPath(projectRoot, wordByRegionProbsFilename);
    }

    public String getRegionByDocumentProbabilitiesPath() {
        return joinPath(projectRoot, regionByDocumentProbsFilename);
    }

    /**
     * Path of input to rlda component
     */
    public String getTrainedModelOutputPath() {
        return joinPath(projectRoot, trainedModelFilename);
    }

    /**
     * 
     * @return
     */
    public String getToponymRegionPath() {
        return joinPath(projectRoot, toponymRegionFilename);
    }

    public int getTrainIterations() {
        return trainIterations;
    }

    public INPUT_FORMAT getInputFormat() {
        return inputFormat;
    }

    /**
     *
     * @param _names
     * @return
     */
    protected String joinPath(String... _names) {
        StringBuilder sb = new StringBuilder();
        String sep = File.separator;
        if (_names[0].startsWith("~")) {
            _names[0].replaceFirst("~", System.getProperty("user.dir"));
        }

        for (String name : _names) {
            for (String comp : name.split(sep)) {
                if (!comp.isEmpty()) {
                    sb.append(sep).append(comp);
                }
            }
        }

        return sb.toString();
    }

    public void setProjectRoot(String root) {
        if (projectRoot.equals(".")) {
            try {
                projectRoot = (new File(root)).getCanonicalPath();
            } catch (IOException ex) {
                Logger.getLogger(ExperimentParameters.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
