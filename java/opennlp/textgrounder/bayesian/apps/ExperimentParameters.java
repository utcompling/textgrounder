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

    public static enum MODEL_TYPE {

        RLDA,
        V1,
        V1_INDEPENDENT_REGIONS,
        V2,
        V2_DEPENDENT_REGIONS
    }
    /**
     * Switch for whether hyperparameters should be reestimated or not.
     * 0 is no, 1 is yes.
     */
    protected int estimateHyperparameter = 0;
    /**
     * alpha for the chinese restaurant process
     */
    protected double crpalpha = 20;
    /**
     * gamma prior rate parameter a for crpalpha
     */
    protected double crpalpha_a = 20;
    /**
     * gamma prior shape parameter b for crpalpha
     */
    protected double crpalpha_b = 20;
    /**
     * Hyperparameter of topic prior
     */
    protected double alpha = 1;
    /**
     * gamma prior rate parameter a for alpha
     */
    protected double alpha_a = 1;
    /**
     * gamma prior shape parameter b for alpha
     */
    protected double alpha_b = 0.1;
    /**
     * Hyperparameter for word/topic prior
     */
    protected double beta = 0.1;
    /**
     * gamma prior rate parameter a for beta
     */
    protected double beta_a = 0.1;
    /**
     * gamma prior shape parameter b for beta
     */
    protected double beta_b = 0.1;
    /**
     * Switch for whether kappa should be reestimated or not.
     * 0 is no, 1 is yes.
     */
    protected int estimateKappa = 0;
    /**
     * kappa (spread) for spherical model. Higher means more peaked.
     */
    protected double kappa = 20;
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
     * Number of non-region topics
     */
    protected int topics = 50;
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
    protected int outputPerClass = 20;
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
    protected String tokenArrayOutputFilename = "token-array-output.dat.gz";
    /**
     * 
     */
    protected String toponymRegionFilename = "toponym-region.dat.gz";
    /**
     *
     */
    protected String toponymCoordinateFilename = "toponym-coordinate.dat.gz";
    /**
     * Path of model that has been saved from previous training runs
     */
    protected String trainedModelFilename = "trained-model.model.gz";
    /**
     * 
     */
    protected String averagedCountsFilename = "averaged-counts.dat.gz";
    /**
     *
     */
    protected String xmlConditionalProbabilitiesFilename = "sorted-conditional-probabilities.xml";
    /**
     *
     */
    protected String wordByRegionProbsFilename = "word-by-region-probabilities.gz";
    /**
     *
     */
    protected String wordByTopicProbsFilename = "word-by-topic-probabilities.gz";
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
    protected Enum<INPUT_FORMAT> inputFormat = INPUT_FORMAT.BINARY;
    /**
     *
     */
    protected Enum<MODEL_TYPE> modelType = MODEL_TYPE.RLDA;

    public double getCrpalpha() {
        return crpalpha;
    }

    /**
     * @return the estimateHyperparameter
     */
    public int getEstimateHyperparameter() {
        return estimateHyperparameter;
    }

    /**
     * @return the crpalpha_a
     */
    public double getCrpalpha_a() {
        return crpalpha_a;
    }

    /**
     * @return the crpalpha_b
     */
    public double getCrpalpha_b() {
        return crpalpha_b;
    }

    /**
     * @return the alpha_a
     */
    public double getAlpha_a() {
        return alpha_a;
    }

    /**
     * @return the alpha_b
     */
    public double getAlpha_b() {
        return alpha_b;
    }

    /**
     * @return the beta_a
     */
    public double getBeta_a() {
        return beta_a;
    }

    /**
     * @return the beta_b
     */
    public double getBeta_b() {
        return beta_b;
    }

    /**
     * @return the estimateKappa
     */
    public int getEstimateKappa() {
        return estimateKappa;
    }

    public double getAlpha() {
        return alpha;
    }

    public double getBeta() {
        return beta;
    }

    public double getKappa() {
        return kappa;
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

    public int getTopics() {
        return topics;
    }

    public int getSamples() {
        return samples;
    }

    public int getLag() {
        return lag;
    }

    public String getTokenArrayOutputPath() {
        return joinPath(projectRoot, tokenArrayOutputFilename);
    }

    public String getTokenArrayInputPath() {
        return joinPath(projectRoot, tokenArrayInputFilename);
    }

    public String getAveragedCountsPath() {
        return joinPath(projectRoot, averagedCountsFilename);
    }

    public int getOutputPerClass() {
        return outputPerClass;
    }

    public String getXmlConditionalProbabilitiesPath() {
        return joinPath(projectRoot, xmlConditionalProbabilitiesFilename);
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

    public String getToponymCoordinatePath() {
        return joinPath(projectRoot, toponymCoordinateFilename);
    }

    public int getTrainIterations() {
        return trainIterations;
    }

    public INPUT_FORMAT getInputFormat() {
        return (INPUT_FORMAT) inputFormat;
    }

    public MODEL_TYPE getModelType() {
        return (MODEL_TYPE) modelType;
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
