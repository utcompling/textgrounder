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
        RLDAC,
        SV1,
        SPHERICAL_V1_INDEPENDENT_REGIONS,
        SV2,
        SPHERICAL_V2_DEPENDENT_REGIONS,
        SV3,
        SPHERICAL_V3_DEPENDENT_REGIONS
    }
    ////////////////////////////////////////////////////////////////////////
    /**
     * spherical model parameters
     */
    ////////////////////////////////////////////////////////////////////////
    /**
     * Print various error messages during execution
     */
    protected boolean verbose = false;
    /**
     * Track the sampler for analytic purposes and to test convergence
     */
    protected boolean trackSamples = true;
    /**
     * Switch for whether hyperparameters should be reestimated or not.
     * 0 is no, 1 is yes.
     */
    protected int estimateHyperparameter = 1;
    /**
     * global stick breaking prior. is specified from param file on first
     * iteration.
     */
    protected double alpha_H = 10;
    /**
     * fixed intermediate parameters for resampling alpha_H. corresponds to
     * d and f in paper
     */
    protected double alpha_h_d = 1, alpha_h_f = 1;
    /**
     * uniform hyperparameter for restaurant local stick breaking weights. only
     * used in first iteration. set from param file
     */
    protected double alpha_init = 10;
    /**
     * hyperparameters for sampling document specific alpha parameters in posterior
     * update stage. corresponds to a and b respectively in algorithm paper
     */
    protected double alpha_shape_a = 0.1;
    protected double alpha_scale_b = 0.1;
    /**
     * gamma hyperparameters for kappa generation in metropolis-hastings
     */
    protected double kappa_hyper_shape = 50, kappa_hyper_scale = 1;
    /**
     * dirichlet hyperparameter for non-toponym word by dish weights. corresponding
     * random variable is labeled phi in the algorithm paper. corresponding hyperparameter
     * is c_0 in paper.
     */
    protected double phi_dirichlet_hyper = 0.1;
    /**
     * dirichlet hyperparameter for coordinate candidates of toponyms. corresponding
     * random variable is ehta in the algorithm paper. corresponding hyperparameter
     * is d_0 in paper.
     */
    protected double ehta_dirichlet_hyper = 0.1;
    /**
     * the concentration parameter in the proposal distribution for region means
     */
    protected double vmf_proposal_kappa = 50;
    /**
     * the standard deviation in the proposal distribution for region kappas
     */
    protected double vmf_proposal_sigma = 1;
    /**
     * number of dishes when starting out
     */
    protected int L = 1000;
    ////////////////////////////////////////////////////////////////////////
    /**
     * region model parameters
     */
    ////////////////////////////////////////////////////////////////////////
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
    protected int burnInIterations = 100;
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
    protected Enum<MODEL_TYPE> modelType = MODEL_TYPE.SV1;

    public boolean isVerbose() {
        return verbose;
    }

    /**
     * @return the estimateHyperparameter
     */
    public int getEstimateHyperparameter() {
        return estimateHyperparameter;
    }

    public double get_alpha_H() {
        return alpha_H;
    }

    public double get_alpha_h_d() {
        return alpha_h_d;
    }

    public double get_alpha_h_f() {
        return alpha_h_f;
    }

    public double get_alpha_init() {
        return alpha_init;
    }

    public double get_alpha_scale_b() {
        return alpha_scale_b;
    }

    public double get_alpha_shape_a() {
        return alpha_shape_a;
    }

    public double get_ehta_dirichlet_hyper() {
        return ehta_dirichlet_hyper;
    }

    public double get_kappa_hyper_scale() {
        return kappa_hyper_scale;
    }

    public double get_kappa_hyper_shape() {
        return kappa_hyper_shape;
    }

    public double get_phi_dirichlet_hyper() {
        return phi_dirichlet_hyper;
    }

    public double get_vmf_proposal_kappa() {
        return vmf_proposal_kappa;
    }

    public double get_vmf_proposal_sigma() {
        return vmf_proposal_sigma;
    }

    public int getL() {
        return L;
    }

    public double getAlpha() {
        return alpha;
    }

    public double getBeta() {
        return beta;
    }

    public int getBurnInIterations() {
        return burnInIterations;
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

    /**
     * Sets the verbosity level at the root level.
     */
    public void setVerbosity() {
        if (!verbose) {
//            System.setOut(new java.io.PrintStream(new java.io.OutputStream() {
//
//                @Override
//                public void write(int b) {
//                }
//            }));
            System.setErr(new java.io.PrintStream(new java.io.OutputStream() {

                @Override
                public void write(int b) {
                }
            }));
        }
    }
}
