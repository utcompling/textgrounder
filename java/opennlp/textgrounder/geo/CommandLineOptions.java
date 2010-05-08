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
package opennlp.textgrounder.geo;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.commons.cli.*;

/**
 * Handles options from the command line. Also sets the default parameter
 * values.
 *
 * @author tsmoon
 */
public class CommandLineOptions {

    /**
     * Number of types (either word or morpheme) to print per state or topic
     */
    protected int outputPerClass = 10;
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
    protected int iterations = 100;
    /**
     * Number to seed random number generator. If 0 is passed from the commandline,
     * it means that a true random seed will be used (i.e. one based on the current time).
     * Otherwise, any value that has been passed to it from the command line be
     * used. If nothing is passed from the commandline, the random number
     * generator will be seeded with 1.
     */
    protected int randomSeed = 1;
    /**
     * Name of file to generate tabulated kmlOutputFilename to
     */
    protected String tabularOutputFilename = null;
    /**
     * Output buffer to write normalized, tabulated data to.
     */
    protected BufferedWriter tabulatedOutput = null;
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
     * Number of topics
     */
    protected int topics = 50;
    /**
     * Number of samples to take
     */
    protected int samples = 100;
    /**
     * Number of iterations between samples
     */
    protected int lag = 10;
    /**
     * Type of gazette. The gazette shorthands from the commandline are:
     * <pre>
     * "c": CensusGazetteer
     * "n": NGAGazetteer
     * "u": USGSGazetteer
     * "w": WGGazetteer (default)
     * "t": TRGazetteer
     * </pre>
     */
    protected String gazetteType = "w";
    /**
     * Path to training data. Can be directory or a single file.
     */
    protected String trainInputPath = null;
    /**
     * Path of model that has been saved from previous training runs
     */
    protected String trainedModelPath = null;
    /**
     * Path to test data. Can be directory or a single file.
     */
    protected String testInputPath = null;
    /**
     * Path to kmlOutputFilename kml file.
     */
    protected String kmlOutputFilename = "output.kml";
    /**
     * Number of paragraphs to treat as a single document.
     */
    protected int paragraphsAsDocs = 10;
    /**
     * Dimensions of region in degrees
     */
    protected double degreesPerRegion = 3.0;
    /**
     * Context window size on either side
     */
    protected int windowSize = 20;
    /**
     * Size of bars in kml kmlOutputFilename
     */
    protected int barScale = 50000;
    /**
     * Model type
     */
    protected String model = "population";
    /**
     * A flag that runs the system on every locality in the gazetteer, ignoring the input text file(s), if set to true
     */
    protected boolean runWholeGazetteer = false;
    /**
     * Evaluation directory; null if no evaluation is to be done.
     */
    protected String evalDir = null;

    /**
     *
     * @param cline
     * @throws IOException
     */
    public CommandLineOptions(CommandLine cline) throws IOException {

        String opt = null;

        for (Option option : cline.getOptions()) {
            String value = option.getValue();
            switch (option.getOpt().charAt(0)) {
                case 'a':
                    alpha = Double.parseDouble(value);
                    break;
                case 'b':
                    beta = Double.parseDouble(value);
                    break;
                case 'c':
                    barScale = Integer.parseInt(value);
                    break;
                case 'd':
                    degreesPerRegion = Double.parseDouble(value);
                    break;
                case 'e':
                    opt = option.getOpt();
                    if (opt.equals("e")) {
                        iterations = Integer.parseInt(value);
                    } else if (opt.equals("ev")) {
                        evalDir = value;
                    }
                    break;
                case 'g':
                    gazetteType = value;
                    break;
                case 'i':
                    opt = option.getOpt();
                    if (opt.equals("i")) {
                        trainInputPath = value;
                    } else if (opt.equals("ie")) {
                        testInputPath = value;
                    }
                    break;
                case 'k':
                    opt = option.getOpt();
                    if (opt.equals("ks")) {
                        samples = Integer.parseInt(value);
                    } else if (opt.equals("kl")) {
                        lag = Integer.parseInt(value);
                    }
                    break;
                case 'l':
                    trainedModelPath = value;
                    break;
                case 'm':
                    model = value;
                    break;
                case 'o':
                    opt = option.getOpt();
                    if (opt.equals("ot")) {
                        tabularOutputFilename = value;
                        tabulatedOutput = new BufferedWriter(new OutputStreamWriter(
                              new FileOutputStream(tabularOutputFilename)));
                    } else if (opt.equals("o")) {
                        kmlOutputFilename = value;
                    }
                    break;
                case 'p':
                    opt = option.getOpt();
                    if (opt.equals("p")) {
                        paragraphsAsDocs = Integer.parseInt(value);
                    } else if (opt.equals("pi")) {
                        initialTemperature = Double.parseDouble(value);
                    } else if (opt.equals("pd")) {
                        temperatureDecrement = Double.parseDouble(value);
                    } else if (opt.equals("pt")) {
                        targetTemperature = Double.parseDouble(value);
                    }
                    break;
                case 'r':
                    randomSeed = Integer.valueOf(value);
                    break;
                case 't':
                    topics = Integer.parseInt(value);
                    break;
                case 'w':
                    if (option.getOpt().equals("wg")) {
                        runWholeGazetteer = true;
                    } else if (option.getOpt().equals("ws")) {
                        windowSize = Integer.parseInt(value);
                    } else {
                        outputPerClass = Integer.parseInt(value);
                    }
                    break;
            }
        }
    }

    public double getAlpha() {
        return alpha;
    }

    public double getBeta() {
        return beta;
    }

    public int getIterations() {
        return iterations;
    }

    public String getTabularOutputFilename() {
        return tabularOutputFilename;
    }

    public BufferedWriter getTabulatedOutput() {
        return tabulatedOutput;
    }

    public int getOutputPerClass() {
        return outputPerClass;
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

    public String getGazetteType() {
        return gazetteType;
    }

    public String getTrainInputPath() {
        return trainInputPath;
    }

    public String getKMLOutputFilename() {
        return kmlOutputFilename;
    }

    public int getParagraphsAsDocs() {
        return paragraphsAsDocs;
    }

    public double getDegreesPerRegion() {
        return degreesPerRegion;
    }

    public int getBarScale() {
        return barScale;
    }

    public String getModel() {
        return model;
    }

    public String getTestInputPath() {
        return testInputPath;
    }

    public boolean getRunWholeGazetteer() {
        return runWholeGazetteer;
    }

    public String getEvalDir() {
        return evalDir;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public String getTrainedModelPath() {
        return trainedModelPath;
    }
}
