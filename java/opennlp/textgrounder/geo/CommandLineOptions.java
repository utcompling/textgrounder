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
     * Size of bars in kml kmlOutputFilename
     */
    protected int barScale = 50000;
    /**
     * Model type
     */
    protected String model = null;

    /**
     * A flag that runs the system on every locality in the gazetteer, ignoring the input text file(s), if set to true
     */
    protected boolean runWholeGazetteer = false;

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
                    iterations = Integer.parseInt(value);
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
		    if(option.getOpt().equals("wg")) {
			runWholeGazetteer = true;
		    }
                    else {
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
}
