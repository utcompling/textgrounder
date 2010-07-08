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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class ExperimentParameters {

    protected static SAXBuilder builder = new SAXBuilder();
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
     * Path to training data. Can be directory or a single file.
     */
    protected String trainInputPath = null;
    /**
     * Path of model that has been saved from previous training runs
     */
    protected String trainedModelPath = null;

    /**
     *
     * @param cline
     * @throws IOException
     */
    public ExperimentParameters(String _paramFilepath) throws IOException {

        File file = new File(_paramFilepath);

        Document doc = null;
        try {
            doc = builder.build(file);
            Element root = doc.getRootElement();
            ArrayList<Element> params = new ArrayList<Element>(root.getChild("root").getChildren("params"));
            for (Element param : params) {
                try {
                    String fieldName = param.getName();
                    String fieldValue = param.getValue();
                    try {
                        int value = Integer.parseInt(fieldValue);
                        this.getClass().getField(fieldName).set(fieldName, value);
                    } catch (NumberFormatException ei) {
                        try {
                            double value = Double.parseDouble(fieldValue);
                            this.getClass().getField(fieldName).set(fieldName, value);
                        } catch (NumberFormatException ed) {
                            this.getClass().getField(fieldName).set(fieldName, fieldValue);
                        }
                    }
                } catch (NoSuchFieldException ex) {
                    Logger.getLogger(ExperimentParameters.class.getName()).log(Level.SEVERE, null, ex);
                    System.exit(1);
                } catch (SecurityException ex) {
                    Logger.getLogger(ExperimentParameters.class.getName()).log(Level.SEVERE, null, ex);
                    System.exit(1);
                } catch (IllegalArgumentException ex) {
                    Logger.getLogger(ExperimentParameters.class.getName()).log(Level.SEVERE, null, ex);
                    System.exit(1);
                } catch (IllegalAccessException ex) {
                    Logger.getLogger(ExperimentParameters.class.getName()).log(Level.SEVERE, null, ex);
                    System.exit(1);
                }
            }

        } catch (JDOMException ex) {
            Logger.getLogger(ExperimentParameters.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
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

    public String getTrainInputPath() {
        return trainInputPath;
    }

    public String getTrainedModelPath() {
        return trainedModelPath;
    }

    protected String canonicalPath(String _path) {
        try {
            String home = System.getProperty("user.home");
            String path = "";
            path = _path.replaceAll("~", home);
            path = (new File(path)).getCanonicalPath();
            return path;
        } catch (IOException ex) {
            Logger.getLogger(CommandLineOptions.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
        return null;
    }
}
