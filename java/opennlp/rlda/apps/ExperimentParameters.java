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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.IllegalFormatConversionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

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
     * Path to array of tokens, toponym status. Should be a file.
     */
    protected String tokenArrayInputPath = null;
    /**
     * Path to array of tokens, toponym status. Should be a file.
     */
    protected String tokenArrayOutputPath = null;
    /**
     * Path of model that has been saved from previous training runs
     */
    protected String trainedModelPath = null;

    public ExperimentParameters() {
    }

    public void dumpFieldsToXML(String _outputPath) {
        Element root = new Element("root");
        Document doc = new Document(root);

        Element stage = new Element("ExperimentStage");
        root.addContent(stage);

        try {
            for (Field field : this.getClass().getDeclaredFields()) {
                String fieldName = field.getName();
                String fieldValue = "";
                Object o = field.getType();
                String fieldTypeName = field.getType().getName();
                if (fieldTypeName.equals("int")) {
                    fieldValue = String.format("%d", field.getInt(this));
                } else if (fieldTypeName.equals("double")) {
                    fieldValue = String.format("%f", field.getDouble(this));
                } else if (fieldTypeName.equals("java.lang.String")) {
                    String s = (String) field.get(this);
                    fieldValue = s;
                    fieldTypeName = "string";
                } else {
                    continue;
                }

                Element element = new Element(fieldName);
                element.setText(fieldValue);
                element.setAttribute("type", fieldTypeName);
                stage.addContent(element);
            }
        } catch (IllegalArgumentException ex) {
            Logger.getLogger(ExperimentParameters.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(ExperimentParameters.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }

        serialize(doc, new File(_outputPath));
    }

    static void serialize(Document doc,
          File _file) {
        try {
            XMLOutputter xout = new XMLOutputter(Format.getPrettyFormat());
            xout.output(doc, new FileOutputStream(_file));
        } catch (IOException ex) {
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

    public String tokenArrayInputPath() {
        return tokenArrayInputPath;
    }

    public String getTrainedModelPath() {
        return trainedModelPath;
    }
}
