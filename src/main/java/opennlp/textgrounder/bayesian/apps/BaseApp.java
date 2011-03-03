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
import org.apache.commons.cli.Options;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class BaseApp {

    /**
     * 
     */
    protected static String helpMessage = "To run:\n"
          + "\tcommand PATH_TO_INPUT [PARAMETER_FILE]\n\n"
          + "PATH_TO_INPUT is a required argument. It specifies a directory to \n"
          + "the files required for running rlda experiments. Two files must \n"
          + "exist in this directory. One is a parameters file for experiments \n"
          + "and the other is the input file, which by default should be named \n"
          + "\"input.xml\". It is possible to change the name of this input \n"
          + "file from within the parameters file.\n\n"
          + "PARAMETER_FILE is an optional parameter. If you do not supply this, \n"
          + "it will default to \"exp-parameters.xml\" which must exist in \n"
          + "PATH_TO_INPUT. Even if you do designate your own filename for the \n"
          + "parameters, it must still reside in the input path.";

    /**
     * Takes an Options instance and adds/sets command line options.
     *
     * @param options Command line options
     */
    protected static void setOptions(Options options) {
        options.addOption("a", "alpha", true, "alpha value (default=50/topics)");
        options.addOption("b", "beta", true, "beta value (default=0.1)");
        options.addOption("d", "degrees-per-region", true, "size of Region squares in degrees [default = 3.0]");
        options.addOption("e", "training-iterations", true, "number of training iterations (default=100)");
        options.addOption("i", "train-input", true, "input file or directory name for training data");
        options.addOption("ks", "samples", true, "number of samples to take (default=100)");
        options.addOption("kl", "lag", true, "number of iterations between samples (default=100)");
        options.addOption("m", "model", true, "model (PopBaseline, RandomBaseline, BasicMinDistance, WeightedMinDistance) [default = PopBaseline]");
        options.addOption("o", "output", true, "output filename [default = 'output.kml']");
        options.addOption("ot", "output-tabulated-probabilities", true, "path of tabulated probability output");
        options.addOption("p", "paragraphs-as-docs", true, "number of paragraphs to treat as a document. Set the argument to 0 if documents should be treated whole");
        options.addOption("pi", "initial-temperature", true, "initial temperature for annealing regime (default=0.1)");
        options.addOption("pd", "temperature-decrement", true, "temperature decrement steps (default=0.1)");
        options.addOption("pt", "target-temperature", true, "temperature at which to stop annealing (default=1)");
        options.addOption("r", "random-seed", true, "seed for random number generator. set argument to 0 to seed with the current time (default=1)");
        options.addOption("w", "output-per-class", true, "number of words to output to path specified in output-tabulated-probabilities (default=10)");

        options.addOption("h", "help", false, "print help");
    }

    protected static void processRawCommandline(String[] _args,
          ExperimentParameters _experimentParameters) {
        String root = null;
        try {
            root = (new File(_args[0])).getCanonicalPath() + File.separator;
        } catch (IOException ex) {
            Logger.getLogger(BaseApp.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch(ArrayIndexOutOfBoundsException ex) {
            System.err.println(helpMessage);
            System.exit(1);
        }

        String paramFilename = null;
        try {
            paramFilename = _args[1];
        } catch (ArrayIndexOutOfBoundsException e) {
            paramFilename = "exp-parameters.xml";
        }

        ExperimentParameterManipulator.loadParameters(_experimentParameters, root + paramFilename, "ALL");
        _experimentParameters.setProjectRoot(root);
    }
}
