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

import org.apache.commons.cli.Options;

/**
 * Base class for setting command line options for all georeferencing classes that have a
 * "main" method.
 *
 * @author tsmoon
 */
public class BaseApp {

    /**
     * Takes an Options instance and adds/sets command line options.
     *
     * @param options Command line options
     */
    protected static void setOptions(Options options) {
        options.addOption("a", "alpha", true, "alpha value (default=50/topics)");
        options.addOption("b", "beta", true, "beta value (default=0.1)");
        options.addOption("c", "bar-scale", true, "height of bars in kml output (default=50000)");
        options.addOption("d", "degrees-per-region", true, "size of Region squares in degrees [default = 3.0]");
        options.addOption("e", "training-iterations", true, "number of training iterations (default=100)");
	options.addOption("ev", "evaluate", true, "specifies evaluation directory and enables evaluation mode");
        options.addOption("g", "gazetteer", true, "gazetteer to use [world, census, NGA, USGS; default = world]");
        options.addOption("i", "train-input", true, "input file or directory name for training data");
        options.addOption("ie", "test-input", true, "input file or directory name for test data");
        options.addOption("ks", "samples", true, "number of samples to take (default=100)");
        options.addOption("kl", "lag", true, "number of iterations between samples (default=100)");
        options.addOption("m", "model", true, "model (PopBaseline or RandomBaseline) [default = PopBaseline]");
        options.addOption("o", "output", true, "output filename [default = 'output.kml']");
        options.addOption("ot", "output-tabulated-probabilities", true, "path of tabulated probability output");
        options.addOption("p", "paragraphs-as-docs", true, "number of paragraphs to treat as a document. Set the argument to 0 if documents should be treated whole");
        options.addOption("pi", "initial-temperature", true, "initial temperature for annealing regime (default=0.1)");
        options.addOption("pd", "temperature-decrement", true, "temperature decrement steps (default=0.1)");
        options.addOption("pt", "target-temperature", true, "temperature at which to stop annealing (default=1)");
        options.addOption("r", "random-seed", true, "seed for random number generator. set argument to 0 to seed with the current time (default=1)");
        options.addOption("t", "topics", true, "number of topics for baseline topic model (default=50)");
        options.addOption("w", "output-per-class", true, "number of words to output to path specified in output-tabulated-probabilities (default=10)");
	options.addOption("wg", "whole-gazetteer", false, "activate regions and run program for entire gazetteer (the -i flag will be ignored in this case)");
	options.addOption("ws", "window-size", true, "size of context window (in either direction) (default=20)");

        options.addOption("h", "help", false, "print help");
    }
}
