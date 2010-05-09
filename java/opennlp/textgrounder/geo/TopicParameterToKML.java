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

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.*;

import opennlp.textgrounder.models.*;

/**
 * Reads in parameters trained from Region Topic model and outputs a kml file
 *
 * @author tsmoon
 */
public class TopicParameterToKML {

    public static void main(String[] args) {

        CommandLineParser optparse = new PosixParser();

        Options options = new Options();

        options.addOption("l", "load-model", true, "path to model to load");
        options.addOption("o", "output", true, "name of kml file to save output to");
        options.addOption("w", "output-per-class", true, "word to look up");
        options.addOption("h", "help", false, "print help");

        CommandLine cline = null;
        try {
            cline = optparse.parse(options, args);
        } catch (ParseException ex) {
            Logger.getLogger(TopicParameterToKML.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(0);
        }

        if (cline.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java GeoReferencerLDA", options);
            System.exit(0);
        }

        String word = null, modelPath = null, outputPath = null;

        for (Option option : cline.getOptions()) {
            String value = option.getValue();
            switch (option.getOpt().charAt(0)) {
                case 'l':
                    modelPath = value;
                    break;
                case 'o':
                    outputPath = value;
                    break;
                case 'w':
                    word = value;
                    break;
            }
        }

        RegionModel rm = new RegionModel();
        try {
            rm.loadSimpleParameters(modelPath);
            if (outputPath == null) {
                rm.writeWordOverGlobeKML(word);
            } else {
                rm.writeWordOverGlobeKML(outputPath, word);
            }
        } catch (IOException ex) {
            Logger.getLogger(TopicParameterToKML.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(0);
        }
    }
}
