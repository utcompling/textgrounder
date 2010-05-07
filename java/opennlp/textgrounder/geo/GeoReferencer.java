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

import org.apache.commons.cli.*;

import opennlp.textgrounder.models.*;

public class GeoReferencer extends BaseApp {

    public static void main(String[] args) throws Exception {

        CommandLineParser optparse = new PosixParser();

        Options options = new Options();
        setOptions(options);

        CommandLine cline = optparse.parse(options, args);

        if (cline.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java GeoReferencer", options);
            System.exit(0);
        }

        CommandLineOptions modelOptions = new CommandLineOptions(cline);

	if(modelOptions.getEvalDir() != null) {
	    System.out.println("Performing EVALUATION on " + modelOptions.getEvalDir());
	}

	Model grefUS = null;
	if(modelOptions.model.toLowerCase().startsWith("p")) {
	    System.out.println("Using POPULATION baseline model.");
	    grefUS = new BaselineModel(modelOptions);
	}
	else if(modelOptions.model.toLowerCase().startsWith("r")) {
	    System.out.println("Using RANDOM baseline model.");
	    grefUS = new RandomBaselineModel(modelOptions);
	}
	else {
	    System.err.println("Invalid model type: " + modelOptions.model);
	    System.exit(0);
	}

        //System.out.println(grefUS.getInputFile().getCanonicalPath());
        grefUS.train();
        grefUS.printRegionArray();

        System.out.println("Writing KML file " + grefUS.getOutputFilename() + " ...");
        grefUS.writeXMLFile();

	if(modelOptions.getEvalDir() != null) {
	    grefUS.evaluate();
	}
    }
}
