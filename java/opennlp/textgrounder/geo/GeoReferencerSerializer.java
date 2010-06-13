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
import opennlp.textgrounder.topostructs.*;

/**
 * App to be called from command line. Runs LDA based georeferencing models
 *
 * @author tsmoon
 */
public class GeoReferencerSerializer extends BaseApp {

    public static void main(String[] args) {
        try {
            CommandLineParser optparse = new PosixParser();
            Options options = new Options();
            setOptions(options);
            CommandLine cline = optparse.parse(options, args);
            if (cline.hasOption('h')) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("java GeoReferencerSerializer", options);
                System.exit(0);
            }
            CommandLineOptions modelOptions = new CommandLineOptions(cline);
            RegionModelSerializer<SmallLocation> rm = new RegionModelSerializer<SmallLocation>(modelOptions, new SmallLocation());
            rm.serialize(modelOptions.getSerializedDataParametersFilename());
        } catch (IOException ex) {
            Logger.getLogger(GeoReferencerSerializer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParseException ex) {
            Logger.getLogger(GeoReferencerSerializer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
