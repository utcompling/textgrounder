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

import org.apache.commons.cli.*;

import opennlp.textgrounder.models.*;

/**
 * App to be called from command line. Runs LDA based georeferencing models
 *
 * @author tsmoon
 */
public class GeoReferencerLDA extends BaseApp {

    public static void main(String[] args) throws Exception {

        CommandLineParser optparse = new PosixParser();

        Options options = new Options();
        setOptions(options);

        CommandLine cline = optparse.parse(options, args);

        if (cline.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java GeoReferencerLDA", options);
            System.exit(0);
        }

        CommandLineOptions modelOptions = new CommandLineOptions(cline);
        NgramRegionModel rm = new NgramRegionModel(modelOptions);
        rm.train();
        if (modelOptions.getTabularOutputFilename() != null) {
            rm.normalize();
            rm.printTabulatedProbabilities();
        }

        if (modelOptions.getKMLOutputFilename() != null) {
            rm.writeXMLFile();
        }
    }
}
