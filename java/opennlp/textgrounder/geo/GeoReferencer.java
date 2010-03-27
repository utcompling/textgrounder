package opennlp.textgrounder.geo;

import org.apache.commons.cli.*;

import opennlp.textgrounder.models.BaselineModel;

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
        BaselineModel grefUS = new BaselineModel(modelOptions);

        System.out.println(grefUS.getInputFile().getCanonicalPath());
        grefUS.train();
        grefUS.printRegionArray();

        System.out.println("Writing KML file " + grefUS.getOutputFilename() + " ...");
        grefUS.writeXMLFile();
    }
}
