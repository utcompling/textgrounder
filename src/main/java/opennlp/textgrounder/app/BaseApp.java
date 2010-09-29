/*
 * Base app for running models and/or other functionality such as evaluation and visualization generation.
 */

package opennlp.textgrounder.app;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.*;

public class BaseApp {

    private static Options options = new Options();

    private static String inputPath = "";
    private static String outputPath = "output.xml";

    public static enum MODEL_TYPE {
        RANDOM,
        BASIC_MIN_DIST
    }
    private static Enum<MODEL_TYPE> modelType = MODEL_TYPE.BASIC_MIN_DIST;

    protected static void initializeOptionsFromCommandLine(String[] args) throws Exception {

        options.addOption("i", "input", true, "input path");
        options.addOption("m", "model", true, "model (RandomModel, BasicMinDistModel) [default = BasicMinDistModel]");
        options.addOption("o", "output", true, "output path [default = 'output.xml']");

        options.addOption("h", "help", false, "print help");
        
        CommandLineParser optparse = new PosixParser();
        CommandLine cline = optparse.parse(options, args);

        if (cline.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java <app-class-name>", options);
            System.exit(0);
        }

        String opt = null;

        for (Option option : cline.getOptions()) {
            String value = option.getValue();
            switch (option.getOpt().charAt(0)) {
                case 'i':
                    inputPath = value;
                    break;
                case 'o':
                    outputPath = value;
                    break;
                case 'm':
                    if(value.toLowerCase().startsWith("r"))
                        modelType = MODEL_TYPE.RANDOM;
                    else
                        modelType = MODEL_TYPE.BASIC_MIN_DIST;
                    break;
            }
        }
    }

    public static String getInputPath() {
        return inputPath;
    }

    public static Enum<MODEL_TYPE> getModelType() {
        return modelType;
    }

    public static String getOutputPath() {
        return outputPath;
    }
}
