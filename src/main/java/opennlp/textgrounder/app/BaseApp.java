/*
 * Base app for running resolvers and/or other functionality such as evaluation and visualization generation.
 */

package opennlp.textgrounder.app;

import org.apache.commons.cli.*;

public class BaseApp {

    private static Options options = new Options();

    private static String inputPath = null;
    private static String additionalInputPath = null;
    private static String graphInputPath = null;
    private static String outputPath = null;
    private static String kmlOutputPath = null;

    private static int numIterations = 1;

    public static enum RESOLVER_TYPE {
        RANDOM,
        BASIC_MIN_DIST,
        WEIGHTED_MIN_DIST,
        LABEL_PROP_DEFAULT_RULE,
        LABEL_PROP_CONTEXT_SENSITIVE
    }
    private static Enum<RESOLVER_TYPE> resolverType = RESOLVER_TYPE.BASIC_MIN_DIST;

    protected static void initializeOptionsFromCommandLine(String[] args) throws Exception {

        options.addOption("i", "input", true, "input path");
        options.addOption("ia", "input-additional", true, "path to additional input data to be used in training but not evaluation");
        options.addOption("ig", "input-graph", true, "path to graph for label propagation resolvers");
        options.addOption("r", "resolver", true, "resolver (RandomResolver, BasicMinDistResolver, WeightedMinDistResolver, LabelPropDefaultRuleResolver, LabelPropContextSensitiveResolver) [default = BasicMinDistResolver]");
        options.addOption("it", "iterations", true, "number of iterations for iterative models [default = 1]");
        options.addOption("o", "output", true, "output path [default = 'output.xml']");
        options.addOption("ok", "output-kml", true, "kml output path");

        options.addOption("h", "help", false, "print help");
        
        CommandLineParser optparse = new PosixParser();
        CommandLine cline = optparse.parse(options, args);

        if (cline.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java <app-class-name>", options);
            System.exit(0);
        }

        for (Option option : cline.getOptions()) {
            String value = option.getValue();
            switch (option.getOpt().charAt(0)) {
                case 'i':
                    if(option.getOpt().equals("i"))
                        inputPath = value;
                    else if(option.getOpt().equals("it"))
                        numIterations = Integer.parseInt(value);
                    else if(option.getOpt().equals("ia"))
                        additionalInputPath = value;
                    else if(option.getOpt().equals("ig"))
                        graphInputPath = value;
                    break;
                case 'o':
                    if(option.getOpt().equals("o"))
                        outputPath = value;
                    else if(option.getOpt().equals("ok"))
                        kmlOutputPath = value;
                    break;
                case 'r':
                    if(value.toLowerCase().startsWith("r"))
                        resolverType = RESOLVER_TYPE.RANDOM;
                    else if(value.toLowerCase().startsWith("w"))
                        resolverType = RESOLVER_TYPE.WEIGHTED_MIN_DIST;
                    else if(value.toLowerCase().startsWith("labelpropd"))
                        resolverType = RESOLVER_TYPE.LABEL_PROP_DEFAULT_RULE;
                    else if(value.toLowerCase().startsWith("labelpropc"))
                        resolverType = RESOLVER_TYPE.LABEL_PROP_CONTEXT_SENSITIVE;
                    else
                        resolverType = RESOLVER_TYPE.BASIC_MIN_DIST;
                    break;
            }
        }
    }

    public static String getInputPath() {
        return inputPath;
    }

    public static String getAdditionalInputPath() {
        return additionalInputPath;
    }

    public static String getGraphInputPath() {
        return graphInputPath;
    }

    public static Enum<RESOLVER_TYPE> getResolverType() {
        return resolverType;
    }

    public static int getNumIterations() {
        return numIterations;
    }

    public static String getOutputPath() {
        return outputPath;
    }

    public static String getKMLOutputPath() {
        return kmlOutputPath;
    }
}
