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
    private static String geoGazetteerFilename = null;
    private static String serializedGazetteerPath = null;
    private static String serializedCorpusPath = null;
    private static boolean readAsTR = false;

    private static boolean doKMeans = false;

    private static String graphOutputPath = null;
    private static String seedOutputPath = null;
    private static String wikiInputPath = null;
    private static String stoplistInputPath = null;
    
    private static int numIterations = 1;

    public static enum RESOLVER_TYPE {
        RANDOM,
        BASIC_MIN_DIST,
        WEIGHTED_MIN_DIST,
        LABEL_PROP_DEFAULT_RULE,
        LABEL_PROP_CONTEXT_SENSITIVE,
        LABEL_PROP_COMPLEX
    }
    private static Enum<RESOLVER_TYPE> resolverType = RESOLVER_TYPE.BASIC_MIN_DIST;

	

    protected static void initializeOptionsFromCommandLine(String[] args) throws Exception {

        options.addOption("i", "input", true, "input path");
        options.addOption("ia", "input-additional", true, "path to additional input data to be used in training but not evaluation");
        options.addOption("ig", "input-graph", true, "path to input graph for label propagation resolvers");
        options.addOption("r", "resolver", true, "resolver (RandomResolver, BasicMinDistResolver, WeightedMinDistResolver, LabelPropDefaultRuleResolver, LabelPropContextSensitiveResolver, LabelPropComplexResolver) [default = BasicMinDistResolver]");
        options.addOption("it", "iterations", true, "number of iterations for iterative models [default = 1]");
        options.addOption("o", "output", true, "output path");
        options.addOption("ok", "output-kml", true, "kml output path");
        options.addOption("g", "geo-gazetteer-filename", true, "GeoNames gazetteer filename");
        options.addOption("sg", "serialized-gazetteer-path", true, "path to serialized GeoNames gazetteer");
        options.addOption("sc", "serialized-corpus-path", true, "path to serialized corpus");
        options.addOption("tr", "tr-conll", false, "read input path as TR-CoNLL directory");

        options.addOption("dkm", "do-k-means-multipoints", false,
                "(import-gazetteer only) run k-means and create multipoint representations of regions (e.g. countries)");

        options.addOption("og", "output-graph", true,
                "(preprocess-labelprop only) path to output graph file");
        options.addOption("os", "output-seed", true,
                "(preprocess-labelprop only) path to output seed file");
        options.addOption("iw", "input-wiki", true,
                "(preprocess-labelprop only) path to wikipedia file (article titles, article IDs, and word lists)");
        options.addOption("is", "input-stoplist", true,
                "(preprocess-labelprob only) path to stop list input file (one stop word per line)");

        options.addOption("h", "help", false, "print help");
        
        CommandLineParser optparse = new PosixParser();
        CommandLine cline = optparse.parse(options, args);

        if (cline.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("textgrounder [command] ", options);
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
                    else if(option.getOpt().equals("iw"))
                        wikiInputPath = value;
                    else if(option.getOpt().equals("is"))
                        stoplistInputPath = value;
                    break;
                case 'o':
                    if(option.getOpt().equals("o"))
                        outputPath = value;
                    else if(option.getOpt().equals("og"))
                        graphOutputPath = value;
                    else if(option.getOpt().equals("ok"))
                        kmlOutputPath = value;
                    else if(option.getOpt().equals("os"))
                        seedOutputPath = value;
                    break;
                case 'r':
                    if(value.toLowerCase().startsWith("r"))
                        resolverType = RESOLVER_TYPE.RANDOM;
                    else if(value.toLowerCase().startsWith("w"))
                        resolverType = RESOLVER_TYPE.WEIGHTED_MIN_DIST;
                    else if(value.toLowerCase().startsWith("labelpropd"))
                        resolverType = RESOLVER_TYPE.LABEL_PROP_DEFAULT_RULE;
                    else if(value.toLowerCase().startsWith("labelpropcontext"))
                        resolverType = RESOLVER_TYPE.LABEL_PROP_CONTEXT_SENSITIVE;
                    else if(value.toLowerCase().startsWith("labelpropcomplex"))
                        resolverType = RESOLVER_TYPE.LABEL_PROP_COMPLEX;
                    else
                        resolverType = RESOLVER_TYPE.BASIC_MIN_DIST;
                    break; 
                case 'g':
                    if(option.getOpt().equals("g"))
                        geoGazetteerFilename = value;
                    break;
                case 's':
                    if(option.getOpt().equals("sg"))
                        serializedGazetteerPath = value;
                    else if(option.getOpt().equals("sc"))
                        serializedCorpusPath = value;
                    break;
                case 't':
                    readAsTR = true;
                    break;
                case 'd':
                    doKMeans = true;
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

    public static String getGeoGazetteerFilename() {
        return geoGazetteerFilename;
    }

    public static String getSerializedGazetteerPath() {
        return serializedGazetteerPath;
    }

    public static String getSerializedCorpusPath() {
        return serializedCorpusPath;
    }

    public static boolean isReadAsTR() {
        return readAsTR;
    }

    
    public static boolean isDoingKMeans() {
        return doKMeans;
    }

    public static String getGraphOutputPath() {
        return graphOutputPath;
    }

    public static String getSeedOutputPath() {
        return seedOutputPath;
    }

    public static String getWikiInputPath() {
        return wikiInputPath;
    }

    public static String getStoplistInputPath() {
        return stoplistInputPath;
    }
}
