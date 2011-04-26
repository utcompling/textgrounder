/*
 * Base app for running resolvers and/or other functionality such as evaluation and visualization generation.
 */

package opennlp.textgrounder.app;

import org.apache.commons.cli.*;
import opennlp.textgrounder.topo.*;

public class BaseApp {

    private static Options options = new Options();

    private static String inputPath = null;
    private static String additionalInputPath = null;
    private static String graphInputPath = null;
    private static String outputPath = null;
    private static String kmlOutputPath = null;
    private static boolean outputGoldLocations = false;
    private static boolean outputUserKML = false;
    private static String geoGazetteerFilename = null;
    private static String serializedGazetteerPath = null;
    private static String serializedCorpusInputPath = null;
    //private static String serializedGoldCorpusInputPath = null;
    private static String serializedCorpusOutputPath = null;
    //private static boolean readAsTR = false;
    private static boolean highRecallNER = false;

    private static Region boundingBox = null;

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
    protected static Enum<RESOLVER_TYPE> resolverType = RESOLVER_TYPE.BASIC_MIN_DIST;

    public static enum CORPUS_FORMAT {
        PLAIN,
        TRCONLL,
        GEOTEXT
    }
    protected static Enum<CORPUS_FORMAT> corpusFormat = CORPUS_FORMAT.PLAIN;
	

    protected static void initializeOptionsFromCommandLine(String[] args) throws Exception {

        options.addOption("i", "input", true, "input path");
        options.addOption("ia", "input-additional", true, "path to additional input data to be used in training but not evaluation");
        options.addOption("ig", "input-graph", true, "path to input graph for label propagation resolvers");
        options.addOption("r", "resolver", true, "resolver (RandomResolver, BasicMinDistResolver, WeightedMinDistResolver, LabelPropDefaultRuleResolver, LabelPropContextSensitiveResolver, LabelPropComplexResolver) [default = BasicMinDistResolver]");
        options.addOption("it", "iterations", true, "number of iterations for iterative models [default = 1]");
        options.addOption("o", "output", true, "output path");
        options.addOption("ok", "output-kml", true, "kml output path");
        options.addOption("oku", "output-kml-users", false, "output user-based KML rather than toponym-based KML");
        options.addOption("gold", "output-gold-locations", false, "output gold locations rather than system locations in KML");
        options.addOption("g", "geo-gazetteer-filename", true, "GeoNames gazetteer filename");
        options.addOption("sg", "serialized-gazetteer-path", true, "path to serialized GeoNames gazetteer");
        options.addOption("sci", "serialized-corpus-input-path", true, "path to serialized corpus for input");
        //options.addOption("sgci", "serialized-gold-corpus-input-path", true, "path to serialized gold corpus for input");
        options.addOption("sco", "serialized-corpus-output-path", true, "path to serialized corpus for output");
        //options.addOption("tr", "tr-conll", false, "read input path as TR-CoNLL directory");
        options.addOption("cf", "corpus-format", true, "corpus format (Plain, TrCoNLL, GeoText) [default = Plain]");

        options.addOption("minlat", "minimum-latitude", true,
                "minimum latitude for bounding box");
        options.addOption("maxlat", "maximum-latitude", true,
                "maximum latitude for bounding box");
        options.addOption("minlon", "minimum-longitude", true,
                "minimum longitude for bounding box");
        options.addOption("maxlon", "maximum-longitude", true,
                "maximum longitude for bounding box");

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
        options.addOption("ner", "named-entity-recognizer", true,
        "option for using High Recall NER");
        
        options.addOption("h", "help", false, "print help");

        Double minLat = null;
        Double maxLat = null;
        Double minLon = null;
        Double maxLon = null;
        
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
                    else if(option.getOpt().equals("oku"))
                        outputUserKML = true;
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
                    else if(option.getOpt().equals("gold"))
                        outputGoldLocations = true;
                    break;
                case 's':
                    if(option.getOpt().equals("sg"))
                        serializedGazetteerPath = value;
                    else if(option.getOpt().equals("sci"))
                        serializedCorpusInputPath = value;
                    else if(option.getOpt().equals("sco"))
                        serializedCorpusOutputPath = value;
                    //else if(option.getOpt().equals("sgci"))
                    //    serializedGoldCorpusInputPath = value;
                    break;
                case 'c':
                    if(value.toLowerCase().startsWith("t"))
                        corpusFormat = CORPUS_FORMAT.TRCONLL;
                    else if(value.toLowerCase().startsWith("g"))
                        corpusFormat = CORPUS_FORMAT.GEOTEXT;
                    else//if(value.toLowerCase().startsWith("p"))
                        corpusFormat = CORPUS_FORMAT.PLAIN;
                    break;
                    /*case 't':
                    readAsTR = true;
                    break;*/

                case 'm':
                    if(option.getOpt().equals("minlat"))
                        minLat = Double.parseDouble(value.replaceAll("n", "-"));
                    else if(option.getOpt().equals("maxlat"))
                        maxLat = Double.parseDouble(value.replaceAll("n", "-"));
                    else if(option.getOpt().equals("minlon"))
                        minLon = Double.parseDouble(value.replaceAll("n", "-"));
                    else if(option.getOpt().equals("maxlon"))
                        maxLon = Double.parseDouble(value.replaceAll("n", "-"));
                    break;
                case 'n':
                	if(option.getOpt().equals("ner"))
                		setHighRecallNER(new Integer(value)!=0);
                		
                case 'd':
                    doKMeans = true;
                    break;
            }
        }

        if(minLat != null && maxLat != null && minLon != null && maxLon != null)
            boundingBox = RectRegion.fromDegrees(minLat, maxLat, minLon, maxLon);
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

    public static boolean getOutputGoldLocations() {
        return outputGoldLocations;
    }

    public static boolean getOutputUserKML() {
        return outputUserKML;
    }

    public static String getGeoGazetteerFilename() {
        return geoGazetteerFilename;
    }

    public static String getSerializedGazetteerPath() {
        return serializedGazetteerPath;
    }

    public static String getSerializedCorpusInputPath() {
        return serializedCorpusInputPath;
    }

    /*public static String getSerializedGoldCorpusInputPath() {
        return serializedGoldCorpusInputPath;
    }*/

    public static String getSerializedCorpusOutputPath() {
        return serializedCorpusOutputPath;
    }

    /*public static boolean isReadAsTR() {
        return readAsTR;
        }*/

    public static Enum<CORPUS_FORMAT> getCorpusFormat() {
        return corpusFormat;
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

    public static Region getBoundingBox() {
        return boundingBox;
    }

	public static void setHighRecallNER(boolean highRecallNER) {
		BaseApp.highRecallNER = highRecallNER;
	}

	public static boolean isHighRecallNER() {
		return highRecallNER;
	}
}
