/*
 * Base app for running resolvers and/or other functionality such as evaluation and visualization generation.
 */

package opennlp.textgrounder.tr.app;

import org.apache.commons.cli.*;
import opennlp.textgrounder.tr.topo.*;
import java.io.*;

public class BaseApp {

    private Options options = new Options();

    private String inputPath = null;
    private String additionalInputPath = null;
    private String graphInputPath = null;
    private String outputPath = null;
    private String xmlInputPath = null;
    private String kmlOutputPath = null;
    private String logFilePath = null;
    private boolean outputGoldLocations = false;
    private boolean outputUserKML = false;
    protected boolean useGoldToponyms = false;
    private String geoGazetteerFilename = null;
    private String serializedGazetteerPath = null;
    private String serializedCorpusInputPath = null;
    private String serializedCorpusOutputPath = null;
    private String maxentModelDirInputPath = null;

    private int sentsPerDocument = -1;

    private boolean highRecallNER = false;

    private Region boundingBox = null;

    protected boolean doKMeans = false;

    private String graphOutputPath = null;
    private String seedOutputPath = null;
    private String wikiInputPath = null;
    private String stoplistInputPath = null;
    
    private int numIterations = 1;

    private int knnForLP = -1;

    public static enum RESOLVER_TYPE {
        RANDOM,
        BASIC_MIN_DIST,
        WEIGHTED_MIN_DIST,
        DOC_DIST,
        TOPO_AS_DOC_DIST,
        LABEL_PROP,
        LABEL_PROP_DEFAULT_RULE,
        LABEL_PROP_CONTEXT_SENSITIVE,
        LABEL_PROP_COMPLEX,
        MAXENT,
        PROB,
        BAYES_RULE
    }
    protected Enum<RESOLVER_TYPE> resolverType = RESOLVER_TYPE.BASIC_MIN_DIST;

    public static enum CORPUS_FORMAT {
        PLAIN,
        TRCONLL,
        GEOTEXT,
        WIKITEXT
    }
    protected Enum<CORPUS_FORMAT> corpusFormat = CORPUS_FORMAT.PLAIN;
	

    protected void initializeOptionsFromCommandLine(String[] args) throws Exception {

        options.addOption("i", "input", true, "input path");
        options.addOption("ix", "input-xml", true, "xml input path");
        options.addOption("im", "input-models", true, "maxent model input directory");
        options.addOption("ia", "input-additional", true, "path to additional input data to be used in training but not evaluation");
        options.addOption("ig", "input-graph", true, "path to input graph for label propagation resolvers");
        options.addOption("r", "resolver", true, "resolver (RandomResolver, BasicMinDistResolver, WeightedMinDistResolver, LabelPropDefaultRuleResolver, LabelPropContextSensitiveResolver, LabelPropComplexResolver) [default = BasicMinDistResolver]");
        options.addOption("it", "iterations", true, "number of iterations for iterative models [default = 1]");
        options.addOption("o", "output", true, "output path");
        options.addOption("ok", "output-kml", true, "kml output path");
        options.addOption("oku", "output-kml-users", false, "output user-based KML rather than toponym-based KML");
        options.addOption("gold", "output-gold-locations", false, "output gold locations rather than system locations in KML");
        options.addOption("gt", "gold-toponyms", false, "use gold toponyms (named entities) if available");
        options.addOption("g", "geo-gazetteer-filename", true, "GeoNames gazetteer filename");
        options.addOption("sg", "serialized-gazetteer-path", true, "path to serialized GeoNames gazetteer");
        options.addOption("sci", "serialized-corpus-input-path", true, "path to serialized corpus for input");
        //options.addOption("sgci", "serialized-gold-corpus-input-path", true, "path to serialized gold corpus for input");
        options.addOption("sco", "serialized-corpus-output-path", true, "path to serialized corpus for output");
        //options.addOption("tr", "tr-conll", false, "read input path as TR-CoNLL directory");
        options.addOption("cf", "corpus-format", true, "corpus format (Plain, TrCoNLL, GeoText) [default = Plain]");

        options.addOption("spd", "sentences-per-document", true, "sentences per document (-1 for unlimited) [default = -1]");

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

        options.addOption("l", "log file input", true, "log file input, from document geolocation");
        options.addOption("knn", "knn", true, "k nearest neighbors to consider from document geolocation log file");

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
                    else if(option.getOpt().equals("ix"))
                        xmlInputPath = value;
                    else if(option.getOpt().equals("im"))
                        maxentModelDirInputPath = value;
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
                    else if(value.toLowerCase().startsWith("d"))
                        resolverType = RESOLVER_TYPE.DOC_DIST;
                    else if(value.toLowerCase().startsWith("t"))
                        resolverType = RESOLVER_TYPE.TOPO_AS_DOC_DIST;
                    else if(value.equalsIgnoreCase("labelprop"))
                        resolverType = RESOLVER_TYPE.LABEL_PROP;
                    else if(value.toLowerCase().startsWith("labelpropd"))
                        resolverType = RESOLVER_TYPE.LABEL_PROP_DEFAULT_RULE;
                    else if(value.toLowerCase().startsWith("labelpropcontext"))
                        resolverType = RESOLVER_TYPE.LABEL_PROP_CONTEXT_SENSITIVE;
                    else if(value.toLowerCase().startsWith("labelpropcomplex"))
                        resolverType = RESOLVER_TYPE.LABEL_PROP_COMPLEX;
                    else if(value.toLowerCase().startsWith("m"))
                        resolverType = RESOLVER_TYPE.MAXENT;
                    else if(value.toLowerCase().startsWith("p"))
                        resolverType = RESOLVER_TYPE.PROB;
                    else if(value.toLowerCase().startsWith("bayes"))
                        resolverType = RESOLVER_TYPE.BAYES_RULE;
                    else
                        resolverType = RESOLVER_TYPE.BASIC_MIN_DIST;
                    break; 
                case 'g':
                    if(option.getOpt().equals("g"))
                        geoGazetteerFilename = value;
                    else if(option.getOpt().equals("gold"))
                        outputGoldLocations = true;
                    else if(option.getOpt().equals("gt"))
                        useGoldToponyms = true;
                    break;
                case 's':
                    if(option.getOpt().equals("sg"))
                        serializedGazetteerPath = value;
                    else if(option.getOpt().equals("sci"))
                        serializedCorpusInputPath = value;
                    else if(option.getOpt().equals("sco"))
                        serializedCorpusOutputPath = value;
                    else if(option.getOpt().equals("spd"))
                        sentsPerDocument = Integer.parseInt(value);
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
                case 'l':
                    if(option.getOpt().equals("l"))
                        logFilePath = value;
                    break;
                case 'k':
                    if(option.getOpt().equals("knn"))
                        knnForLP = Integer.parseInt(value);
                    break;
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
                    break;
                case 'd':
                    doKMeans = true;
                    break;
            }
        }

        if(minLat != null && maxLat != null && minLon != null && maxLon != null)
            boundingBox = RectRegion.fromDegrees(minLat, maxLat, minLon, maxLon);
    }

    public static void checkExists(String filename) throws Exception {
        if(filename == null) {
            System.out.println("Null filename; aborting.");
            System.exit(0);
        }
        File f = new File(filename);
        if(!f.exists()) {
            System.out.println(filename + " doesn't exist; aborting.");
            System.exit(0);
        }
    }

    public String getInputPath() {
        return inputPath;
    }

    public String getXMLInputPath() {
        return xmlInputPath;
    }

    public String getAdditionalInputPath() {
        return additionalInputPath;
    }

    public String getMaxentModelDirInputPath() {
        return maxentModelDirInputPath;
    }

    public String getGraphInputPath() {
        return graphInputPath;
    }

    public Enum<RESOLVER_TYPE> getResolverType() {
        return resolverType;
    }

    public int getNumIterations() {
        return numIterations;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public String getKMLOutputPath() {
        return kmlOutputPath;
    }

    public boolean getOutputGoldLocations() {
        return outputGoldLocations;
    }

    public boolean getUseGoldToponyms() {
        return useGoldToponyms;
    }

    public boolean getOutputUserKML() {
        return outputUserKML;
    }

    public String getGeoGazetteerFilename() {
        return geoGazetteerFilename;
    }

    public String getSerializedGazetteerPath() {
        return serializedGazetteerPath;
    }

    public String getSerializedCorpusInputPath() {
        return serializedCorpusInputPath;
    }

    public String getSerializedCorpusOutputPath() {
        return serializedCorpusOutputPath;
    }


    public Enum<CORPUS_FORMAT> getCorpusFormat() {
        return corpusFormat;
    }

    public int getSentsPerDocument() {
        return sentsPerDocument;
    }
    
    public boolean isDoingKMeans() {
        return doKMeans;
    }

    public String getGraphOutputPath() {
        return graphOutputPath;
    }

    public String getSeedOutputPath() {
        return seedOutputPath;
    }

    public String getWikiInputPath() {
        return wikiInputPath;
    }

    public String getStoplistInputPath() {
        return stoplistInputPath;
    }

    public String getLogFilePath() {
        return logFilePath;
    }

    public int getKnnForLP() {
        return knnForLP;
    }

    public Region getBoundingBox() {
        return boundingBox;
    }

	public void setHighRecallNER(boolean highRecallNER) {
		highRecallNER = highRecallNER;
	}

	public boolean isHighRecallNER() {
		return highRecallNER;
	}
}
