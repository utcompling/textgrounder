/*
 * This class runs the resolvers in opennlp.textgrounder.resolver
 */

package opennlp.textgrounder.app;

import opennlp.textgrounder.resolver.*;
import opennlp.textgrounder.text.*;
import opennlp.textgrounder.text.io.*;
import opennlp.textgrounder.text.prep.*;
import opennlp.textgrounder.topo.gaz.*;
import opennlp.textgrounder.eval.*;
import opennlp.textgrounder.util.*;
import java.io.*;
import java.util.*;
import java.util.zip.*;

public class RunResolver extends BaseApp {

    public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();

        initializeOptionsFromCommandLine(args);

        Tokenizer tokenizer = new OpenNLPTokenizer();
        OpenNLPRecognizer recognizer = new OpenNLPRecognizer();

        StoredCorpus goldCorpus = null;
        if(isReadAsTR()) {
            System.out.print("Reading gold corpus from " + getInputPath() + " ...");
            goldCorpus = Corpus.createStoredCorpus();
            goldCorpus.addSource(new TrXMLDirSource(new File(getInputPath()), tokenizer));
            goldCorpus.load();
            System.out.println("done.");
        }

        GeoNamesGazetteer gnGaz = null;
        if(getSerializedGazetteerPath() != null) {
            System.out.println("Reading serialized GeoNames gazetteer from " + getSerializedGazetteerPath() + " ...");
            ObjectInputStream ois = null;
            if(getSerializedGazetteerPath().toLowerCase().endsWith(".gz")) {
                GZIPInputStream gis = new GZIPInputStream(new FileInputStream(getSerializedGazetteerPath()));
                ois = new ObjectInputStream(gis);
            }
            else {
                FileInputStream fis = new FileInputStream(getSerializedGazetteerPath());
                ois = new ObjectInputStream(fis);
            }
            long startMemoryUse = MemoryUtil.getMemoryUsage();
            gnGaz = (GeoNamesGazetteer) ois.readObject();
            long endMemoryUse = MemoryUtil.getMemoryUsage();
            System.out.println("Size of gazetteer object in bytes: " + (endMemoryUse - startMemoryUse));
            System.out.println("Done.");
        }
        else if(getGeoGazetteerFilename() != null) {
            System.out.println("Reading GeoNames gazetteer from " + Constants.getGazetteersDir() + File.separator + getGeoGazetteerFilename()+" ...");
            gnGaz = new GeoNamesGazetteer(new BufferedReader(
                    new FileReader(Constants.getGazetteersDir() + File.separator + getGeoGazetteerFilename())));
            System.out.println("Done.");
        }
        else {
            System.out.println("Must specify a gazetteer.");
            System.exit(0);
        }

        System.out.print("Reading test corpus from " + getInputPath() + " ...");
        StoredCorpus testCorpus = Corpus.createStoredCorpus();
        if(isReadAsTR()) {
            testCorpus.addSource(new ToponymAnnotator(
                new ToponymRemover(new TrXMLDirSource(new File(getInputPath()), tokenizer)),
                recognizer, gnGaz));
        }
	else if (getInputPath().endsWith("txt")) {
	    
            testCorpus.addSource(new ToponymAnnotator(new PlainTextSource(
									  new BufferedReader(new FileReader(getInputPath())), new OpenNLPSentenceDivider(), tokenizer),
                recognizer, gnGaz));
	}
        else {
            testCorpus.addSource(new ToponymAnnotator(new PlainTextDirSource(
                new File(getInputPath()), new OpenNLPSentenceDivider(), tokenizer),
                recognizer, gnGaz));
        }
        testCorpus.load();
        System.out.println("done.");

        StoredCorpus trainCorpus = Corpus.createStoredCorpus();
        if(getAdditionalInputPath() != null) {
            System.out.print("Reading additional training corpus from " + getAdditionalInputPath() + " ...");
            List<Gazetteer> gazList = new ArrayList<Gazetteer>();
            LoadableGazetteer trGaz = new InMemoryGazetteer();
            trGaz.load(new CorpusGazetteerReader(testCorpus));
            LoadableGazetteer otherGaz = new InMemoryGazetteer();
            otherGaz.load(new WorldReader(new File(Constants.getGazetteersDir() + File.separator + "dataen-fixed.txt.gz")));
            gazList.add(trGaz);
            gazList.add(otherGaz);
            Gazetteer multiGaz = new MultiGazetteer(gazList);
            /*trainCorpus.addSource(new ToponymAnnotator(new PlainTextSource(
                    new BufferedReader(new FileReader(getAdditionalInputPath())), new OpenNLPSentenceDivider(), tokenizer),
                    recognizer,
                    multiGaz));*/
            trainCorpus.addSource(new ToponymAnnotator(new GigawordSource(
                    new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(new FileInputStream(getAdditionalInputPath())))), 10, 40000),
                    recognizer,
                    multiGaz));
            trainCorpus.addSource(new TrXMLDirSource(new File(getInputPath()), tokenizer));
            trainCorpus.load();
            System.out.println("done.");
        }

        long endTime = System.currentTimeMillis();
        float seconds = (endTime - startTime) / 1000F;
        System.out.println("\nInitialization took " + Float.toString(seconds/(float)60.0) + " minutes.");

        System.out.println("\nNumber of documents: " + testCorpus.getDocumentCount());
        System.out.println("Number of toponym types: " + testCorpus.getToponymTypeCount());
        System.out.println("Maximum ambiguity (locations per toponym): " + testCorpus.getMaxToponymAmbiguity() + "\n");

        Resolver resolver;
        if(getResolverType() == RESOLVER_TYPE.RANDOM) {
            System.out.print("Running RANDOM resolver...");
            resolver = new RandomResolver();
        }
        else if(getResolverType() == RESOLVER_TYPE.WEIGHTED_MIN_DIST) {
            System.out.println("Running WEIGHTED MINIMUM DISTANCE resolver with " + getNumIterations() + " iteration(s)...");
            resolver = new WeightedMinDistResolver(getNumIterations());
        }
        else if(getResolverType() == RESOLVER_TYPE.LABEL_PROP_DEFAULT_RULE) {
            System.out.print("Running LABEL PROP DEFAULT RULE resolver, using graph at " + getGraphInputPath() + " ...");
            resolver = new LabelPropDefaultRuleResolver(getGraphInputPath());
        }
        else if(getResolverType() == RESOLVER_TYPE.LABEL_PROP_CONTEXT_SENSITIVE) {
            System.out.print("Running LABEL PROP CONTEXT SENSITIVE resolver, using graph at " + getGraphInputPath() + " ...");
            resolver = new LabelPropContextSensitiveResolver(getGraphInputPath());
        }
        else if(getResolverType() == RESOLVER_TYPE.LABEL_PROP_COMPLEX) {
            System.out.print("Running LABEL PROP COMPLEX resolver, using graph at " + getGraphInputPath() + " ...");
            resolver = new LabelPropComplexResolver(getGraphInputPath());
        }
        else {//if(getResolverType() == RESOLVER_TYPE.BASIC_MIN_DIST) {
            System.out.print("Running BASIC MINIMUM DISTANCE resolver...");
            resolver = new BasicMinDistResolver();
        }

        if(getAdditionalInputPath() != null)
            resolver.train(trainCorpus);
        Corpus disambiguated = resolver.disambiguate(testCorpus);

        System.out.println("done.");

        if(goldCorpus != null) {
            System.out.print("\nEvaluating...");
            Evaluator evaluator = new SignatureEvaluator(goldCorpus);
            Report report = evaluator.evaluate(disambiguated, false);
            System.out.println("done.");

            System.out.println("\nResults:");
            System.out.println("P: " + report.getPrecision());
            System.out.println("R: " + report.getRecall());
            System.out.println("F: " + report.getFScore());
            System.out.println("A: " + report.getAccuracy() + "\n");
        }

        if(getOutputPath() != null) {
            System.out.print("Writing resolved corpus in XML format to " + getOutputPath() + " ...");
            CorpusXMLWriter w = new CorpusXMLWriter(disambiguated);
            w.write(new File(getOutputPath()));
            System.out.println("done.");
        }

        if(getKMLOutputPath() != null) {
            System.out.print("Writing visualizable resolved corpus in KML format to " + getKMLOutputPath() + " ...");
            CorpusKMLWriter kw = new CorpusKMLWriter(disambiguated);
            kw.write(new File(getKMLOutputPath()));
            System.out.println("done.");
        }

        endTime = System.currentTimeMillis();
        seconds = (endTime - startTime) / 1000F;
        System.out.println("\nTotal time elapsed: " + Float.toString(seconds/(float)60.0) + " minutes.");
    }
}
