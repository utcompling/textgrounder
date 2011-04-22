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

        if(getSerializedGazetteerPath() == null && getSerializedCorpusInputPath() == null) {
            System.out.println("Abort: you must specify a path to a serialized gazetteer or corpus. To generate one, run ImportGazetteer and/or ImportCorpus.");
            System.exit(0);
        }

        Tokenizer tokenizer = new OpenNLPTokenizer();
        OpenNLPRecognizer recognizer = new OpenNLPRecognizer();

        StoredCorpus goldCorpus = null;
        if(getCorpusFormat() == CORPUS_FORMAT.TRCONLL) {
            System.out.print("Reading gold corpus from " + getInputPath() + " ...");
            goldCorpus = Corpus.createStoredCorpus();
            goldCorpus.addSource(new TrXMLDirSource(new File(getInputPath()), tokenizer));
            goldCorpus.setFormat(CORPUS_FORMAT.TRCONLL);
            goldCorpus.load();
            System.out.println("done.");
        }

        StoredCorpus testCorpus;
        if(getSerializedCorpusInputPath() != null) {
            System.out.print("Reading serialized corpus from " + getSerializedCorpusInputPath() + " ...");
            testCorpus = TopoUtil.readStoredCorpusFromSerialized(getSerializedCorpusInputPath());
            System.out.println("done.");
        }
        else {
            testCorpus = ImportCorpus.doImport(getInputPath(), getSerializedGazetteerPath(), getCorpusFormat());
        }

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
            trainCorpus.setFormat(getCorpusFormat());
            trainCorpus.load();
            System.out.println("done.");
        }

        long endTime = System.currentTimeMillis();
        float seconds = (endTime - startTime) / 1000F;
        System.out.println("\nInitialization took " + Float.toString(seconds/(float)60.0) + " minutes.");

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
        StoredCorpus disambiguated = resolver.disambiguate(testCorpus);
        disambiguated.setFormat(getCorpusFormat());
        if(getCorpusFormat() == CORPUS_FORMAT.GEOTEXT) {
            if(getBoundingBox() != null)
                System.out.println("\nOnly disambiguating documents within bounding box: " + getBoundingBox().toString());
            SimpleDocumentResolver dresolver = new SimpleDocumentResolver();
            disambiguated = dresolver.disambiguate(disambiguated, getBoundingBox());
        }

        System.out.println("done.\n");

        if(goldCorpus != null || getCorpusFormat() == CORPUS_FORMAT.GEOTEXT) {
            EvaluateCorpus.doEval(disambiguated, goldCorpus, corpusFormat);
        }

        if(getSerializedCorpusOutputPath() != null) {
            ImportCorpus.serialize(disambiguated, getSerializedCorpusOutputPath());
        }

        if(getOutputPath() != null) {
            System.out.print("Writing resolved corpus in XML format to " + getOutputPath() + " ...");
            CorpusXMLWriter w = new CorpusXMLWriter(disambiguated);
            w.write(new File(getOutputPath()));
            System.out.println("done.");
        }

        if(getKMLOutputPath() != null) {
            WriteCorpusToKML.writeToKML(disambiguated, getKMLOutputPath(), getOutputGoldLocations(), getOutputUserKML(), getCorpusFormat());
        }

        endTime = System.currentTimeMillis();
        seconds = (endTime - startTime) / 1000F;
        System.out.println("\nTotal time elapsed: " + Float.toString(seconds/(float)60.0) + " minutes.");
    }
}
