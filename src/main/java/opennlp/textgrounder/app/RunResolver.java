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

        initializeOptionsFromCommandLine(args);

        Tokenizer tokenizer = new OpenNLPTokenizer();

        StoredCorpus testCorpus = Corpus.createStoredCorpus();
        System.out.print("Reading corpus from " + getInputPath() + " ...");
        testCorpus.addSource(new TrXMLDirSource(new File(getInputPath()), tokenizer));
        testCorpus.load();
        System.out.println("done.");

        StoredCorpus trainCorpus = Corpus.createStoredCorpus();
        if(getAdditionalInputPath() != null) {
            System.out.print("Reading additional training corpus from " + getAdditionalInputPath() + " ...");
            List<Gazetteer> gazList = new ArrayList<Gazetteer>();
            Gazetteer trGaz = new InMemoryGazetteer();
            trGaz.load(new CorpusGazetteerReader(testCorpus));
            Gazetteer otherGaz = new InMemoryGazetteer();
            otherGaz.load(new WorldReader(new File(Constants.getGazetteersDir() + File.separator + "dataen-fixed.txt.gz")));
            gazList.add(trGaz);
            gazList.add(otherGaz);
            Gazetteer multiGaz = new MultiGazetteer(gazList);
            /*trainCorpus.addSource(new ToponymAnnotator(new PlainTextSource(
                    new BufferedReader(new FileReader(getAdditionalInputPath())), new OpenNLPSentenceDivider(), tokenizer),
                    new OpenNLPRecognizer(),
                    multiGaz));*/
            trainCorpus.addSource(new ToponymAnnotator(new GigawordSource(
                    new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(new FileInputStream(getAdditionalInputPath())))), 50, 100),
                    new OpenNLPRecognizer(),
                    multiGaz));
            trainCorpus.addSource(new TrXMLDirSource(new File(getInputPath()), tokenizer));
            trainCorpus.load();
        }
        System.out.println("done.");

        System.out.println("Number of documents: " + testCorpus.getDocumentCount());
        System.out.println("Number of toponym types: " + testCorpus.getToponymTypeCount());
        System.out.println("Maximum ambiguity (locations per toponym): " + testCorpus.getMaxToponymAmbiguity());

        Resolver resolver;
        if(getResolverType() == RESOLVER_TYPE.RANDOM) {
            System.out.println("Running RANDOM resolver...");
            resolver = new RandomResolver();
        }
        else if(getResolverType() == RESOLVER_TYPE.WEIGHTED_MIN_DIST) {
            System.out.println("Running WEIGHTED MINIMUM DISTANCE resolver with " + getNumIterations() + " iteration(s)...");
            resolver = new WeightedMinDistResolver(getNumIterations());
        }
        else {//if(getResolverType() == RESOLVER_TYPE.BASIC_MIN_DIST) {
            System.out.println("Running BASIC MINIMUM DISTANCE resolver...");
            resolver = new BasicMinDistResolver();
        }

        if(getAdditionalInputPath() != null)
            resolver.train(trainCorpus);
        Corpus disambiguated = resolver.disambiguate(testCorpus);

        CorpusXMLWriter w = new CorpusXMLWriter(disambiguated);
        w.write(new File(getOutputPath()));

        Evaluator evaluator = new SignatureEvaluator(testCorpus);

        Report report = evaluator.evaluate(disambiguated, false);

        System.out.println("P: " + report.getPrecision());
        System.out.println("R: " + report.getRecall());
        System.out.println("F: " + report.getFScore());
        System.out.println("A: " + report.getAccuracy());
    }

}

