/*
 * This class runs the resolvers in opennlp.textgrounder.resolver
 */

package opennlp.textgrounder.app;

import opennlp.textgrounder.resolver.*;
import opennlp.textgrounder.text.*;
import opennlp.textgrounder.text.io.*;
import opennlp.textgrounder.text.prep.*;
import opennlp.textgrounder.eval.*;
import java.io.*;

public class RunResolver extends BaseApp {

    public static void main(String[] args) throws Exception {

        initializeOptionsFromCommandLine(args);

        Tokenizer tokenizer = new OpenNLPTokenizer();
        StoredCorpus corpus = Corpus.createStoredCorpus();
        corpus.addSource(new TrXMLDirSource(new File(getInputPath()), tokenizer));
        corpus.load();

        Resolver resolver;
        if(getResolverType() == RESOLVER_TYPE.RANDOM) {
            System.out.println("Running RANDOM resolver...");
            resolver = new RandomResolver();
        }
        else if(getResolverType() == RESOLVER_TYPE.WEIGHTED_MIN_DIST) {
            System.out.println("Running WEIGHTED MINIMUM DISTANCE resolver with " + getNumIterations() + " iterations...");
            resolver = new WeightedMinDistResolver(getNumIterations());
        }
        else {//if(getResolverType() == RESOLVER_TYPE.BASIC_MIN_DIST) {
            System.out.println("Running BASIC MINIMUM DISTANCE resolver...");
            resolver = new BasicMinDistResolver();
        }

        Corpus disambiguated = resolver.disambiguate(corpus);

        CorpusXMLWriter w = new CorpusXMLWriter(disambiguated);
        w.write(new File(getOutputPath()));

        Evaluator evaluator = new SignatureEvaluator(corpus);

        Report report = evaluator.evaluate(disambiguated, false);

        System.out.println("P: " + report.getPrecision());
        System.out.println("R: " + report.getRecall());
        System.out.println("F: " + report.getFScore());
        System.out.println("A: " + report.getAccuracy());
    }

}

