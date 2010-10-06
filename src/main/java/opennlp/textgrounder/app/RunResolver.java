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
            resolver = new RandomResolver();
        }
        else {// if(getResolverType() == RESOLVER_TYPE.BASIC_MIN_DIST)
            resolver = new BasicMinDistResolver();
        }

        StoredCorpus disambiguated = resolver.disambiguate(corpus);

        CorpusXMLWriter w = new CorpusXMLWriter(disambiguated);
        w.write(new File(getOutputPath()));

        Evaluator evaluator = new AccuracyEvaluator(disambiguated);

        Report report = evaluator.evaluate();

        System.out.println("Accuracy: " + report.getAccuracy());
    }

}

