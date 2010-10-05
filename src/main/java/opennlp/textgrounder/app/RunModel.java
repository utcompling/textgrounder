/*
 * This class runs the models in opennlp.textgrounder.model
 */

package opennlp.textgrounder.app;

import opennlp.textgrounder.model.*;
import opennlp.textgrounder.text.*;
import opennlp.textgrounder.text.io.*;
import opennlp.textgrounder.text.prep.*;
import opennlp.textgrounder.eval.*;
import java.io.*;

public class RunModel extends BaseApp {

    public static void main(String[] args) throws Exception {

        initializeOptionsFromCommandLine(args);

        Tokenizer tokenizer = new OpenNLPTokenizer();
        StoredCorpus corpus = Corpus.createStoredCorpus();
        corpus.addSource(new TrXMLDirSource(new File(getInputPath()), tokenizer));
        corpus.load();

        Model model;
        if(getModelType() == MODEL_TYPE.RANDOM)
            model = new RandomModel();
        else// if(getModelType() == MODEL_TYPE.BASIC_MIN_DIST)
            model = new BasicMinDistModel();

        StoredCorpus disambiguated = model.disambiguate(corpus);

        CorpusXMLWriter w = new CorpusXMLWriter(disambiguated);
        w.write(new File(getOutputPath()));

        Evaluator evaluator = new AccuracyEvaluator(disambiguated);

        Report report = evaluator.evaluate();

        System.out.println("Accuracy: " + report.computeAccuracy());
    }

}

