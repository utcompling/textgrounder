/*
 * This class runs the models in opennlp.textgrounder.model
 */

package opennlp.textgrounder.app;

import opennlp.textgrounder.model.*;
import opennlp.textgrounder.text.*;
import opennlp.textgrounder.text.io.*;
import opennlp.textgrounder.text.prep.*;
import java.io.*;

public class RunModel extends BaseApp {

    public static void main(String[] args) throws Exception {

        initializeOptionsFromCommandLine(args);

        Tokenizer tokenizer = new OpenNLPTokenizer();
        StoredCorpus corpus = Corpus.createStoredCorpus();
        corpus.addSource(new TrXMLDirSource(new File(getInputPath()), tokenizer));

        Model model;
        if(getModelType() == MODEL_TYPE.RANDOM)
            model = new RandomModel();
        else// if(getModelType() == MODEL_TYPE.BASIC_MIN_DIST)
            model = new BasicMinDistModel();

        StoredCorpus disambiguated = model.disambiguate(corpus);

        BufferedWriter out = new BufferedWriter(new FileWriter(getOutputPath()));

        CorpusXMLWriter w = new CorpusXMLWriter(disambiguated);
        w.write(out);
    }

}
