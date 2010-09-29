/*
 * This class runs the models in opennlp.textgrounder.model
 */

package opennlp.textgrounder.app;

import opennlp.textgrounder.model.*;
import opennlp.textgrounder.text.*;
import opennlp.textgrounder.text.io.*;
import java.io.*;

public class RunModel extends BaseApp {

    public static void main(String[] args) throws Exception {

        initializeOptionsFromCommandLine(args);

        Corpus corpus = new StreamCorpus();
        //corpus.addSource(new TrXMLDocumentSource(getInputPath()));

        Model model;
        if(getModelType() == MODEL_TYPE.RANDOM)
            model = new RandomModel();
        else// if(getModelType() == MODEL_TYPE.BASIC_MIN_DIST)
            model = new BasicMinDistModel();

        Corpus disambiguated = model.disambiguate(corpus);

        BufferedWriter out = new BufferedWriter(new FileWriter(getOutputPath()));

        CorpusXMLWriter w = new CorpusXMLWriter(disambiguated);
        w.write(out);
    }

}
