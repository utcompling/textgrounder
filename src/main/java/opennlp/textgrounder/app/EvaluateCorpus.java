/* Evaluates a given corpus with system disambiguated toponyms againt a given gold corpus.
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

public class EvaluateCorpus extends BaseApp {
    public static void main(String[] args) throws Exception {
        initializeOptionsFromCommandLine(args);

        if(getCorpusFormat() == CORPUS_FORMAT.TRCONLL) {
            if(getInputPath() == null || (getSerializedCorpusInputPath() == null && getXMLInputPath() == null)) {
                System.out.println("Please specify both a system serialized corpus file via the -sci or -ix flag and a gold plaintext corpus file via the -i flag.");
                System.exit(0);
            }
        }
        else {
            if(getSerializedCorpusInputPath() == null) {
                System.out.println("Please specify a system serialized corpus file via the -sci flag.");
                System.exit(0);
            }
        }

        StoredCorpus systemCorpus;
        if(getSerializedCorpusInputPath() != null) {
            System.out.print("Reading serialized system corpus from " + getSerializedCorpusInputPath() + " ...");
            systemCorpus = TopoUtil.readStoredCorpusFromSerialized(getSerializedCorpusInputPath());
            System.out.println("done.");
        }
        else {// if(getXMLInputPath() != null) {
            Tokenizer tokenizer = new OpenNLPTokenizer();
            systemCorpus = Corpus.createStoredCorpus();
            systemCorpus.addSource(new CorpusXMLSource(new BufferedReader(new FileReader(getXMLInputPath())),
                                                       tokenizer));
            systemCorpus.setFormat(getCorpusFormat()==null?CORPUS_FORMAT.PLAIN:getCorpusFormat());
            systemCorpus.load();
        }

        StoredCorpus goldCorpus = null;

        if(getInputPath() != null) {
            Tokenizer tokenizer = new OpenNLPTokenizer();
            System.out.print("Reading plaintext gold corpus from " + getInputPath() + " ...");
            goldCorpus = Corpus.createStoredCorpus();
            goldCorpus.addSource(new TrXMLDirSource(new File(getInputPath()), tokenizer));
            goldCorpus.load();
            System.out.println("done.");
        }

        doEval(systemCorpus, goldCorpus, getCorpusFormat());
    }

    public static void doEval(Corpus systemCorpus, Corpus goldCorpus, Enum<CORPUS_FORMAT> corpusFormat) throws Exception {
        if(corpusFormat == CORPUS_FORMAT.GEOTEXT) {
            System.out.print("\nEvaluating...");
            DocDistanceEvaluator evaluator = new DocDistanceEvaluator(systemCorpus);
            DistanceReport dreport = evaluator.evaluate();
            System.out.println("done.");
            
            System.out.println("\nMean error distance (km): " + dreport.getMeanDistance());
            System.out.println("Median error distance (km): " + dreport.getMedianDistance());
            System.out.println("Minimum error distance (km): " + dreport.getMinDistance());
            System.out.println("Maximum error distance (km): " + dreport.getMaxDistance());
            System.out.println("Total documents evaluated: " + dreport.getNumDistances());
        }
        else {
            System.out.print("\nEvaluating...");
            Evaluator evaluator = new SignatureEvaluator(goldCorpus);
            Report report = evaluator.evaluate(systemCorpus, false);
            System.out.println("done.");

            System.out.println("\nResults:");
            System.out.println("P: " + report.getPrecision());
            System.out.println("R: " + report.getRecall());
            System.out.println("F: " + report.getFScore());
            System.out.println("A: " + report.getAccuracy());
        }
    }
}
