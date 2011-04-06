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

        if(getInputPath() == null || getSerializedCorpusInputPath() == null) {
            System.out.println("Please specify both a system serialized corpus file via the -sci flag and a gold plaintext corpus file via the -i flag.");
            System.exit(0);
        }

        System.out.print("Reading serialized system corpus from " + getSerializedCorpusInputPath() + " ...");
        Corpus systemCorpus = TopoUtil.readCorpusFromSerialized(getSerializedCorpusInputPath());
        System.out.println("done.");

        Tokenizer tokenizer = new OpenNLPTokenizer();
        System.out.print("Reading plaintext gold corpus from " + getInputPath() + " ...");
        StoredCorpus goldCorpus;
        goldCorpus = Corpus.createStoredCorpus();
        goldCorpus.addSource(new TrXMLDirSource(new File(getInputPath()), tokenizer));
        goldCorpus.load();
        System.out.println("done.");

        doEval(systemCorpus, goldCorpus);
    }

    public static void doEval(Corpus systemCorpus, Corpus goldCorpus) throws Exception {
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
