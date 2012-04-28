/* Evaluates a given corpus with system disambiguated toponyms againt a given gold corpus.
 */

package opennlp.textgrounder.tr.app;

import opennlp.textgrounder.tr.resolver.*;
import opennlp.textgrounder.tr.text.*;
import opennlp.textgrounder.tr.text.io.*;
import opennlp.textgrounder.tr.text.prep.*;
import opennlp.textgrounder.tr.topo.gaz.*;
import opennlp.textgrounder.tr.eval.*;
import opennlp.textgrounder.tr.util.*;
import java.io.*;
import java.util.*;
import java.util.zip.*;

public class EvaluateCorpus extends BaseApp {

    public static void main(String[] args) throws Exception {

        EvaluateCorpus currentRun = new EvaluateCorpus();
        currentRun.initializeOptionsFromCommandLine(args);

        if(currentRun.getCorpusFormat() == CORPUS_FORMAT.TRCONLL) {
            if(currentRun.getInputPath() == null || (currentRun.getSerializedCorpusInputPath() == null && currentRun.getXMLInputPath() == null)) {
                System.out.println("Please specify both a system annotated corpus file via the -sci or -ix flag and a gold plaintext corpus file via the -i flag.");
                System.exit(0);
            }
        }
        else {
            if(currentRun.getSerializedCorpusInputPath() == null && currentRun.getXMLInputPath() == null) {
                System.out.println("Please specify a system annotated corpus file via the -sci or -ix flag.");
                System.exit(0);
            }
        }

        StoredCorpus systemCorpus;
        if(currentRun.getSerializedCorpusInputPath() != null) {
            System.out.print("Reading serialized system corpus from " + currentRun.getSerializedCorpusInputPath() + " ...");
            systemCorpus = TopoUtil.readStoredCorpusFromSerialized(currentRun.getSerializedCorpusInputPath());
            System.out.println("done.");
        }
        else {// if(getXMLInputPath() != null) {
            Tokenizer tokenizer = new OpenNLPTokenizer();
            systemCorpus = Corpus.createStoredCorpus();
            systemCorpus.addSource(new CorpusXMLSource(new BufferedReader(new FileReader(currentRun.getXMLInputPath())),
                                                       tokenizer));
            systemCorpus.setFormat(currentRun.getCorpusFormat()==null?CORPUS_FORMAT.PLAIN:currentRun.getCorpusFormat());
            systemCorpus.load();
        }

        StoredCorpus goldCorpus = null;

        if(currentRun.getInputPath() != null && currentRun.getCorpusFormat() == CORPUS_FORMAT.TRCONLL) {
            Tokenizer tokenizer = new OpenNLPTokenizer();
            System.out.print("Reading plaintext gold corpus from " + currentRun.getInputPath() + " ...");
            goldCorpus = Corpus.createStoredCorpus();
            goldCorpus.addSource(new TrXMLDirSource(new File(currentRun.getInputPath()), tokenizer));
            goldCorpus.load();
            System.out.println("done.");
        }

        currentRun.doEval(systemCorpus, goldCorpus, currentRun.getCorpusFormat(), currentRun.getUseGoldToponyms());
    }

    public void doEval(Corpus systemCorpus, Corpus goldCorpus, Enum<BaseApp.CORPUS_FORMAT> corpusFormat, boolean useGoldToponyms) throws Exception {
        System.out.print("\nEvaluating...");
        if(corpusFormat == CORPUS_FORMAT.GEOTEXT) {
            DocDistanceEvaluator evaluator = new DocDistanceEvaluator(systemCorpus);
            DistanceReport dreport = evaluator.evaluate();

            System.out.println("\nMean error distance (km): " + dreport.getMeanDistance());
            System.out.println("Median error distance (km): " + dreport.getMedianDistance());
            System.out.println("Minimum error distance (km): " + dreport.getMinDistance());
            System.out.println("Maximum error distance (km): " + dreport.getMaxDistance());
            System.out.println("Fraction of distances within 161 km: " + dreport.getFractionDistancesWithinThreshold(161.0));
            System.out.println("Total documents evaluated: " + dreport.getNumDistances());
        }

        else {
            SignatureEvaluator evaluator = new SignatureEvaluator(goldCorpus);
            Report report = evaluator.evaluate(systemCorpus, false);
            DistanceReport dreport = evaluator.getDistanceReport();

            System.out.println("\nP: " + report.getPrecision());
            System.out.println("R: " + report.getRecall());
            System.out.println("F: " + report.getFScore());
            //System.out.println("A: " + report.getAccuracy());

            System.out.println("\nMean error distance (km): " + dreport.getMeanDistance());
            System.out.println("Median error distance (km): " + dreport.getMedianDistance());
            System.out.println("Minimum error distance (km): " + dreport.getMinDistance());
            System.out.println("Maximum error distance (km): " + dreport.getMaxDistance());
            System.out.println("Fraction of distances within 161 km: " + dreport.getFractionDistancesWithinThreshold(161.0));
            System.out.println("Total toponyms evaluated: " + dreport.getNumDistances());
        }
    }

}
