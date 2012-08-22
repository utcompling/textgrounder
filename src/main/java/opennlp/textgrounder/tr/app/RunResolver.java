/*
 * This class runs the resolvers in opennlp.textgrounder.tr.resolver
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

public class RunResolver extends BaseApp {

    public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();

        RunResolver currentRun = new RunResolver();
        currentRun.initializeOptionsFromCommandLine(args);

        if(currentRun.getSerializedGazetteerPath() == null && currentRun.getSerializedCorpusInputPath() == null) {
            System.out.println("Abort: you must specify a path to a serialized gazetteer or corpus. To generate one, run ImportGazetteer and/or ImportCorpus.");
            System.exit(0);
        }

        Tokenizer tokenizer = new OpenNLPTokenizer();
        OpenNLPRecognizer recognizer = new OpenNLPRecognizer();

        StoredCorpus goldCorpus = null;
        if(currentRun.getCorpusFormat() == CORPUS_FORMAT.TRCONLL) {
            System.out.print("Reading gold corpus from " + currentRun.getInputPath() + " ...");
            goldCorpus = Corpus.createStoredCorpus();
            File goldFile = new File(currentRun.getInputPath());
            if(goldFile.isDirectory())
                goldCorpus.addSource(new TrXMLDirSource(goldFile, tokenizer));
            else
                goldCorpus.addSource(new TrXMLSource(new BufferedReader(new FileReader(goldFile)), tokenizer));
            goldCorpus.setFormat(CORPUS_FORMAT.TRCONLL);
            goldCorpus.load();
            System.out.println("done.");
        }

        StoredCorpus testCorpus;
        if(currentRun.getSerializedCorpusInputPath() != null) {
            System.out.print("Reading serialized corpus from " + currentRun.getSerializedCorpusInputPath() + " ...");
            testCorpus = TopoUtil.readStoredCorpusFromSerialized(currentRun.getSerializedCorpusInputPath());
            System.out.println("done.");
        }
        else {
            ImportCorpus importCorpus = new ImportCorpus();
            testCorpus = importCorpus.doImport(currentRun.getInputPath(), currentRun.getSerializedGazetteerPath(), currentRun.getCorpusFormat(), currentRun.getUseGoldToponyms());
        }

        StoredCorpus trainCorpus = Corpus.createStoredCorpus();
        if(currentRun.getAdditionalInputPath() != null) {
            System.out.print("Reading additional training corpus from " + currentRun.getAdditionalInputPath() + " ...");
            List<Gazetteer> gazList = new ArrayList<Gazetteer>();
            LoadableGazetteer trGaz = new InMemoryGazetteer();
            trGaz.load(new CorpusGazetteerReader(testCorpus));
            LoadableGazetteer otherGaz = new InMemoryGazetteer();
            otherGaz.load(new WorldReader(new File(Constants.getGazetteersDir() + File.separator + "dataen-fixed.txt.gz")));
            gazList.add(trGaz);
            gazList.add(otherGaz);
            Gazetteer multiGaz = new MultiGazetteer(gazList);
            /*trainCorpus.addSource(new ToponymAnnotator(new PlainTextSource(
                    new BufferedReader(new FileReader(currentRun.getAdditionalInputPath())), new OpenNLPSentenceDivider(), tokenizer),
                    recognizer,
                    multiGaz));*/
            trainCorpus.addSource(new ToponymAnnotator(new GigawordSource(
                    new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(new FileInputStream(currentRun.getAdditionalInputPath())))), 10, 40000),
                    recognizer,
                    multiGaz));
            trainCorpus.addSource(new TrXMLDirSource(new File(currentRun.getInputPath()), tokenizer));
            trainCorpus.setFormat(currentRun.getCorpusFormat());
            trainCorpus.load();
            System.out.println("done.");
        }

        long endTime = System.currentTimeMillis();
        float seconds = (endTime - startTime) / 1000F;
        System.out.println("\nInitialization took " + Float.toString(seconds/(float)60.0) + " minutes.");

        Resolver resolver;
        if(currentRun.getResolverType() == RESOLVER_TYPE.RANDOM) {
            System.out.print("Running RANDOM resolver...");
            resolver = new RandomResolver();
        }
        else if(currentRun.getResolverType() == RESOLVER_TYPE.WEIGHTED_MIN_DIST) {
            System.out.println("Running WEIGHTED MINIMUM DISTANCE resolver with " + currentRun.getNumIterations() + " iteration(s)...");
            resolver = new WeightedMinDistResolver(currentRun.getNumIterations());
        }
        else if(currentRun.getResolverType() == RESOLVER_TYPE.DOC_DIST) {
            System.out.println("Running DOC DIST resolver, using log file at " + currentRun.getLogFilePath() + " ...");
            resolver = new DocDistResolver(currentRun.getLogFilePath());
        }
        else if(currentRun.getResolverType() == RESOLVER_TYPE.TOPO_AS_DOC_DIST) {
            System.out.println("Running TOPO AS DOC DIST resolver...");
            resolver = new ToponymAsDocDistResolver(currentRun.getLogFilePath());
        }
        else if(currentRun.getResolverType() == RESOLVER_TYPE.LABEL_PROP) {
            System.out.print("Running LABEL PROP resolver...");
            resolver = new LabelPropResolver(currentRun.getLogFilePath(), currentRun.getKnnForLP());
        }
        else if(currentRun.getResolverType() == RESOLVER_TYPE.LABEL_PROP_DEFAULT_RULE) {
            System.out.print("Running LABEL PROP DEFAULT RULE resolver, using graph at " + currentRun.getGraphInputPath() + " ...");
            resolver = new LabelPropDefaultRuleResolver(currentRun.getGraphInputPath());
        }
        else if(currentRun.getResolverType() == RESOLVER_TYPE.LABEL_PROP_CONTEXT_SENSITIVE) {
            System.out.print("Running LABEL PROP CONTEXT SENSITIVE resolver, using graph at " + currentRun.getGraphInputPath() + " ...");
            resolver = new LabelPropContextSensitiveResolver(currentRun.getGraphInputPath());
        }
        else if(currentRun.getResolverType() == RESOLVER_TYPE.LABEL_PROP_COMPLEX) {
            System.out.print("Running LABEL PROP COMPLEX resolver, using graph at " + currentRun.getGraphInputPath() + " ...");
            resolver = new LabelPropComplexResolver(currentRun.getGraphInputPath());
        }
        else if(currentRun.getResolverType() == RESOLVER_TYPE.MAXENT) {
            System.out.print("Running MAXENT resolver, using models at " + currentRun.getMaxentModelDirInputPath() + " and log file at " + currentRun.getLogFilePath());
            resolver = new MaxentResolver(currentRun.getLogFilePath(), currentRun.getMaxentModelDirInputPath());
        }
        else if(currentRun.getResolverType() == RESOLVER_TYPE.PROB) {
            System.out.println("Running PROBABILISTIC resolver, using models at " + currentRun.getMaxentModelDirInputPath() + " and log file at " + currentRun.getLogFilePath());

            resolver = new ProbabilisticResolver(currentRun.getLogFilePath(), currentRun.getMaxentModelDirInputPath());
        }
        else {//if(getResolverType() == RESOLVER_TYPE.BASIC_MIN_DIST) {
            System.out.print("Running BASIC MINIMUM DISTANCE resolver...");
            resolver = new BasicMinDistResolver();
        }

        if(currentRun.getAdditionalInputPath() != null)
            resolver.train(trainCorpus);
        StoredCorpus disambiguated = resolver.disambiguate(testCorpus);
        disambiguated.setFormat(currentRun.getCorpusFormat());
        if(currentRun.getCorpusFormat() == CORPUS_FORMAT.GEOTEXT) {
            if(currentRun.getBoundingBox() != null)
                System.out.println("\nOnly disambiguating documents within bounding box: " + currentRun.getBoundingBox().toString());
            SimpleDocumentResolver dresolver = new SimpleDocumentResolver();
            disambiguated = dresolver.disambiguate(disambiguated, currentRun.getBoundingBox());
        }

        System.out.println("done.\n");

        if(goldCorpus != null || currentRun.getCorpusFormat() == CORPUS_FORMAT.GEOTEXT) {
            EvaluateCorpus evaluateCorpus = new EvaluateCorpus();
            evaluateCorpus.doEval(disambiguated, goldCorpus, currentRun.getCorpusFormat(), true);
        }

        if(currentRun.getSerializedCorpusOutputPath() != null) {
            ImportCorpus importCorpus = new ImportCorpus();
            importCorpus.serialize(disambiguated, currentRun.getSerializedCorpusOutputPath());
        }

        if(currentRun.getOutputPath() != null) {
            System.out.print("Writing resolved corpus in XML format to " + currentRun.getOutputPath() + " ...");
            CorpusXMLWriter w = new CorpusXMLWriter(disambiguated);
            w.write(new File(currentRun.getOutputPath()));
            System.out.println("done.");
        }

        if(currentRun.getKMLOutputPath() != null) {
            WriteCorpusToKML writeCorpusToKML = new WriteCorpusToKML();
            writeCorpusToKML.writeToKML(disambiguated, currentRun.getKMLOutputPath(), currentRun.getOutputGoldLocations(), currentRun.getOutputUserKML(), currentRun.getCorpusFormat());
        }

        endTime = System.currentTimeMillis();
        seconds = (endTime - startTime) / 1000F;
        System.out.println("\nTotal time elapsed: " + Float.toString(seconds/(float)60.0) + " minutes.");
    }

}
