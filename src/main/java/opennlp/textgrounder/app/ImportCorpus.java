/* This class takes a serialized gazetteer and a corpus, and outputs a preprocessed, serialized version of that corpus, ready to be read in quickly by RunResolver.
 */

package opennlp.textgrounder.app;

import opennlp.textgrounder.text.*;
import opennlp.textgrounder.text.io.*;
import opennlp.textgrounder.text.prep.*;
import opennlp.textgrounder.topo.gaz.*;
import java.io.*;
import java.util.zip.*;

public class ImportCorpus extends BaseApp {
    
    public static void main(String[] args) throws Exception {
        initializeOptionsFromCommandLine(args);

        if(getSerializedCorpusOutputPath() == null && getOutputPath() == null) {
            System.out.println("Please specify a serialized corpus output file with the -sco flag and/or an XML output file with the -o flag.");
            System.exit(0);
        }

        StoredCorpus corpus = doImport(getInputPath(), getSerializedGazetteerPath(), getCorpusFormat());
        
        if(getSerializedCorpusOutputPath() != null)
            serialize(corpus, getSerializedCorpusOutputPath());
        if(getOutputPath() != null)
            writeToXML(corpus, getOutputPath());
    }

    public static StoredCorpus doImport(String corpusInputPath, String serGazInputPath, Enum<CORPUS_FORMAT> corpusFormat) throws Exception {

        Tokenizer tokenizer = new OpenNLPTokenizer();
        OpenNLPRecognizer recognizer = new OpenNLPRecognizer();

        GeoNamesGazetteer gnGaz = null;
        System.out.println("Reading serialized GeoNames gazetteer from " + serGazInputPath + " ...");
        ObjectInputStream ois = null;
        if(serGazInputPath.toLowerCase().endsWith(".gz")) {
            GZIPInputStream gis = new GZIPInputStream(new FileInputStream(serGazInputPath));
            ois = new ObjectInputStream(gis);
        }
        else {
            FileInputStream fis = new FileInputStream(serGazInputPath);
            ois = new ObjectInputStream(fis);
        }
        gnGaz = (GeoNamesGazetteer) ois.readObject();
        System.out.println("Done.");

        System.out.print("Reading raw corpus from " + corpusInputPath + " ...");
        StoredCorpus corpus = Corpus.createStoredCorpus();
        if(corpusFormat == CORPUS_FORMAT.TRCONLL) {
            corpus.addSource(new ToponymAnnotator(
                new ToponymRemover(new TrXMLDirSource(new File(corpusInputPath), tokenizer)),
                recognizer, gnGaz, null));
        }
        else if(corpusFormat == CORPUS_FORMAT.GEOTEXT) {
            corpus.addSource(new ToponymAnnotator(new GeoTextSource(
                new BufferedReader(new FileReader(corpusInputPath)), tokenizer),
                recognizer, gnGaz, null));
        }
	else if (corpusInputPath.endsWith("txt")) {
            corpus.addSource(new ToponymAnnotator(new PlainTextSource(
		new BufferedReader(new FileReader(corpusInputPath)), new OpenNLPSentenceDivider(), tokenizer),
                recognizer, gnGaz, null));
	}
        else {
            corpus.addSource(new ToponymAnnotator(new PlainTextDirSource(
                new File(corpusInputPath), new OpenNLPSentenceDivider(), tokenizer),
                recognizer, gnGaz, null));
        }
        corpus.setFormat(corpusFormat);
        //if(corpusFormat != CORPUS_FORMAT.GEOTEXT)
        corpus.load();
        System.out.println("done.");

        System.out.println("\nNumber of documents: " + corpus.getDocumentCount());
        System.out.println("Number of toponym types: " + corpus.getToponymTypeCount());
        System.out.println("Maximum ambiguity (locations per toponym): " + corpus.getMaxToponymAmbiguity());

        return corpus;
    }

    public static void serialize(Corpus corpus, String serializedCorpusPath) throws Exception {

        System.out.print("\nSerializing corpus to " + serializedCorpusPath + " ...");
        
        ObjectOutputStream oos = null;
        if(serializedCorpusPath.toLowerCase().endsWith(".gz")) {
            GZIPOutputStream gos = new GZIPOutputStream(new FileOutputStream(serializedCorpusPath));
            oos = new ObjectOutputStream(gos);
        }
        else {
            FileOutputStream fos = new FileOutputStream(serializedCorpusPath);
            oos = new ObjectOutputStream(fos);
        }
        oos.writeObject(corpus);
        oos.close();
        
        System.out.println("done.");
    }

    public static void writeToXML(Corpus corpus, String xmlOutputPath) throws Exception {
        System.out.print("\nWriting corpus in XML format to " + xmlOutputPath + " ...");
        CorpusXMLWriter w = new CorpusXMLWriter(corpus);
        w.write(new File(xmlOutputPath));
        System.out.println("done.");
    }
}
