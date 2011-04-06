/*
 * This class takes a corpus with system resolved toponyms and generates a KML file visualizable in Google Earth.
 */

package opennlp.textgrounder.app;

import opennlp.textgrounder.text.*;
import opennlp.textgrounder.text.io.*;
import opennlp.textgrounder.util.*;
import java.io.*;

public class WriteCorpusToKML extends BaseApp {
    public static void main(String[] args) throws Exception {
        initializeOptionsFromCommandLine(args);

        if(getSerializedCorpusInputPath() == null) {
            System.out.println("Please specify an input corpus in serialized format via the -sci flag.");
            System.exit(0);
        }

        if(getKMLOutputPath() == null) {
            System.out.println("Please specify a KML output path via the -ok flag.");
            System.exit(0);
        }

        System.out.print("Reading serialized corpus from " + getSerializedCorpusInputPath() + " ...");
        Corpus corpus = TopoUtil.readCorpusFromSerialized(getSerializedCorpusInputPath());
        System.out.println("done.");

        writeToKML(corpus, getKMLOutputPath());
    }

    public static void writeToKML(Corpus corpus, String kmlOutputPath) throws Exception {
        System.out.print("Writing visualizable corpus in KML format to " + kmlOutputPath + " ...");
        CorpusKMLWriter kw = new CorpusKMLWriter(corpus);
        kw.write(new File(kmlOutputPath));
        System.out.println("done.");
    }
}
