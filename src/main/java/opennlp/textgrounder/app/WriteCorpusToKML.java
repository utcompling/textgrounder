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

        writeToKML(corpus, getKMLOutputPath(), getOutputGoldLocations(), getOutputUserKML(), getCorpusFormat());
    }

    public static void writeToKML(Corpus corpus, String kmlOutputPath, boolean outputGoldLocations, boolean outputUserKML, Enum<CORPUS_FORMAT> corpusFormat) throws Exception {
        System.out.print("Writing visualizable corpus in KML format to " + kmlOutputPath + " ...");
        CorpusKMLWriter kw;
        if(corpusFormat == CORPUS_FORMAT.GEOTEXT && outputUserKML)
            kw = new GeoTextCorpusKMLWriter(corpus, outputGoldLocations);
        else
            kw = new CorpusKMLWriter(corpus, outputGoldLocations);
        kw.write(new File(kmlOutputPath));
        System.out.println("done.");
    }
}
