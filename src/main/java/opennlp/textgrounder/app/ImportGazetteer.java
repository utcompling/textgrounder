/*
 * This class imports a gazetteer from a text file and serializes it, to be read quickly by RunResolver quickly.
 */

package opennlp.textgrounder.app;

import org.apache.commons.cli.*;
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

public class ImportGazetteer {

    private static Options options = new Options();

    private static String gazInputPath = null;
    private static String serializedGazOutputPath = null;
    private static boolean runKMeans = false;

    public static void main(String[] args) throws Exception {
        initializeOptionsFromCommandLine(args);
        doImport(gazInputPath, serializedGazOutputPath, runKMeans);
    }

    public static void doImport(String gazInputPath, String serializedGazOutputPath, boolean runKMeans) throws Exception {

        long startTime = System.currentTimeMillis();
        
        if(gazInputPath == null) {
            System.out.println("Must specify gazetteer input path.");
            System.exit(0);
        }
        if(serializedGazOutputPath == null) {
            serializedGazOutputPath = gazInputPath + ".ser";
        }

        System.out.println("Reading GeoNames gazetteer from " + gazInputPath + " ...");
        GeoNamesGazetteer gnGaz = null;
        if(gazInputPath.toLowerCase().endsWith(".zip")) {
            ZipFile zf = new ZipFile(gazInputPath);
            ZipInputStream zis = new ZipInputStream(new FileInputStream(gazInputPath));
            ZipEntry ze = zis.getNextEntry();
            gnGaz = new GeoNamesGazetteer(new BufferedReader(new InputStreamReader(zf.getInputStream(ze))), runKMeans);
            zis.close();
        }
        else {
            gnGaz = new GeoNamesGazetteer(new BufferedReader(new FileReader(gazInputPath)), runKMeans);
        }
        System.out.println("Done.");

        System.out.println("Serializing GeoNames gazetteer to " + serializedGazOutputPath + " ...");

        ObjectOutputStream oos = null;
        if(serializedGazOutputPath.toLowerCase().endsWith(".gz")) {
            GZIPOutputStream gos = new GZIPOutputStream(new FileOutputStream(serializedGazOutputPath));
            oos = new ObjectOutputStream(gos);
        }
        else {
            FileOutputStream fos = new FileOutputStream(serializedGazOutputPath);
            oos = new ObjectOutputStream(fos);
        }
        oos.writeObject(gnGaz);
        oos.close();

        long endTime = System.currentTimeMillis();
        float seconds = (endTime - startTime) / 1000F;
        System.out.println("Done. Time elapsed: " + Float.toString(seconds/(float)60.0) + " minutes.");
    }

    private static void initializeOptionsFromCommandLine(String[] args) throws Exception {
        
        options.addOption("i", "input", true, "gazetteer input path");
        options.addOption("o", "output", true, "output path for serialized gazetteer");
        options.addOption("dkm", "do-k-means-multipoints", false,
                "run k-means and create multipoint representations of regions (e.g. countries)");

        options.addOption("h", "help", false, "print help");

        CommandLineParser optparse = new PosixParser();
        CommandLine cline = optparse.parse(options, args);

        if (cline.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("textgrounder import-gazetteer", options);
            System.exit(0);
        }
        
        for (Option option : cline.getOptions()) {
            String value = option.getValue();
            switch (option.getOpt().charAt(0)) {
                case 'i':
                    gazInputPath = value;
                    break;
                case 'o':
                    serializedGazOutputPath = value;
                    break;
                case 'd':
                    runKMeans = true;
                    break;
            }
        }

    }
}
