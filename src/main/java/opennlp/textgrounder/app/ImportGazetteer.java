/*
 * This class imports a gazetteer from a text file and serializes it, to be read quickly by RunResolver quickly.
 */

package opennlp.textgrounder.app;

import opennlp.textgrounder.topo.gaz.*;
import opennlp.textgrounder.util.*;
import java.io.*;
import java.util.zip.*;

public class ImportGazetteer extends BaseApp {

    public static void main(String[] args) throws Exception {
        initializeOptionsFromCommandLine(args);
        doImport(getInputPath(), getOutputPath(), isDoingKMeans());
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

        //long startMemoryUse = MemoryUtil.getMemoryUsage();
        
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

        //long endMemoryUse = MemoryUtil.getMemoryUsage();

        //System.out.println(( endMemoryUse - startMemoryUse ));

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
}
