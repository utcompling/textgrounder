/*
 * This class imports a gazetteer from a text file and serializes it, to be read quickly by RunResolver quickly.
 */

package opennlp.textgrounder.tr.app;

import opennlp.textgrounder.tr.topo.gaz.*;
import opennlp.textgrounder.tr.util.*;
import java.io.*;
import java.util.zip.*;

public class ImportGazetteer extends BaseApp {

    public static void main(String[] args) throws Exception {
        ImportGazetteer currentRun = new ImportGazetteer();
        currentRun.initializeOptionsFromCommandLine(args);
        currentRun.serialize(currentRun.doImport(currentRun.getInputPath(), currentRun.isDoingKMeans()), currentRun.getOutputPath());
    }

    public GeoNamesGazetteer doImport(String gazInputPath, boolean runKMeans) throws Exception {
        System.out.println("Reading GeoNames gazetteer from " + gazInputPath + " ...");

        checkExists(gazInputPath);
        
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

        return gnGaz;
    }

    public void serialize(GeoNamesGazetteer gnGaz, String serializedGazOutputPath) throws Exception {
        System.out.print("Serializing GeoNames gazetteer to " + serializedGazOutputPath + " ...");

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

        System.out.println("done.");
    }
}
