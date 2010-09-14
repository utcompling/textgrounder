/*
 * GeoNames Gazetteer; works much like WGGazetteer (World Gazetteer).
 */

package opennlp.textgrounder.gazetteers.old;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.*;

import java.sql.*;
import org.sqlite.JDBC;
import org.sqlite.*;

import gnu.trove.*;
import opennlp.textgrounder.geo.CommandLineOptions;

import opennlp.textgrounder.topostructs.*;
import opennlp.textgrounder.util.*;

public class GNGazetteer extends Gazetteer{

    public GNGazetteer(String location) throws FileNotFoundException,
          IOException, ClassNotFoundException, SQLException {
        super(location);
        initialize(Constants.TEXTGROUNDER_DATA + "/gazetteer/allCountries.txt.gz");
    }

    /**
     * Do all the work of the creation method -- read the file and populate the
     * database.
     */
    @Override
    protected void initialize(String location) throws FileNotFoundException,
          IOException, ClassNotFoundException, SQLException {

        BufferedReader gazIn = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(location))));

        System.out.println("Populating GeoNames gazetteer from " + location + " ...");

        Class.forName("org.sqlite.JDBC");

        File gntmp = File.createTempFile("gngaz", ".db");
        gntmp.deleteOnExit();
        System.out.println("jdbc:sqlite:" + gntmp.getAbsolutePath());
        conn = DriverManager.getConnection("jdbc:sqlite:" + gntmp.getAbsolutePath());

        stat = conn.createStatement();
        stat.executeUpdate("drop table if exists places;");
        stat.executeUpdate("create table places (id integer primary key, name, type, lat float, lon float, pop int, container);");
        PreparedStatement prep = conn.prepareStatement("insert into places values (?, ?, ?, ?, ?, ?, ?);");
        int placeId = 1;

        String curLine;
        String[] tokens;

        int lines = 0;
        while (true) {
            curLine = gazIn.readLine();
            if (curLine == null) {
                break;
            }
            lines++;
            if (lines % 1000000 == 0) {
                System.out.println("  Read " + lines + " lines...");// gazetteer has " + populations.size() + " entries so far.");
            }
            tokens = curLine.split("\t");

            String placeName = tokens[1].toLowerCase();

            String placeType = tokens[6].toLowerCase();

            if(!placeType.equals("a") && !placeType.equals("p")) // only read in regions/countries/etc ("a") and cities/villages/etc ("p")
                continue;

            double lat = Double.parseDouble(tokens[4]);
            double lon = Double.parseDouble(tokens[5]);

            int population;
            if (!tokens[14].equals("")) {
                population = Integer.parseInt(tokens[14]);
            } else {
		population = 0;
            }

            if(lines % 1000000 == 0) {
                System.out.println("    Last added: " + placeName + ", " + placeType + ", (" + lat + ", " + lon + "), " + population);
            }

            prep.setInt(1, placeId);
            prep.setString(2, placeName);
            prep.setString(3, placeType);
            prep.setDouble(4, lat);
            prep.setDouble(5, lon);
            prep.setInt(6, population);
            prep.setString(7, "");
            prep.addBatch();
            placeId++;

            int topidx = toponymLexicon.addWord(placeName);
	    put(topidx, null);
            
        }

        System.out.println("Executing batch insert...");
        conn.setAutoCommit(false);
        prep.executeBatch();
        conn.setAutoCommit(true);

        gazIn.close();

        System.out.println("Done. Number of entries in database = " + (placeId - 1));

        //System.exit(0);
    }
}
