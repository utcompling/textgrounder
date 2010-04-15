package opennlp.textgrounder.gazetteers;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.*;

import java.sql.*;
import org.sqlite.JDBC;
import org.sqlite.*;

import gnu.trove.*;

import opennlp.textgrounder.topostructs.*;
import opennlp.textgrounder.util.*;

public class TRGazetteer extends Gazetteer {

    private Connection conn;
    private Statement stat;

    private static Coordinate nullCoord = new Coordinate(0.0,0.0);

    public TRGazetteer () throws FileNotFoundException, IOException, Exception {
	this("/tmp/toponym.db"/*Constants.TRDB_PATH*/);
    }

    public TRGazetteer (String location) throws FileNotFoundException, IOException, Exception {

	System.out.println("Populating TR-Gazetteer from " + location + " ...");

	Class.forName("org.sqlite.JDBC");

	conn = DriverManager.getConnection("jdbc:sqlite:"+location);

	stat = conn.createStatement();

	stat.executeUpdate("drop table if exists places;");
	stat.executeUpdate("create table places (id integer primary key, name, type, lat float, lon float, pop int, container);");
	PreparedStatement prep = conn.prepareStatement("insert into places values (?, ?, ?, ?, ?, ?, ?);");

	int placeId = 1;
	int ignoreCount = 0;

	ResultSet rs;

	
	
	// get places from CIA centroids section:
	System.out.println(" Reading CIA centroids section...");
	rs = stat.executeQuery("select * from T_CIA_CENTROIDS");
	while(rs.next()) {
	    prep.setInt(1, placeId);
	    String nameToInsert = rs.getString("COUNTRY").toLowerCase();
	    if(nameToInsert != null)
		prep.setString(2, nameToInsert);
	    prep.setString(3, "cia_centroid");
	    double latToInsert = DMDtoDD(rs.getInt("LONG_DEG"), rs.getInt("LONG_MIN"), rs.getString("LONG_DIR"));/////
	    prep.setDouble(4, latToInsert);
	    double longToInsert = DMDtoDD(rs.getInt("LAT_DEG"), rs.getInt("LAT_MIN"), rs.getString("LAT_DIR"));///////
	    prep.setDouble(5, longToInsert);

	    if(latToInsert < -180.0 || latToInsert > 180.0
	       || longToInsert < -90.0 || longToInsert > 90.0) {
		ignoreCount++;
		continue;
	    }

	    prep.addBatch();

	    if(placeId % 100 == 0) {
	    	System.out.println("  Added " + placeId + " places...");// gazetteer has " + populations.size() + " entries so far.");
		System.out.println("    Last added: " + nameToInsert + ", cia_centroid, (" + latToInsert + ", " + longToInsert + ")");
	    }

	    placeId++;

	    put(nameToInsert, nullCoord);
	}

	

	

	// get places from USGS section:
	System.out.println(" Reading USGS section...");
	rs = stat.executeQuery("select * from T_USGS_PP");
	while(rs.next()) {
	    String rawType = rs.getString("FeatureType");
	    String typeToInsert = "";
	    if(rawType != null) {
		if(rawType.equals("ppl"))
		    typeToInsert = "locality";
		else {
		    continue; // skipping non-localities for now
		    //typeToInsert = "NON-locality";
		}
		prep.setString(3, typeToInsert);
	    }
	    prep.setInt(1, placeId);
	    String nameToInsert = rs.getString("FeatureName").toLowerCase();
	    if(nameToInsert != null)
		prep.setString(2, nameToInsert);
	    double latToInsert = rs.getDouble("PrimaryLongitudeDD");///////
	    prep.setDouble(4, latToInsert);
	    double longToInsert = rs.getDouble("PrimaryLatitudeDD");//////
	    prep.setDouble(5, longToInsert);

	    if(latToInsert < -180.0 || latToInsert > 180.0
	       || longToInsert < -90.0 || longToInsert > 90.0) {
		ignoreCount++;
		continue;
	    }

	    int popToInsert = rs.getInt("EstimatedPopulation");
	    if(popToInsert != 0) {
		prep.setDouble(6, popToInsert);
		//System.out.println(popToInsert);
	    }
	    
	    prep.addBatch();

	    /*if(nameToInsert.equals("texas") || nameToInsert.equals("california"))
	      System.out.println(nameToInsert + ", " + typeToInsert + ", (" + latToInsert + ", " + longToInsert + "), " + popToInsert);*/

	    if(placeId % 50000 == 0) {
		System.out.println("  Added " + placeId + " places...");// gazetteer has " + populations.size() + " entries so far.");
		System.out.println("    Last added: " + nameToInsert + ", " + typeToInsert + ", (" + latToInsert + ", " + longToInsert + "), " + popToInsert);
	    }

	    placeId++;

	    put(nameToInsert, nullCoord);
	}

	
	
	

	// get places from NGA section:
	System.out.println(" Reading NGA section...");
	rs = stat.executeQuery("select * from T_NGA");
	while(rs.next()) {
	    String rawType = rs.getString("FC");
	    String typeToInsert = "";
	    if(rawType != null) {
		if(rawType.equals("P"))
		    typeToInsert = "locality";
		else {
		    continue; // skipping non-localities for now
		    //typeToInsert = "NON-locality";
		}
		prep.setString(3, typeToInsert);
	    }
	    prep.setInt(1, placeId);
	    String nameToInsert = rs.getString("FULL_NAME_ND").toLowerCase();
	    if(nameToInsert != null)
		prep.setString(2, nameToInsert);
	    
	    double latToInsert = rs.getDouble("DD_LONG");//"DD_LAT"); //switched?
	    //if(latToInsert != null)
	    prep.setDouble(4, latToInsert);
	    double longToInsert = rs.getDouble("DD_LAT");//"DD_LONG");
	    //if(longToInsert != null)
	    prep.setDouble(5, longToInsert);

	    if(latToInsert < -180.0 || latToInsert > 180.0
	       || longToInsert < -90.0 || longToInsert > 90.0) {
		ignoreCount++;
		continue;
	    }

	    int popToInsert = rs.getInt("DIM"); // note: DIM holds population for type P and elevation for all others
	    if(popToInsert != 0 && rawType != null && rawType.equals("P")) {
		prep.setInt(6, popToInsert);
		//System.out.println(popToInsert);
	    }
	    // could use ADM2 for container, but those are counties, not countries, so probably a bad idea. leaving container field blank for now
	    prep.addBatch();


	    /*if(nameToInsert.equals("texas") || nameToInsert.equals("california"))
	      System.out.println(nameToInsert + ", " + typeToInsert + ", (" + latToInsert + ", " + longToInsert + "), " + popToInsert);*/

	    if(placeId % 100000 == 0) {
	    	System.out.println("  Added " + placeId + " places...");// gazetteer has " + populations.size() + " entries so far.");
		System.out.println("    Last added: " + nameToInsert + ", " + typeToInsert + ", (" + latToInsert + ", " + longToInsert + "), " + popToInsert);
	    }
	    //if(placeId == 1000)//////////////////
	    //  System.exit(0);//////////////////
	    
	    placeId++;

	    put(nameToInsert, nullCoord);
	}

	

	if(ignoreCount > 0)
	    System.out.println(ignoreCount + " (" + ((ignoreCount * 100) / (ignoreCount + placeId)) + "%) entries were ignored due to invalid coordinates (out of range)");

	System.out.print("Executing batch insertion into database...");
	conn.setAutoCommit(false);
	prep.executeBatch();
	conn.setAutoCommit(true);
	System.out.println("done.");

	System.out.println("Number of entries in database = " + (placeId - 1));

	/*List<Location> testloc = this.get("norway");
	System.out.println(testloc.size());
	for (Location loc : testloc)
	    System.out.println(loc);
	testloc = this.get("austin");
	System.out.println(testloc.size());
	for (Location loc : testloc)
	    System.out.println(loc);
	    System.exit(0);*/

	//ResultSet rs = stat.executeQuery("show tables;");
	//ResultSet rs = stat.executeQuery("select * from sqlite_master where type='table';");
	
	//while(rs.next()) {
	//    System.out.println(rs.toString()/*rs.getString("id") + ": " + rs.getString("name") + ": " + rs.getString("type") + ": " + rs.getString("lat") + ": " + rs.getString("lon") + ": " + rs.getString("pop")*/);
	//    }

	//rs.close();

	//conn.close();
	
	/*BufferedReader gazIn = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(location))));
	
	System.out.println("Populating World Gazetteer gazetteer from " + location + " ...");

	Class.forName("org.sqlite.JDBC");
	//System.out.println("after class");
	//System.out.println(DriverManager.getLoginTimeout());
	//System.out.println(DriverManager.getDrivers());
	conn = DriverManager.getConnection("jdbc:sqlite:"+Constants.WGDB_PATH);
	//System.out.println("after conn");

	stat = conn.createStatement();
	stat.executeUpdate("drop table if exists places;");
	stat.executeUpdate("create table places (id integer primary key, name, type, lat float, lon float, pop int, container);");
	PreparedStatement prep = conn.prepareStatement("insert into places values (?, ?, ?, ?, ?, ?, ?);");
	int placeId = 1;

	String curLine;
	String[] tokens;

	TObjectIntHashMap<String> populations = new TObjectIntHashMap<String>();
	TObjectIntHashMap<String> nonPointPopulations = new TObjectIntHashMap<String>();

	//	curLine = gazIn.readLine(); // first line of gazetteer is legend (but only for other gaz types)
	int lines = 0;
	while(true) {
	    curLine = gazIn.readLine();
	    if(curLine == null) break;
	    lines++;
	    if(lines % 100000 == 0)
	    	System.out.println("  Read " + lines + " lines...");// gazetteer has " + populations.size() + " entries so far.");
	    //System.out.println(curLine);
	    tokens = curLine.split("\t");

	    if(tokens.length < 8) {
		//System.out.println("\nNot enough columns found; this file format should have at least " + 8 + " but only " + tokens.length + " were found. Quitting.");
		//System.exit(0);

		if(tokens.length >= 6 && /+!tokens[4].equals("locality")+/allDigits.matcher(tokens[5]).matches()) {
		    nonPointPopulations.put(tokens[1].toLowerCase(), Integer.parseInt(tokens[5]));
		    //System.out.println("Found country " + tokens[1].toLowerCase() + ": " + tokens[5]);
		}

		continue;
	    }

	    if(tokens.length < 6) {
		if(tokens.length >= 2) {
		    System.out.println("Skipping " + tokens[1]);
		    System.out.println(curLine);
		}
		continue;
	    }


	    if(!tokens[4].equals("locality") && allDigits.matcher(tokens[5]).matches()) {
		nonPointPopulations.put(tokens[1].toLowerCase(), Integer.parseInt(tokens[5]));
		//System.out.println("Found country " + tokens[1].toLowerCase() + ": " + tokens[5]);
		}

	    String placeName = tokens[1].toLowerCase();

	    String placeType = tokens[4].toLowerCase();

	    String rawLat, rawLon;
	    if(tokens.length >= 8) {
		rawLat = tokens[7].trim();
		rawLon = tokens[6].trim();
	    }
	    else {
		rawLat = "999999"; // sentinel values for entries with no coordinates
		rawLon = "999999";
	    }

	    if(rawLat.equals("") || rawLon.equals("")
	    //if(rawLat.length() < 2 || rawLon.length() < 2
	       || !rawCoord.matcher(rawLat).matches() || !rawCoord.matcher(rawLon).matches()) {
		//System.out.println("bad lat/lon");
		//System.out.println(curLine);
		//continue;
		rawLat = "999999";
		rawLon = "999999";
	    }

	    //System.out.println(placeName);
	    double lat = convertRawToDec(rawLat);
	    double lon = convertRawToDec(rawLon);

	    Coordinate curCoord = new Coordinate(lon, lat);

	    String container = null;
	    if(tokens.length >= 11 && !tokens[10].trim().equals(""))
		container = tokens[10].trim().toLowerCase();

	    int population;
	    //if(allDigits.matcher(tokens[5]).matches()) {
	    if(!tokens[5].equals("")) {
		population = Integer.parseInt(tokens[5]);
		//if(placeName.contains("venice"))
		//   System.out.println(placeName + ": " + population);
	    }
	    else {
		//population = -1 // sentinal value in database for unlisted population
		System.out.println("bad pop");
		System.out.println(curLine);
		continue; // no population was listed, so skip this entry
	    }



	    //stat.executeUpdate("insert into places values ("
	    //+ placeId + ", \"" + placeName + "\", " + lat + ", " + lon + ", " + population + ")");
	    prep.setInt(1, placeId);
	    prep.setString(2, placeName);
	    prep.setString(3, placeType);
	    if(lat != 9999.99) {
		prep.setDouble(4, lat);
		prep.setDouble(5, lon);
	    }
	    prep.setInt(6, population);
	    if(container != null)
		prep.setString(7, container);
	    prep.addBatch();
	    placeId++;
	    //if(placeId % 10000 == 0)
	    //	System.out.println("Added " + placeId + " places to the batch...");


	    // old disambiguation method; as of now still used just to keep a hashtable of places for quick lookup of whether the database has at least 1 mention of the place:
	    if(placeType.equals("locality"))
		put(placeName, new Coordinate(0.0, 0.0));

	    int storedPop = populations.get(placeName);
	    if(storedPop == 0) { // 0 is not-found sentinal for TObjectIntHashMap
		populations.put(placeName, population);
		put(placeName, curCoord);
		//if(placeName.contains("venice"))
		//   System.out.println("putting " + placeName + ": " + population);
	    }
	    else if(population > storedPop) {
		populations.put(placeName, population);
		put(placeName, curCoord);
		//if(placeName.contains("venice")) {
		//    System.out.println("putting bigger " + placeName + ": " + population);
		//    System.out.println("  coordinates: " + curCoord);
		//    }
		//System.out.println("Found a bigger " + placeName + " with population " + population + "; was " + storedPop);
	    }

	}

	//System.out.println("Executing batch...");
	conn.setAutoCommit(false);
	prep.executeBatch();
	conn.setAutoCommit(true);

	/*ResultSet rs = stat.executeQuery("select * from places where pop > 10000000;");
	while(rs.next()) {
	    System.out.println(rs.getString("id") + ": " + rs.getString("name") + ": " + rs.getString("type") + ": " + rs.getString("lat") + ": " + rs.getString("lon") + ": " + rs.getString("pop"));
	    }*/

	/*ResultSet rs = stat.executeQuery("select * from places where name = \"san francisco\";");
	while(rs.next()) {
	    System.out.println(rs.getString("id") + ": " + rs.getString("name") + ": " + rs.getString("type") + ": " + rs.getString("lat") + ": " + rs.getString("lon") + ": " + rs.getString("pop") + ": " + rs.getString("container"));
	    }*/

	/*rs = stat.executeQuery("select * from places where name = \"london\";");
	while(rs.next()) {
	    System.out.println(rs.getString("id") + ": " + rs.getString("name") + ": " + rs.getString("type") + ": " + rs.getString("lat") + ": " + rs.getString("lon") + ": " + rs.getString("pop"));
	    }*/

	//rs.close();

	//conn.close();
	
	/*gazIn.close();

	  System.out.println("Done. Number of entries in database = " + (placeId - 1));*/

	//System.exit(0);

	/*System.out.print("Removing place names with smaller populations than non-point places of the same name...");
	Object[] countrySet = nonPointPopulations.keys();
	for(int i = 0; i < countrySet.length; i++) {
	    String curCountry = (String)countrySet[i];
	    if(populations.containsKey(curCountry) && populations.get(curCountry) < nonPointPopulations.get(curCountry)) {
		remove(curCountry);
		//System.out.println("removed " + curCountry);
	    }
	}
	System.out.println("done.");*/

    }

    /*    private static double convertRawToDec(String raw) {
	//System.out.println(raw);
	if(raw.length() <= 1) {
	    return Double.parseDouble(raw);
	}
	if(raw.length() <= 2) {
	    if(raw.startsWith("-"))
		return Double.parseDouble("-.0" + raw.charAt(1));
	}
	int willBeDecimalIndex = raw.length() - 2;
	return Double.parseDouble(raw.substring(0, willBeDecimalIndex) + "." + raw.substring(willBeDecimalIndex));
	}*/

    /*public Coordinate baselineGet(String placename) throws Exception {
	ResultSet rs = stat.executeQuery("select pop from places where type != \"locality\" and name = \""
					 + placename + "\" order by pop desc;");
	int nonLocalityPop = 0;
	if(rs.next())
	    nonLocalityPop = rs.getInt("pop");
	rs.close();

	rs = stat.executeQuery("select * from places where type = \"locality\" and name = \""
					 + placename + "\" order by pop desc;");
	whileif(rs.next()) {
	    //System.out.println(rs.getString("id") + ": " + rs.getString("name") + ": " + rs.getString("type") + ": " + rs.getString("lat") + ": " + rs.getString("lon") + ": " + rs.getString("pop"));
	    if(rs.getInt("pop") > nonLocalityPop || rs.getString("container").equals(rs.getString("name"))) {
		Coordinate returnCoord = new Coordinate(rs.getDouble("lon"), rs.getDouble("lat"));
		rs.close();
		return returnCoord;
	    }
	}
	rs.close();
	return new Coordinate(9999.99, 9999.99);
    }*/

    /*public boolean hasPlace(String placename) throws Exception {
	ResultSet rs = stat.executeQuery("select * from places where type = \"locality\" and name = \"" + placename + "\";");
	if(rs.next()) {
	    rs.close();
	    return true;
	}
	else {
	    rs.close();
	    return false;
	}
	}*/
    /**
     * Converts (half) a coordinate from degrees-minutes-direction to decimal degrees
     */
    private double DMDtoDD(int degrees, int minutes, String direction) {
	double dd = degrees;
	dd += ((double)minutes)/60.0;
	if(direction.equals("S") || direction.equals("W"))
	    dd *= -1;
	return dd;
    }

    protected void finalize() throws Throwable {
	try {
	    conn.close();
	} finally {
	    super.finalize();
	}
    }

    public List<Location> get(String placename) throws Exception {
	ArrayList<Location> locationsToReturn = new ArrayList<Location>();

	ResultSet rs = stat.executeQuery("select * from places where name = \"" + placename + "\";");
	while(rs.next()) {
	    Location locationToAdd = new Location(rs.getInt("id"),
						  rs.getString("name"),
						  rs.getString("type"),
						  new Coordinate(rs.getDouble("lon"), rs.getDouble("lat")),
						  rs.getInt("pop"),
						  rs.getString("container"),
						  0);
	    locationsToReturn.add(locationToAdd);
	}
	rs.close();
	return locationsToReturn;
    }
}
