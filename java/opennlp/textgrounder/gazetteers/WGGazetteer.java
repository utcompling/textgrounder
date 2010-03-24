package opennlp.textgrounder.gazetteers;

import opennlp.textgrounder.util.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.*;

import java.sql.*;
import org.sqlite.JDBC;
import org.sqlite.*;

import gnu.trove.*;
import opennlp.textgrounder.topostructs.Coordinate;
import opennlp.textgrounder.topostructs.Location;

public class WGGazetteer extends Gazetteer {

    private Connection conn;
    private Statement stat;

    public WGGazetteer () throws FileNotFoundException, IOException, Exception {
	this(Constants.TEXTGROUNDER_DATA+"/gazetteer/dataen-fixed.txt.gz");
    }

    public WGGazetteer (String location) throws FileNotFoundException, IOException, Exception {
	
	BufferedReader gazIn = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(location))));
	
	System.out.println("Populating World Gazetteer gazetteer from " + location + " ...");

	Class.forName("org.sqlite.JDBC");
	//System.out.println("after class");
	//System.out.println(DriverManager.getLoginTimeout());
	//System.out.println(DriverManager.getDrivers());
	System.out.println("jdbc:sqlite:"+Constants.WGDB_PATH);
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

	    /*if(tokens.length < 8) {
		//System.out.println("\nNot enough columns found; this file format should have at least " + 8 + " but only " + tokens.length + " were found. Quitting.");
		//System.exit(0);

		if(tokens.length >= 6 && /+!tokens[4].equals("locality")+/allDigits.matcher(tokens[5]).matches()) {
		    nonPointPopulations.put(tokens[1].toLowerCase(), Integer.parseInt(tokens[5]));
		    //System.out.println("Found country " + tokens[1].toLowerCase() + ": " + tokens[5]);
		}

		continue;
	    }*/

	    if(tokens.length < 6) {
		if(tokens.length >= 2) {
		    System.out.println("Skipping " + tokens[1]);
		    System.out.println(curLine);
		}
		continue;
	    }


	    /*if(!tokens[4].equals("locality") && allDigits.matcher(tokens[5]).matches()) {
		nonPointPopulations.put(tokens[1].toLowerCase(), Integer.parseInt(tokens[5]));
		//System.out.println("Found country " + tokens[1].toLowerCase() + ": " + tokens[5]);
		}*/

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

	    /*int storedPop = populations.get(placeName);
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
	    }*/

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
	
	gazIn.close();

	System.out.println("Done. Number of entries in database = " + (placeId - 1));

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

    private static double convertRawToDec(String raw) {
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
    }

    public Coordinate baselineGet(String placename) throws Exception {
	ResultSet rs = stat.executeQuery("select pop from places where type != \"locality\" and name = \""
					 + placename + "\" order by pop desc;");
	int nonLocalityPop = 0;
	if(rs.next())
	    nonLocalityPop = rs.getInt("pop");
	rs.close();

	rs = stat.executeQuery("select * from places where type = \"locality\" and name = \""
					 + placename + "\" order by pop desc;");
	/*while*/if(rs.next()) {
	    //System.out.println(rs.getString("id") + ": " + rs.getString("name") + ": " + rs.getString("type") + ": " + rs.getString("lat") + ": " + rs.getString("lon") + ": " + rs.getString("pop"));
	    if(rs.getInt("pop") > nonLocalityPop || rs.getString("container").equals(rs.getString("name"))) {
		Coordinate returnCoord = new Coordinate(rs.getDouble("lon"), rs.getDouble("lat"));
		rs.close();
		return returnCoord;
	    }
	}
	rs.close();
	return new Coordinate(9999.99, 9999.99);
    }

    public boolean hasPlace(String placename) throws Exception {
	ResultSet rs = stat.executeQuery("select * from places where type = \"locality\" and name = \"" + placename + "\";");
	if(rs.next()) {
	    rs.close();
	    return true;
	}
	else {
	    rs.close();
	    return false;
	}
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
