package opennlp.textgrounder.geo;

import opennlp.textgrounder.util.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.*;

import gnu.trove.*;

public class WGGazetteer extends Gazetteer {

    public WGGazetteer () throws FileNotFoundException, IOException {
	this(Constants.TEXTGROUNDER_DATA+"/gazetteer/dataen.txt.gz");
    }

    public WGGazetteer (String location) throws FileNotFoundException, IOException {
	
	BufferedReader gazIn = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(location))));
	
	System.out.print("Populating World Gazetteer gazetteer from " + location + " ...");

	String curLine;
	String[] tokens;

	TObjectIntHashMap<String> populations = new TObjectIntHashMap<String>();
	TObjectIntHashMap<String> nonPointPopulations = new TObjectIntHashMap<String>();

	curLine = gazIn.readLine(); // first line of gazetteer is legend
	int lines = 1;
	while(true) {
	    curLine = gazIn.readLine();
	    if(curLine == null) break;
	    lines++;
	    if(lines % 1000000 == 0)
		System.out.println("  Read " + lines + " lines; gazetteer has " + populations.size() + " entries so far.");
	    //System.out.println(curLine);
	    tokens = curLine.split("\t");

	    if(tokens.length < 8) {
		//System.out.println("\nNot enough columns found; this file format should have at least " + 8 + " but only " + tokens.length + " were found. Quitting.");
		//System.exit(0);

		if(tokens.length >= 6 && /*!tokens[4].equals("locality")*/allDigits.matcher(tokens[5]).matches()) {
		    nonPointPopulations.put(tokens[1].toLowerCase(), Integer.parseInt(tokens[5]));
		    //System.out.println("Found country " + tokens[1].toLowerCase() + ": " + tokens[5]);
		}

		continue;
	    }

	    if(!tokens[4].equals("locality") && allDigits.matcher(tokens[5]).matches()) {
		nonPointPopulations.put(tokens[1].toLowerCase(), Integer.parseInt(tokens[5]));
		//System.out.println("Found country " + tokens[1].toLowerCase() + ": " + tokens[5]);
	    }

	    String placeName = tokens[1].toLowerCase();

	    String rawLat = tokens[6].trim();
	    String rawLon = tokens[7].trim();

	    if(rawLat.equals("") || rawLon.equals("")
	    //if(rawLat.length() < 2 || rawLon.length() < 2
	       || !rawCoord.matcher(rawLat).matches() || !rawCoord.matcher(rawLon).matches())
		continue;

	    //System.out.println(placeName);
	    double lat = convertRawToDec(rawLat);
	    double lon = convertRawToDec(rawLon);

	    Coordinate curCoord = new Coordinate(lat, lon);

	    int population;
	    //if(allDigits.matcher(tokens[5]).matches()) {
	    if(!tokens[5].equals("")) {
		population = Integer.parseInt(tokens[5]);
		//if(placeName.contains("venice"))
		//   System.out.println(placeName + ": " + population);
	    }
	    else
		continue; // no population was listed, so skip this entry

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
		/*if(placeName.contains("venice")) {
		    System.out.println("putting bigger " + placeName + ": " + population);
		    System.out.println("  coordinates: " + curCoord);
		    }*/
		//System.out.println("Found a bigger " + placeName + " with population " + population + "; was " + storedPop);
	    }

	}
	
	gazIn.close();

	System.out.println("done. Total number of actual place names = " + size());

	System.out.print("Removing place names with smaller populations than non-point places of the same name...");
	Object[] countrySet = nonPointPopulations.keys();
	for(int i = 0; i < countrySet.length; i++) {
	    String curCountry = (String)countrySet[i];
	    if(populations.containsKey(curCountry) && populations.get(curCountry) < nonPointPopulations.get(curCountry)) {
		remove(curCountry);
		//System.out.println("removed " + curCountry);
	    }
	}
	System.out.println("done.");

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
}