package opennlp.textgrounder.geo;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.*;

import gnu.trove.*;

public class Gazetteer extends THashMap<String, Coordinate> {

    protected static Pattern allDigits = Pattern.compile("^[0-9]+$");


    public CensusGazetteer () throws FileNotFoundException, IOException {
	this(Constants.TEXTGROUNDER_DATA+"/gazetteer/places2k.txt.gz")
    }

    public CensusGazetteer (String location, int gazType) throws FileNotFoundException, IOException {

	//BufferedReader gazIn = new BufferedReader(new FileReader(location));
	
	BufferedReader gazIn = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(location))));
	
	System.out.print("Populating gazetteer...");

	String curLine;
	String[] tokens;

	TObjectIntHashMap<String> populations = new TObjectIntHashMap<String>();
	    
	while(true) {
		curLine = gazIn.readLine();
		if(curLine == null) break;
		tokens = curLine.split("\\s+");
		//if(tokens.length < ...
		int popIndex = -1; // which column holds the population
		String placeName = "";
		int population = -1;
		for(int i = 0; i < tokens.length; i++) {
		    String token = tokens[i].trim();
		    if(i == 0)
			token = token.substring(9); // trims US Census ID from first word in name
		    //System.out.println(curLine);
		    //System.out.println(token);
		    if(allDigits.matcher(token).matches()) { // found population; done getting name
			popIndex = i;
			//System.out.println("popIndex is " + popIndex);

			break;
		    }
		    placeName += token + " ";
		}
		placeName = placeName.toLowerCase().trim();
		//System.out.println("original placeName: " + placeName);
		int lastSpaceIndex = placeName.lastIndexOf(" ");
		if(lastSpaceIndex != -1) // trim descriptor word like 'city' from name
		    placeName = placeName.substring(0, lastSpaceIndex).trim();
		//System.out.println("placeName: " + placeName);

		population = Integer.parseInt(tokens[popIndex]);
		//System.out.println("population: " + population);

		double latitude, longitude;
		int negIndex = tokens[popIndex+6].indexOf("-"); // handle buggy case for places in Alaska, etc
		if(negIndex != -1) {
		    latitude = Double.parseDouble(tokens[popIndex+6].substring(0, negIndex).trim());
		    longitude = Double.parseDouble(tokens[popIndex+6].substring(negIndex).trim());
		}
		else {
		    latitude = Double.parseDouble(tokens[popIndex+6]);
		    longitude = Double.parseDouble(tokens[popIndex+7]);
		}

		Coordinate curCoord =
		    new Coordinate(latitude, longitude);
		//System.out.println("coordinates: " + curCoord);

		int storedPop = populations.get(placeName);
		if(storedPop == 0) { // 0 is not-found sentinal for TObjectIntHashMap
		    populations.put(placeName, population);
		    put(placeName, curCoord);
		}
		else if(population > storedPop) {
		    populations.put(placeName, population);
		    put(placeName, curCoord);
		    //System.out.println("Found a bigger " + placeName + " with population " + population + "; was " + storedPop);
		}

		//System.out.println("-------");
	}
	
	break;

	
	gazIn.close();

	System.out.println("done. Total number of actual place names = " + size());
	       		
	/*System.out.println(placenamesToCoords.get("salt springs"));
	  System.out.println(placenamesToCoords.get("galveston"));*/

    }

}