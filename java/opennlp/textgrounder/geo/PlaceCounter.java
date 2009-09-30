package opennlp.textgrounder.geo;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import gnu.trove.*;

public class PlaceCounter extends TObjectIntHashMap<String> {

    private static Pattern placeNamePattern = Pattern.compile("[A-Z][\\w]*([\\s][A-Z][\\w]*)*");

    public PlaceCounter(String locationOfFile, Gazetteer gaz) 
	throws FileNotFoundException, IOException {

	BufferedReader textIn = new BufferedReader(new FileReader(locationOfFile));
		
	System.out.print("Scanning raw text for place names...");
		
	//curLine = textIn.readLine();
	while(true) {
	    //System.out.println(curLine);
	    String curLine = textIn.readLine();
	    if(curLine == null) break;
	    Matcher m = placeNamePattern.matcher(curLine);
	    while(m != null && m.find()) {
		String potentialPlacename = m.group().toLowerCase();

		if(gaz.containsKey(potentialPlacename)) {
		    if (!increment(potentialPlacename))
			put(potentialPlacename,1);

		    //System.out.println(potentialPlacename + ": " + get(potentialPlacename));
		}
	    }
	}

	textIn.close();

		
	System.out.println("done. Number of distinct place names found = " + size());

    }

}