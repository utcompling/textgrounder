package opennlp.textgrounder.gazetteers;

import opennlp.textgrounder.util.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.*;

import gnu.trove.*;
import opennlp.textgrounder.geo.Coordinate;
import opennlp.textgrounder.geo.Location;

public class USGSGazetteer extends Gazetteer {

    public USGSGazetteer () throws FileNotFoundException, IOException {
	this(Constants.TEXTGROUNDER_DATA+"/gazetteer/pop_places_plaintext.txt.gz");
    }

    public USGSGazetteer (String location) throws FileNotFoundException, IOException {

	//BufferedReader gazIn = new BufferedReader(new FileReader(location));
	
	BufferedReader gazIn = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(location))));
	
	System.out.print("Populating USGS gazetteer from " + location + " ...");

	String curLine;
	String[] tokens;

	curLine = gazIn.readLine(); // first line of gazetteer is legend
	while(true) {
	    curLine = gazIn.readLine();
	    if(curLine == null) break;
	    //System.out.println(curLine);
	    tokens = curLine.split("\\|");
	    /*for(String token : tokens) {
	      System.out.println(token);
	      }*/
	    if(tokens.length < 17) {
		System.out.println("\nNot enough columns found; this file format should have at least " + 17 + " but only " + tokens.length + " were found. Quitting.");
		System.exit(0);
	    }
	    Coordinate curCoord = 
		new Coordinate(Double.parseDouble(tokens[9]), Double.parseDouble(tokens[10]));
	    
	    putIfAbsent(tokens[1].toLowerCase(), curCoord);
	    putIfAbsent(tokens[5].toLowerCase(), curCoord);
	    putIfAbsent(tokens[16].toLowerCase(), curCoord);
	}
	
	gazIn.close();

	System.out.println("done. Total number of actual place names = " + size());
	       		
	/*System.out.println(placenamesToCoords.get("salt springs"));
	  System.out.println(placenamesToCoords.get("galveston"));*/

    }

    public List<Location> get(String placename) {
	return new ArrayList<Location>();
    }
}