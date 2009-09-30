package opennlp.textgrounder.geo;

import java.io.*;
import java.util.*;
import java.util.zip.*;

import gnu.trove.*;

public class Gazetteer extends THashMap<String, Coordinate> {

    public Gazetteer (String location) throws FileNotFoundException, IOException {

	BufferedReader gazIn = new BufferedReader(new FileReader(location));
	
	//BufferedReader gazIn = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(location))));
	
	System.out.print("Populating gazetteer...");
		
	String curLine = gazIn.readLine(); // first line of gazetteer is legend
	while(true) {
	    curLine = gazIn.readLine();
	    if(curLine == null) break;
	    //System.out.println(curLine);
	    String[] tokens = curLine.split("\\|");
	    /*for(String token : tokens) {
	      System.out.println(token);
	      }*/
	    //System.out.println(tokens[9]);
	    //System.out.println(tokens[10]);
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
}