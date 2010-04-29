///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.ners;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import gnu.trove.*;

import opennlp.textgrounder.gazetteers.Gazetteer;

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
