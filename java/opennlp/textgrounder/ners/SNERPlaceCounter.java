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

import edu.stanford.nlp.ie.crf.*;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.ling.CoreAnnotations.*;

import opennlp.textgrounder.gazetteers.Gazetteer;
import opennlp.textgrounder.util.*;

public class SNERPlaceCounter extends TObjectIntHashMap<String> {

    public static final Pattern locationPattern = Pattern.compile("(\\w+/LOCATION(\\s*\\w+/LOCATION)*)");

    public Gazetteer gazetteer;
    public CRFClassifier classifier;

    public SNERPlaceCounter(Gazetteer gaz, CRFClassifier classif) {
	//	throws FileNotFoundException, IOException {

	gazetteer = gaz;
	classifier = classif;
    }


    public void extractPlacesFromFile(String locationOfFile) throws Exception {
	BufferedReader textIn = new BufferedReader(new FileReader(locationOfFile));
		
	System.out.print("Extracting place names from " + locationOfFile + " ...");

	String contents = IOUtil.readFileAsString(locationOfFile);
	String nerOutput = classifier.classifyToString(contents);

	Matcher m = locationPattern.matcher(nerOutput);
	while(m.find() == true) {
	    String potentialPlacename = m.group();
	    potentialPlacename = potentialPlacename.replaceAll("/LOCATION", "").toLowerCase();
	    if(gazetteer.contains(potentialPlacename))
		//|| (gazetteer instanceof WGGazetteer && ((WGGazetteer)gazetteer).hasPlace(potentialPlacename)))
		if (!increment(potentialPlacename))
		    put(potentialPlacename,1);
	}    

	//while(true) {
	//    String curLine = textIn.readLine();
	//    if(curLine == null) break;
	//    curLine = curLine.trim();
	//    if(curLine.length() == 0) continue;
	//
	//    String nerOutput = classifier.classifyToString(curLine);
	//
	//    Matcher m = locationPattern.matcher(nerOutput);
	//
	//    while(m.find() == true) {
	//	String potentialPlacename = m.group();
	//	potentialPlacename = potentialPlacename.replaceAll("/LOCATION", "").toLowerCase();
	//	if(gazetteer.containsKey(potentialPlacename))
	//	    if (!increment(potentialPlacename))
	//		put(potentialPlacename,1);
	//    }
	//}

	//String nerOutput = classif.classifyToString(locationOfFile);

	

	/*for(List<CoreLabel> sentence : classifiedLine) {
		for(CoreLabel curLabel : sentence) {
		    String label = curLabel.ner();
		    System.out.println(curLabel.current() + "/" + label);
		    if(label != null && label.equals("LOCATION")) {
		       String potentialPlacename = curLabel.current();
	    
		       if(gaz.containsKey(potentialPlacename)) {
			   if (!increment(potentialPlacename))
			       put(potentialPlacename,1);
			   
		       }
		    }
		}
		}*/
		
	/*while(true) {
	    String curLine = textIn.readLine();
	    if(curLine == null) break;
	    curLine = curLine.trim();
	    if(curLine.length() == 0) continue;
	    //Matcher m = placeNamePattern.matcher(curLine);
	    //while(m != null && m.find()) {
	    //String potentialPlacename = curLine.toLowerCase();

	    //System.out.println(classif.classifyToString(curLine));

	    List<List<CoreLabel> > classifiedLine = classif.classify(curLine);

	    for(List<CoreLabel> sentence : classifiedLine) {
		for(CoreLabel curLabel : sentence) {
		    String label = curLabel.getString(AnswerAnnotation.class);
		    //System.out.println(curLabel.current() + "/" + label);
		    if(label != null && label.startsWith("LOC")) {
			String potentialPlacename = curLabel.current().toLowerCase();
	    
			if(gaz.containsKey(potentialPlacename)) {
			    if (!increment(potentialPlacename)) {
			       put(potentialPlacename,1);
			       System.out.println("Added " + potentialPlacename);
			    }
			}
		    }
		}
		}
		}*/

	textIn.close();

		
	System.out.println("done. Number of distinct place names found = " + size());

    }

}
