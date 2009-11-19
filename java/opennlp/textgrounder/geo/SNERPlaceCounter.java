package opennlp.textgrounder.geo;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import gnu.trove.*;

import edu.stanford.nlp.ie.crf.*;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.ling.CoreAnnotations.*;

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

	while(true) {
	    String curLine = textIn.readLine();
	    if(curLine == null) break;
	    curLine = curLine.trim();
	    if(curLine.length() == 0) continue;

	    String nerOutput = classifier.classifyToString(curLine);

	    Matcher m = locationPattern.matcher(nerOutput);

	    while(m.find() == true) {
		String potentialPlacename = m.group();
		potentialPlacename = potentialPlacename.replaceAll("/LOCATION", "").toLowerCase();
		if(gazetteer.containsKey(potentialPlacename))
		    if (!increment(potentialPlacename))
			put(potentialPlacename,1);
	    }
	}

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