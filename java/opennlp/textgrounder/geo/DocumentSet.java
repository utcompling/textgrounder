package opennlp.textgrounder.geo;

import opennlp.textgrounder.util.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import gnu.trove.*;

import edu.stanford.nlp.ie.crf.*;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.ling.CoreAnnotations.*;

public class DocumentSet extends ArrayList<ArrayList<Integer> > {

    public TObjectIntHashMap<String> wordsToInts = new TObjectIntHashMap<String>();
    public TIntObjectHashMap<String> intsToWords = new TIntObjectHashMap<String>();

    protected int nextInt = 0;

    public DocumentSet() {
	
    }

    public String getWordForInt(int someInt) {
	return intsToWords.get(someInt);
    }

    public int getIntForWord(String someWord) {
	return wordsToInts.get(someWord);
    }

    public void addDocumentFromFile(String locationOfFile) throws Exception {

	BufferedReader textIn = new BufferedReader(new FileReader(locationOfFile));

	System.out.print("Processing document at " + locationOfFile + " ...");

	ArrayList<Integer> curDoc = new ArrayList<Integer>();
	this.add(curDoc);

	String curLine;
	while(true) {
	    curLine = textIn.readLine();
	    if(curLine == null || curLine.equals(""))
		break;
	    
	    for(String token : curLine.split(" ")) {
		if(wordsToInts.containsKey(token)) {
		    curDoc.add(wordsToInts.get(token));
		}
		else {
		    wordsToInts.put(token, nextInt);
		    intsToWords.put(nextInt, token);
		    curDoc.add(nextInt);
		    nextInt++;
		}
	    }
	    
	}

	System.out.println("done.");

	textIn.close();
    }

}
