package opennlp.textgrounder.ners;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import gnu.trove.*;

import edu.stanford.nlp.ie.crf.*;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.ling.CoreAnnotations.*;
import opennlp.textgrounder.io.DocumentSet;

import opennlp.textgrounder.util.*;

public class SNERPairListSet extends ArrayList<ArrayList<ToponymSpan>> {

    //public static final Pattern locationPattern = Pattern.compile("(\\w+/LOCATION(\\s*\\w+/LOCATION)*)");
    public static final Pattern locationTokenPattern = Pattern.compile("(\\w+/LOCATION)");
    //public Gazetteer gazetteer;
    public CRFClassifier classifier;
    //public DocumentSet docSet;
    //private int docCounter = 0;

    public SNERPairListSet(CRFClassifier classif/*, DocumentSet ds*/) {
        //	throws FileNotFoundException, IOException {

        //gazetteer = gaz;
        classifier = classif;
        //docSet = ds;
    }

    public void addToponymSpansFromFile(String locationOfFile) throws Exception {

        BufferedReader textIn = new BufferedReader(new FileReader(locationOfFile));
        //BufferedWriter textOut = new BufferedWriter(new FileWriter("SNERdump.txt"));

        System.out.print("Extracting toponym indices from " + locationOfFile + " ...");

        String contents = IOUtil.readFileAsString(locationOfFile);
        ArrayList<ToponymSpan> curSpanList = addToponymSpansFromString(contents);
        this.add(curSpanList);

        textIn.close();

        System.out.println("done. Found " + curSpanList.size()
              + " toponym spans in document " + (this.size() - 1) + ".");
    }

    /**
     * 
     * @param text
     */
    protected ArrayList<ToponymSpan> addToponymSpansFromString(String text) {
        ArrayList<ToponymSpan> curSpanList = new ArrayList<ToponymSpan>();

        String nerOutput = classifier.classifyToString(text);

        String[] tokens = nerOutput.split(" ");

        int toponymStartIndex = -1;
        int toponymEndIndex = -1;

        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
	    /*if(i==89111) {
		System.out.println(token);
		System.exit(0);
		}*/
            boolean isLocationToken = token.contains("/LOCATION");
            if (isLocationToken) {
                if (toponymStartIndex == -1) {
                    toponymStartIndex = i;
                }
                if (token.endsWith("/O")) {
                    toponymEndIndex = i + 1;
                    curSpanList.add(new ToponymSpan(toponymStartIndex, toponymEndIndex));
                    toponymStartIndex = -1;
                    toponymEndIndex = -1;
                }
            } else if (toponymStartIndex != -1) {
                toponymEndIndex = i;
                curSpanList.add(new ToponymSpan(toponymStartIndex, toponymEndIndex));
                toponymStartIndex = -1;
                toponymEndIndex = -1;
            }
        }
        //case where toponym ended at very end of document:
        if (toponymStartIndex != -1) {
            curSpanList.add(new ToponymSpan(toponymStartIndex, tokens.length));
        }
        return curSpanList;
    }

    /**
     *
     * @param docSet
     */
    public void addToponymSpansFromDocumentSet(DocumentSet docSet) {
        System.out.println("Extracting toponym indices from set of documents");
        for (int docid = 0; docid < docSet.size(); docid++) {
            String doctext = docSet.getDocumentAsString(docid);
            ArrayList<ToponymSpan> curSpanList = addToponymSpansFromString(doctext);
            this.add(curSpanList);
            System.out.println("done. Found " + curSpanList.size()
                  + " toponym spans in document " + (this.size() - 1) + ".");
        }
    }
}
