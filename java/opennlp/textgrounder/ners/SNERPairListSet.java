package opennlp.textgrounder.ners;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import gnu.trove.*;

import edu.stanford.nlp.ie.crf.*;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.ling.CoreAnnotations.*;
import java.util.ArrayList;

import opennlp.textgrounder.io.DocumentSet;
import opennlp.textgrounder.util.StringUtil;
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

    /**
     * Identify toponyms and populate docSet from input file.
     * 
     * @param locationOfFile
     * @param docSet
     */
    public void addToponymSpansFromFile(String locationOfFile,
          DocumentSet docSet) throws FileNotFoundException, IOException {

        BufferedReader textIn = new BufferedReader(new FileReader(locationOfFile));
        System.out.print("Extracting toponym indices from " + locationOfFile + " ...");

        int parAsDocSize = docSet.getParAsDocSize();
        docSet.newDoc();
        
        ArrayList<ToponymSpan> curSpanList = new ArrayList<ToponymSpan>();
        String curLine = null;
        StringBuffer buf = new StringBuffer();
        int counter = 1;
        while (true) {
            curLine = textIn.readLine();
            if (curLine == null || curLine.equals("")) {
                break;
            }
            buf.append(curLine);
            buf.append(" ");

            if (counter < parAsDocSize) {
                counter++;
            } else {
                addToponymSpans(buf, curSpanList, docSet);
                add(curSpanList);
                
                curSpanList = new ArrayList<ToponymSpan>();
                buf = new StringBuffer();
                docSet.newDoc();
                counter = 1;
            }
        }

        /**
         * Add last lines if they have not been processed and added
         */
        if (counter > 1) {
            addToponymSpans(buf, curSpanList, docSet);
            add(curSpanList);
        }
    }

    /**
     * 
     * @param buf
     * @param curSpanList
     * @param docSet
     */
    public void addToponymSpans(StringBuffer buf,
          ArrayList<ToponymSpan> curSpanList, DocumentSet docSet) {

        String nerOutput = classifier.classifyToString(buf.toString());

        String[] tokens = nerOutput.split(" ");
        int toponymStartIndex = -1;
        int toponymEndIndex = -1;

        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            boolean isLocationToken = token.contains("/LOCATION");
            if (isLocationToken) {
                if (toponymStartIndex == -1) {
                    toponymStartIndex = i;
                }
                if (token.endsWith("/O")) {
                    toponymEndIndex = i + 1;
                    curSpanList.add(new ToponymSpan(toponymStartIndex, toponymEndIndex));
                    docSet.addWordToSeq(StringUtil.join(tokens, " ", toponymStartIndex, toponymEndIndex, "/"));

                    toponymStartIndex = -1;
                    toponymEndIndex = -1;
                }
            } else if (toponymStartIndex != -1) {
                toponymEndIndex = i;
                curSpanList.add(new ToponymSpan(toponymStartIndex, toponymEndIndex));
                docSet.addWordToSeq(StringUtil.join(tokens, " ", toponymStartIndex, toponymEndIndex, "/"));
                
                String curToken = token.split("/")[0];
                docSet.addWordToSeq(curToken);

                toponymStartIndex = -1;
                toponymEndIndex = -1;
            } else {
                String curToken = token.split("/")[0];
                docSet.addWordToSeq(curToken);
            }
        }

        //case where toponym ended at very end of document:
        if (toponymStartIndex != -1) {
            curSpanList.add(new ToponymSpan(toponymStartIndex, tokens.length));
        }
    }
}
