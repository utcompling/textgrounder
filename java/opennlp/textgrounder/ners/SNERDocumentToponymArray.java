package opennlp.textgrounder.ners;

import edu.stanford.nlp.ie.crf.*;
import edu.stanford.nlp.ling.CoreAnnotations.*;

import java.io.*;
import java.util.ArrayList;

import opennlp.textgrounder.io.DocumentSet;
import opennlp.textgrounder.models.callbacks.StopwordList;
import opennlp.textgrounder.models.callbacks.TokenArrayBuffer;
import opennlp.textgrounder.util.*;

public class SNERDocumentToponymArray extends ArrayList<ArrayList<Integer>> {

    public CRFClassifier classifier;

    public SNERDocumentToponymArray(CRFClassifier classif) {
        classifier = classif;
    }

    /**
     * Identify toponyms and populate docSet from input file.
     * 
     * @param locationOfFile
     * @param docSet
     * @param tokenArrayBuffer
     * @throws FileNotFoundException
     * @throws IOException
     */
    public void addToponymsFromFile(String locationOfFile,
          DocumentSet docSet, TokenArrayBuffer tokenArrayBuffer,
          StopwordList stopwordList) throws
          FileNotFoundException, IOException {

        BufferedReader textIn = new BufferedReader(new FileReader(locationOfFile));
        System.out.print("Extracting toponym indices from " + locationOfFile + " ...");

        int parAsDocSize = docSet.getParAsDocSize();
        docSet.newDoc();
        int currentDoc = docSet.size() - 1;

        ArrayList<Integer> curSpanList = new ArrayList<Integer>();
        String curLine = null;
        StringBuffer buf = new StringBuffer();
        int counter = 1;
        System.err.print("Processing document 1,");
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
                addToponymSpans(buf, curSpanList, docSet, tokenArrayBuffer, stopwordList, currentDoc);
                add(curSpanList);

                curSpanList = new ArrayList<Integer>();
                buf = new StringBuffer();
                docSet.newDoc();
                currentDoc += 1;
                counter = 1;
                System.err.print(currentDoc + 1 + ",");
            }
        }

        /**
         * Add last lines if they have not been processed and added
         */
        if (counter > 1) {
            addToponymSpans(buf, curSpanList, docSet, tokenArrayBuffer, stopwordList, currentDoc);
            add(curSpanList);
            System.err.print(currentDoc + 1 + ",");
        }
        System.err.println();
    }

    /**
     * 
     * @param buf
     * @param curSpanList
     * @param docSet
     * @param tokenArrayBuffer
     * @param currentDoc
     */
    public void addToponymSpans(StringBuffer buf,
          ArrayList<Integer> curSpanList, DocumentSet docSet,
          TokenArrayBuffer tokenArrayBuffer, StopwordList stopwordList,
          int currentDoc) {

        String nerOutput = classifier.classifyToString(buf.toString());

        String[] tokens = nerOutput.split(" ");
        int toponymStartIndex = -1;
        int toponymEndIndex = -1;

        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            int wordidx = 0;
            boolean isLocationToken = token.contains("/LOCATION");
            if (isLocationToken) {
                if (toponymStartIndex == -1) {
                    toponymStartIndex = i;
                }
                if (token.endsWith("/O")) {
                    toponymEndIndex = i + 1;
                    String cur = StringUtil.join(tokens, " ", toponymStartIndex, toponymEndIndex, "/").toLowerCase();
                    wordidx = docSet.addWordToSeq(cur);
                    curSpanList.add(wordidx);
                    tokenArrayBuffer.addWord(wordidx);
                    tokenArrayBuffer.addDoc(currentDoc);
                    tokenArrayBuffer.addToponym(1);

                    toponymStartIndex = -1;
                    toponymEndIndex = -1;
                }
            } else if (toponymStartIndex != -1) {
                toponymEndIndex = i;
                /**
                 * Add the toponym that spans from toponymStartIndex to
                 * toponymEndIndex. This does not include the current token
                 */
                String cur = StringUtil.join(tokens, " ", toponymStartIndex, toponymEndIndex, "/").toLowerCase();
                wordidx = docSet.addWordToSeq(cur);
                curSpanList.add(wordidx);
                tokenArrayBuffer.addWord(wordidx);
                tokenArrayBuffer.addDoc(currentDoc);
                tokenArrayBuffer.addToponym(1);

                /**
                 * Add the current token
                 */
                cur = token.split("/")[0];
                if (!stopwordList.isStopWord(cur)) {
                    wordidx = docSet.addWordToSeq(cur);
                    tokenArrayBuffer.addWord(wordidx);
                    tokenArrayBuffer.addDoc(currentDoc);
                    tokenArrayBuffer.addToponym(0);
                }

                toponymStartIndex = -1;
                toponymEndIndex = -1;
            } else {
                String cur = token.split("/")[0];
                if (!stopwordList.isStopWord(cur)) {
                    docSet.addWordToSeq(cur);
                    tokenArrayBuffer.addWord(wordidx);
                    tokenArrayBuffer.addDoc(currentDoc);
                    tokenArrayBuffer.addToponym(0);
                }
            }
        }

        //case where toponym ended at very end of document:
        if (toponymStartIndex != -1) {
            int wordidx = docSet.addWordToSeq(StringUtil.join(tokens, " ", toponymStartIndex, toponymEndIndex, "/"));
            curSpanList.add(wordidx);
            tokenArrayBuffer.addWord(wordidx);
            tokenArrayBuffer.addDoc(currentDoc);
            tokenArrayBuffer.addToponym(1);
        }
    }
}
