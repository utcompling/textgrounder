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
package opennlp.textgrounder.textstructs;

import edu.stanford.nlp.ie.crf.*;
import edu.stanford.nlp.ling.CoreAnnotations.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import opennlp.textgrounder.util.*;
import opennlp.textgrounder.topostructs.*;

/**
 * Array of array of toponym indexes.
 *
 * This class stores an array of sequences of toponym indexes. Any token that
 * has not been identified as a toponym is not retained in this class. The
 * indexes reference actual string toponyms, tables for which are maintained
 * in Lexicon.
 *
 * This class also processes all input text and identifies toponyms through
 * a named entity recognizer (NER) in addToponymsFromFile. It is the only
 * method in the entire project that processes input text and as such any
 * other classes which need to be populated from input is data is processed
 * here.
 * 
 * @author 
 */
public class TextProcessor {

    /**
     * NER system. This uses the Stanford NER system {@link TO COME}.
     */
    public CRFClassifier classifier;
    /**
     * Table of words and indexes.
     */
    protected Lexicon lexicon;
    /**
     * the index (or offset) of the current document being
     * examined
     */
    protected static int currentDoc = 0;
    /**
     * The number of paragraphs to treat as a single document.
     */
    protected final int parAsDocSize;

    /**
     * Constructor only to be used with derived classes
     * 
     * @param lexicon
     * @throws ClassCastException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    protected TextProcessor(Lexicon lexicon) throws ClassCastException,
          IOException, ClassNotFoundException {
        parAsDocSize = Integer.MAX_VALUE;
        Properties myClassifierProperties = new Properties();
        classifier = new CRFClassifier(myClassifierProperties);
        classifier.loadClassifier(Constants.STANFORD_NER_HOME + "/classifiers/ner-eng-ie.crf-3-all2008-distsim.ser.gz");
        this.lexicon = lexicon;
    }

    /**
     * Default constructor. Instantiate CRFClassifier.
     * 
     * @param lexicon lookup table for words and indexes
     * @param parAsDocSize number of paragraphs to use as single document. if
     * set to 0, it will process an entire file intact.
     */
    public TextProcessor(Lexicon lexicon, int parAsDocSize) throws
          ClassCastException, IOException, ClassNotFoundException {
        Properties myClassifierProperties = new Properties();
        classifier = new CRFClassifier(myClassifierProperties);
        classifier.loadClassifier(Constants.STANFORD_NER_HOME + "/classifiers/ner-eng-ie.crf-3-all2008-distsim.ser.gz");

        this.lexicon = lexicon;
        if (parAsDocSize == 0) {
            this.parAsDocSize = Integer.MAX_VALUE;
        } else {
            this.parAsDocSize = parAsDocSize;
        }
    }

    public TextProcessor(Lexicon lexicon, int parAsDocSize, boolean doingEval)
          throws
          ClassCastException, IOException, ClassNotFoundException {
        //Properties myClassifierProperties = new Properties();
        classifier = null;//new CRFClassifier(myClassifierProperties);
        //classifier.loadClassifier(Constants.STANFORD_NER_HOME + "/classifiers/ner-eng-ie.crf-3-all2008-distsim.ser.gz");

        this.lexicon = lexicon;
        if (parAsDocSize == 0) {
            this.parAsDocSize = Integer.MAX_VALUE;
        } else {
            this.parAsDocSize = parAsDocSize;
        }
    }

    /**
     * Constructor only for classes which either specify their own classifier,
     * or those which override and use the NullClassifier, which does not
     * do any named entity recognition.
     * 
     * @param classifier named entity recognition system
     * @param lexicon lookup table for words and indexes
     * @param parAsDocSize number of paragraphs to use as single document. if
     * set to 0, it will process an entire file intact.
     * @throws ClassCastException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public TextProcessor(CRFClassifier classifier, Lexicon lexicon,
          int parAsDocSize) throws ClassCastException, IOException,
          ClassNotFoundException {
        this.classifier = classifier;
        this.lexicon = lexicon;
        if (parAsDocSize == 0) {
            this.parAsDocSize = Integer.MAX_VALUE;
        } else {
            this.parAsDocSize = parAsDocSize;
        }
    }

    /**
     * Identify toponyms and populate lexicon from input file.
     * 
     * This method only splits any incoming document into smaller 
     * subdocuments based on Lexicon.parAsDocSize. The actual work of
     * identifying toponyms, converting tokens to indexes, and populating
     * arrays is handled in addToponymSpans.
     * 
     * @param locationOfFile path to input. must be a single file
     * @param lexicon the Lexicon instance that contains both the sequence
     * of token indexes and the lexicon.
     * @param evalTokenArrayBuffer buffer that holds the array of token indexes,
     * document indexes, and the toponym indexes. If this class object is
     * not needed from the calling class, then a NullTokenArrayBuffer is
     * instantiated and passed. Nothing happens inside this object.
     * @param stopwordList table that contains the list of stopwords. if
     * this is an instance of NullStopwordList, it will return false
     * through stopwordList.isStopWord for every string token examined
     * (i.e. the token is not a stopword).
     * @throws FileNotFoundException
     * @throws IOException
     */
    public void addToponymsFromFile(String locationOfFile,
          TokenArrayBuffer tokenArrayBuffer, StopwordList stopwordList) throws
          FileNotFoundException, IOException {

        BufferedReader textIn = new BufferedReader(new FileReader(locationOfFile));
        System.out.println("Extracting toponym indices from " + locationOfFile + " ...");

        String curLine = null;
        StringBuffer buf = new StringBuffer();
        int counter = 1;
        System.err.print("Processing document:" + currentDoc + ",");
        while (true) {
            curLine = textIn.readLine();
            if (curLine == null || curLine.equals("")) {
                break;
            }
            buf.append(curLine.replaceAll("[<>]", "").replaceAll("&", "and"));
            buf.append(" ");

            if (counter < parAsDocSize) {
                counter++;
            } else {
                addToponymSpans(buf.toString(), tokenArrayBuffer, stopwordList);

                buf = new StringBuffer();
                currentDoc += 1;
                counter = 1;
                System.err.print(currentDoc + ",");
            }
        }

        /**
         * Add last lines if they have not been processed and added
         */
        if (counter > 1) {
            addToponymSpans(buf.toString(), tokenArrayBuffer, stopwordList);
            System.err.print(currentDoc + ",");
        }
        System.err.println();

        assert (tokenArrayBuffer.toponymArrayList.size() == tokenArrayBuffer.wordArrayList.size());
    }

    public void addToponymsFromGoldFile(String locationOfFile,
          EvalTokenArrayBuffer evalTokenArrayBuffer, StopwordList stopwordList) throws
          FileNotFoundException, IOException {

        if (locationOfFile.endsWith("d663.tr")) {
            System.err.println(locationOfFile + " has incorrect format; skipping.");
            return;
        }

        BufferedReader textIn = new BufferedReader(new FileReader(locationOfFile));

        String curLine = null;
        while (true) {
            curLine = textIn.readLine();
            if (curLine == null) {
                break;
            }

            if ((curLine.startsWith("\t") && (!curLine.startsWith("\tc") && !curLine.startsWith("\t>")))
                  || (curLine.startsWith("c") && curLine.length() >= 2 && Character.isDigit(curLine.charAt(1)))) {
                //System.err.println(curLine);
                System.err.println(locationOfFile + " has incorrect format; skipping.");
                return;
            }

        }
        textIn.close();

        textIn = new BufferedReader(new FileReader(locationOfFile));

        System.out.println("Extracting gold standard toponym indices from " + locationOfFile + " ...");

        curLine = null;
        //StringBuffer buf = new StringBuffer();
        //int counter = 1;
        int wordidx = 0;
        boolean lookingForGoldLoc = false;
        System.err.print("Processing document:" + currentDoc + ",");
        while (true) {
            curLine = textIn.readLine();
            if (curLine == null /*|| curLine.equals("")*/) {
                break;
            }
            //buf.append(curLine);
            //buf.append(" ");

            // no idea where paragraph breaks are in TR-CoNLL dev/test sets...

            /*if (counter < parAsDocSize) {
            counter++;
            } else {*/
            //addGoldToponymSpans(curLine, evalTokenArrayBuffer, stopwordList);

            if (lookingForGoldLoc && curLine.startsWith("\t>")) {
                evalTokenArrayBuffer.goldLocationArrayList.add(parseLocation(curLine));
                lookingForGoldLoc = false;
                continue;
            } else if (curLine.startsWith("\t")) {
                continue;
            } else if (lookingForGoldLoc && !curLine.startsWith("\t")) {
                //there was no correct gold Location for this toponym
                evalTokenArrayBuffer.goldLocationArrayList.add(null);
                continue;
            }

            String[] tokens = curLine.split("\\t");

            if (tokens.length < 2) {

                continue;
            }

            String cur = tokens[0].toLowerCase();

            wordidx = lexicon.addWord(cur);
            if (!tokens[1].equals("LOC")) {
                evalTokenArrayBuffer.addElement(wordidx, currentDoc, 0, stopwordList.isStopWord(cur)
                      ? 1 : 0);
                evalTokenArrayBuffer.goldLocationArrayList.add(null);
            } else {
                evalTokenArrayBuffer.addElement(wordidx, currentDoc, 1, stopwordList.isStopWord(cur)
                      ? 1 : 0);
                lookingForGoldLoc = true;
                //gold standard Location will be added later, when line starting with tab followed by > occurs
            }

            if (Math.abs(evalTokenArrayBuffer.toponymArrayList.size() - evalTokenArrayBuffer.goldLocationArrayList.size()) > 1) {
                System.out.println(curLine);
                System.out.println("toponym: " + evalTokenArrayBuffer.toponymArrayList.size());
                System.out.println("word: " + evalTokenArrayBuffer.wordArrayList.size());
                System.out.println("gold: " + evalTokenArrayBuffer.goldLocationArrayList.size());
                System.exit(0);
            }

            //buf = new StringBuffer();
            //currentDoc += 1;
            //counter = 1;
            //System.err.print(currentDoc + ",");
            // }
        }

        /**
         * Add last lines if they have not been processed and added
         */
        /*        if (counter > 1) {
        //addToponymSpans(curLine, evalTokenArrayBuffer, stopwordList);
        if(lookingForGoldLoc && curLine.startsWith("\t>")) {
        evalTokenArrayBuffer.goldLocationArrayList.add(parseLocation(curLine));
        lookingForGoldLoc = false;
        }
        else if(!curLine.startsWith("\t")) {
        String[] tokens = curLine.split("\\t");

        if(tokens.length >= 2) {
        String cur = tokens[0].toLowerCase();

        wordidx = lexicon.addWord(cur);
        if(!tokens[1].equals("LOC")) {
        evalTokenArrayBuffer.addElement(wordidx, currentDoc, 0, stopwordList.isStopWord(cur)?1:0);
        evalTokenArrayBuffer.goldLocationArrayList.add(null);
        }
        }
        }
        System.err.print(currentDoc + ",");
        }*/
        System.err.println();

        assert (evalTokenArrayBuffer.toponymArrayList.size() == evalTokenArrayBuffer.wordArrayList.size());
        assert (evalTokenArrayBuffer.toponymArrayList.size() == evalTokenArrayBuffer.goldLocationArrayList.size());
        /*System.out.println("toponym: " + evalTokenArrayBuffer.toponymArrayList.size());
        System.out.println("word: " + evalTokenArrayBuffer.wordArrayList.size());
        System.out.println("gold: " + evalTokenArrayBuffer.goldLocationArrayList.size());*/
    }

    public Location parseLocation(String line) {
        String[] tokens = line.split("\\t");

        if (tokens.length < 6) {
            return null;
        }

        double lon = Double.parseDouble(tokens[3]);
        double lat = Double.parseDouble(tokens[4]);

        String placename;
        int gtIndex = tokens[5].indexOf(">");
        if (gtIndex != -1) {
            placename = tokens[5].substring(0, gtIndex).trim().toLowerCase();
        } else {
            placename = tokens[5].trim().toLowerCase();
        }

        return new Location(-1, placename, "", new Coordinate(lon, lat), 0, "", -1);
    }

    /**
     * Identify toponyms and non-toponyms and process accordingly.
     *
     * The entire input string (text.toString) is passed to the NER classifier.
     * The output of the classifier will be of the form 
     * <p>token/LABEL token/LABELpunctuation/LABEL token/LABEL ...</p>.
     * If LABEL is LOCATION, it is processed as a toponym or as a
     * multiword toponym if necessary. Any word that is not identified as a
     * toponym is processed as a regular token.
     *
     * When adding tokens and toponyms, all LABELs and the preceding slashes
     * are removed and the token is lowercased before being added to the
     * lexicon and the array of indexes.
     *
     * This method also adds to a sequence of document indexes and toponym
     * indexes maintained in evalTokenArrayBuffer. The sequences of token, document
     * and toponym indexes are of the same size so that they are coindexed.
     * The toponym sequence is slightly different from the other sequences
     * in that it is only populated by ones and zeros. If the current toponym
     * index is one, it means the current token is a toponym. If it is zero,
     * the current token is not a toponym.
     *
     * @param text StringBuffer of input to be processed. This should always be
     * space delimited raw text.
     * @param curSpanList sequence of toponym indexes. memory is allocated
     * outside the method but is populated in this method. After being
     * processed here, it is appended to an instance of this class through
     * add.
     * @param lexicon the Lexicon instance that contains both the sequence
     * of token indexes and the lexicon.
     * @param evalTokenArrayBuffer buffer that holds the array of token indexes,
     * document indexes, and the toponym indexes. If this class object is
     * not needed from the calling class, then a NullTokenArrayBuffer is
     * instantiated and passed. Nothing happens inside this object.
     * @param stopwordList table that contains the list of stopwords. if
     * this is an instance of NullStopwordList, it will return false
     * through stopwordList.isStopWord for every string token examined
     * (i.e. the token is not a stopword).
     * @param currentDoc 
     */
    public void addToponymSpans(String text,
          TokenArrayBuffer tokenArrayBuffer, StopwordList stopwordList) {

        String nerOutput = classifier.classifyToString(text);

        String[] tokens = nerOutput.replaceAll("-[LR]RB-", "").split(" ");
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
                    wordidx = lexicon.addWord(cur);
                    tokenArrayBuffer.addElement(wordidx, currentDoc, 1, 0);

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
                wordidx = lexicon.addWord(cur);
                tokenArrayBuffer.addElement(wordidx, currentDoc, 1, 0);

                /**
                 * Add the current token
                 */
                List<String> subtokes = retrieveWords(token);
                for (String subtoke : subtokes) {
                    if (!subtoke.isEmpty()) {
                        subtoke = subtoke.trim().toLowerCase();
                        int isstop = 0;
                        if (stopwordList.isStopWord(subtoke)) {
                            isstop = 1;
                        }
                        wordidx = lexicon.addWord(subtoke);
                        tokenArrayBuffer.addElement(wordidx, currentDoc, 0, isstop);
                    }
                }

                toponymStartIndex = -1;
                toponymEndIndex = -1;
            } else {
                List<String> subtokes = retrieveWords(token);
                for (String subtoke : subtokes) {
                    if (!subtoke.isEmpty()) {
                        subtoke = subtoke.trim().toLowerCase();
                        int isstop = 0;
                        if (stopwordList.isStopWord(subtoke)) {
                            isstop = 1;
                        }
                        wordidx = lexicon.addWord(subtoke);
                        tokenArrayBuffer.addElement(wordidx, currentDoc, 0, isstop);
                    }
                }
            }
        }

        //case where toponym ended at very end of document:
        if (toponymStartIndex != -1) {
            int wordidx = lexicon.addWord(StringUtil.join(tokens, " ", toponymStartIndex, toponymEndIndex, "/"));
            tokenArrayBuffer.addElement(wordidx, currentDoc, 1, 0);
        }
    }

    /**
     * 
     * @param token
     * @return
     */
    protected List<String> retrieveWords(String token) {
        String[] sarray = token.split("(/LOCATION|/PERSON|/ORGANIZATION|/O)");
        List<String> valid_tokens = new ArrayList<String>();
        for (String cur : sarray) {
            if (!cur.matches("^\\W*$")) {
                valid_tokens.add(cur);
            }
        }
        return valid_tokens;
    }

    /**
     * Retrieve first string of alphanumeric elements in an array of strings.
     *
     * The Stanford NER will add multiple /O, /LOCATION, /PERSON etc. tags to
     * a single token if it includes punctuation (though there might be other
     * character types that trigger the NER system to add multiple tags). We
     * take this token with multiple tags and split it on the slashes. This
     * method will find the first string that includes only characters
     * that fit the regex <p>^\\W*$</p>. If no such string is found, it
     * returns the empty string "".
     *
     * @param sarray Array of strings.
     * @param idx Index value in array of strings to examine
     * @return String that has only alphanumeric characters
     */
    protected String retrieveWords(String[] sarray, int idx) {
        String cur = null;
        try {
            cur = sarray[idx];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }

        if (idx != 0 && cur.startsWith("O")) {
            cur = cur.substring(1);
        }

        if (!cur.matches("^\\W*$")) {
            return cur;
        } else {
            return retrieveWords(sarray, idx + 1);
        }
    }

    public void addGoldToponymSpans(String text,
          TokenArrayBuffer tokenArrayBuffer, StopwordList stopwordList) {

        /*if(text.startsWith("\t>")) {
        evalTokenArrayBuffer.goldLocationArrayList.add(parseLocation(text));
        continue;
        }
        else if(text.startsWith("\t")) continue;

        String[] tokens = text.split("\\t");

        if(tokens.length < 2) continue;

        String cur = tokens[0].toLowerCase();

        wordidx = lexicon.addWord(cur);
        if(!tokens[1].equals("LOC")) {
        evalTokenArrayBuffer.addElement(wordidx, currentDoc, 0, stopwordList.isStopWord(cur)?1:0);
        evalTokenArrayBuffer.goldLocationArrayList.add(null);
        }
        else {
        evalTokenArrayBuffer.addElement(wordidx, currentDoc, 1, stopwordList.isStopWord(cur)?1:0);
        //gold standard Location will be added later, when line starting with tab followed by > occurs
        }*/
    }
}
