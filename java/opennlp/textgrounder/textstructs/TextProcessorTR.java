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
import opennlp.textgrounder.ners.NullClassifier;

import opennlp.textgrounder.util.*;
import opennlp.textgrounder.topostructs.*;

/**
 * 
 * @author 
 */
public class TextProcessorTR extends TextProcessor {

    /**
     * Default constructor. Instantiate CRFClassifier.
     * 
     * @param lexicon lookup table for words and indexes
     * @param parAsDocSize number of paragraphs to use as single document. if
     * set to 0, it will process an entire file intact.
     */
    public TextProcessorTR(Lexicon lexicon) throws
          ClassCastException, IOException, ClassNotFoundException {
        super(new NullClassifier(), lexicon, 0);
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
}
