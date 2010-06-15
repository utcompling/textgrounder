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

import edu.stanford.nlp.ling.CoreAnnotations.*;

import java.io.*;

import opennlp.textgrounder.ners.NullClassifier;
import opennlp.textgrounder.topostructs.*;

/**
 * 
 * @author 
 */
public class TextProcessorTR<E extends SmallLocation> extends TextProcessor {

    /**
     *
     */
    protected E genericsKludgeFactor;

    /**
     * Default constructor. Instantiate CRFClassifier.
     * 
     * @param lexicon lookup table for words and indexes
     * @param parAsDocSize number of paragraphs to use as single document. if
     * set to 0, it will process an entire file intact.
     */
    public TextProcessorTR(Lexicon lexicon, E _genericsKludgeFactor) throws
          ClassCastException, IOException, ClassNotFoundException {
        super(new NullClassifier(), lexicon, 0);
        genericsKludgeFactor = _genericsKludgeFactor;
    }

    /**
     *
     * @param locationOfFile
     * @param evalTokenArrayBuffer
     * @param stopwordList
     * @throws FileNotFoundException
     * @throws IOException
     */
    @Override
    public void addToponymsFromFile(String locationOfFile,
          TokenArrayBuffer evalTokenArrayBuffer, StopwordList stopwordList)
          throws
          FileNotFoundException, IOException {

        if (locationOfFile.endsWith("d663.tr")) {
            System.err.println(locationOfFile + " has incorrect format; skipping.");
            return;
        }

        BufferedReader textIn = new BufferedReader(new FileReader(locationOfFile));

        String curLine = null, cur = null;
        while (true) {
            curLine = textIn.readLine();
            if (curLine == null) {
                break;
            }

            if ((curLine.startsWith("\t") && (!curLine.startsWith("\tc") && !curLine.startsWith("\t>")))
                  || (curLine.startsWith("c") && curLine.length() >= 2 && Character.isDigit(curLine.charAt(1)))) {
                System.err.println(locationOfFile + " has incorrect format; skipping.");
                return;
            }

        }
        textIn.close();

        textIn = new BufferedReader(new FileReader(locationOfFile));

        System.out.println("Extracting gold standard toponym indices from " + locationOfFile + " ...");

        curLine = null;

        int wordidx = 0;
        boolean lookingForGoldLoc = false;
        System.err.print("Processing document:" + currentDoc + ",");
        while (true) {
            curLine = textIn.readLine();
            if (curLine == null) {
                break;
            }

            if (lookingForGoldLoc && curLine.startsWith("\t>")) {
                evalTokenArrayBuffer.addElement(wordidx, currentDoc, 1, stopwordList.isStopWord(cur)
                      ? 1 : 0, (E) parseLocation(cur, curLine, wordidx));
                lookingForGoldLoc = false;
                continue;
            } else if (curLine.startsWith("\t")) {
                continue;
            } else if (lookingForGoldLoc && !curLine.startsWith("\t")) {
                //there was no correct gold Location for this toponym
                evalTokenArrayBuffer.addElement(wordidx, currentDoc, 1, stopwordList.isStopWord(cur)
                      ? 1 : 0, null);
                lookingForGoldLoc = false;
//                continue;
            }

            String[] tokens = curLine.split("\\t");

            if (tokens.length < 2) {

                continue;
            }

            cur = tokens[0].toLowerCase();

            wordidx = lexicon.addWord(cur);
            if (!tokens[1].equals("LOC")) {
                evalTokenArrayBuffer.addElement(wordidx, currentDoc, 0, stopwordList.isStopWord(cur)
                      ? 1 : 0, null);
            } else {
                lookingForGoldLoc = true;
                //gold standard Location will be added later, when line starting with tab followed by > occurs
            }

            evalTokenArrayBuffer.verboseSanityCheck(curLine);
        }

        currentDoc += 1;

        System.err.println();

        assert (evalTokenArrayBuffer.sanityCheck1());
        assert (evalTokenArrayBuffer.sanityCheck2());
    }

    public E parseLocation(String token, String line, int wordidx) {
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

        return generateLocation(-1, token, "", new Coordinate(lon, lat), 0, "", -1, wordidx);
    }

    protected E generateLocation(int _id, String _name, String _type,
          Coordinate _coord, int _pop, String _container, int _count,
          int _nameid) {
        E locationToAdd;
        if (genericsKludgeFactor instanceof Location) {
            locationToAdd = (E) new Location(_id, _name, _type, _coord, _pop, _container, _count);
        } else {
            locationToAdd = (E) new SmallLocation(_id, _nameid, _coord, _count);
        }
        return locationToAdd;
    }
}
