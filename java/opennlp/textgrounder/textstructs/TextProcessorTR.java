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

// FIXME: Doesn't appear that the following code is used
import edu.stanford.nlp.ling.CoreAnnotations.*;

import java.io.*;

import opennlp.textgrounder.ners.NullClassifier;
import opennlp.textgrounder.topostructs.*;

/**
 * Class that reads from a file in TR-CoNLL format, extracts tokens and (for
 * tokens that are toponyms) gold-standard location information, and adds the
 * token data to a TokenArrayBuffer.
 * 
 * Files in TR-CoNLL format look like this: <code>
Turkey  LOC
        c1      NGA     -24.35  30.4333333      Turkey > (SF04) > South Africa
        c2      NGA     -24.3333333     30.45   Turkey > (SF04) > South Africa
        c3      NGA     -24.1   151.65  Turkey > Queensland > Australia
        c4      NGA     -24.1   151.6333333     Turkey > Queensland > Australia
        >c5     NGA     39      35      Turkey
        c6      USGS_PP 36.24444        -92.76361       Turkey > Marion > AR > US > North America
        c7      USGS_PP 37.47917        -83.50778       Turkey > Breathitt > KY > US > North America
        c8      USGS_PP 34.99222        -78.18333       Turkey > Sampson > NC > US > North America
        c9      USGS_PP 39.10833        -83.44889       Turkey > Highland > OH > US > North America
        c10     USGS_PP 34.3925 -100.89722      Turkey > Hall > TX > US > North America
        c11     USGS_PP 37.64028        -81.57833       Turkey > Wyoming > WV > US > North America
        c12     CIAWFB  39      -0      Turkey
says    O       I-VP    VBZ
killed  O       I-VP    VBN
17      O       I-NP    CD
Kurd    I-MISC  I-NP    NNP
rebels  O       I-NP    NNS
in      O       I-PP    IN
clashes O       I-NP    NNS
.       O       O       .
 * </code>
 * 
 * Note that we only extract the gold-standard location from the list of all
 * possible locations.
 * 
 * FIXME: This class is a subclass of TextProcessor but in reality it's almost
 * completely independent.
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

    /**
     *
     * @param locationOfFile
     * @param evalTokenArrayBuffer
     * @param stopwordList
     * @throws FileNotFoundException
     * @throws IOException
     */
    public void processFile(String locationOfFile,
          TokenArrayBuffer evalTokenArrayBuffer)
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
                evalTokenArrayBuffer.addElement(wordidx, currentDoc, 1, parseLocation(cur, curLine, wordidx));
                lookingForGoldLoc = false;
                continue;
            } else if (curLine.startsWith("\t")) {
                continue;
            } else if (lookingForGoldLoc && !curLine.startsWith("\t")) {
                //there was no correct gold Location for this toponym
                evalTokenArrayBuffer.addElement(wordidx, currentDoc, 1, null);
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
                evalTokenArrayBuffer.addElement(wordidx, currentDoc, 0, null);
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

    public Location parseLocation(String token, String line, int wordidx) {
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
