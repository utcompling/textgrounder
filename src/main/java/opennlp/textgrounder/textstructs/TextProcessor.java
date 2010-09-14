///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Ben Wing, Taesun Moon, The University of Texas at Austin
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

import java.io.*;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;

import org.jdom.*;
import org.jdom.input.SAXBuilder;

import opennlp.textgrounder.ners.*;
import opennlp.textgrounder.topostructs.Coordinate;
import opennlp.textgrounder.topostructs.Location;

/**
 * Class of static methods that read from a file containing text in some format,
 * possibly along with tags identifying whether they are toponyms (i.e.
 * locations) or other such information; generates a CorpusDocument object; and
 * populates it with tokens (mostly words, but can be multiword place-name
 * collocations). Each token is identified as to whether it is a toponym and
 * possibly also what type of token it is from a named-entity-recognition (NER)
 * perspective. If the text doesn't explicitly indicate which words are
 * toponyms, a named-entity recognizer (NER) is run to determine this info.
 * 
 * In addition, the CorpusDocument object has a mid-level structure made up of
 * Division objects that reflect the internal structure of the source file (e.g.
 * chapters, paragraphs, sentences). Furthermore, each CorpusDocument is itself
 * part of a Corpus (i.e. a collection of documents with common Lexicon and
 * Gazetteer objects). they are locations (toponyms), possibly with additional
 * properties. Tokens are added to a Document object.
 * 
 * Multi-word location tokens (e.g. New York) are joined by the code below into
 * a single token.
 * 
 * @author Ben Wing partially based on earlier code by Taesun Moon
 */
public abstract class TextProcessor {
    /**
     * Process a raw-text file, running it through an NER to identify toponyms.
     * See comment at top of class.
     * 
     * This function works by splitting any incoming document into smaller
     * subdocuments based on parAsDocSize. These subdocuments are then passed to
     * the NER to do the actual work of generating Token objects and populating
     * the CorpusDocument with them.
     * 
     * @param locationOfFile
     *            Path to input. Must be a single file.
     * @param corpus
     * @param ner
     * @param parAsDocSize
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void processNER(String locationOfFile, Corpus corpus,
            NamedEntityRecognizer ner, int parAsDocSize) {

        try {
            CorpusDocument doc = new CorpusDocument(corpus, locationOfFile);
            BufferedReader textIn = new BufferedReader(new FileReader(
                    locationOfFile));
            System.out.println("Extracting toponym indices from "
                    + locationOfFile + " ...");

            String curLine = null;
            StringBuffer buf = new StringBuffer();
            int counter = 1;
            int currentDoc = 0;
            System.err.print("Processing document:" + locationOfFile + ",");
            while (true) {
                curLine = textIn.readLine();
                if (curLine == null || curLine.equals("")) {
                    break;
                }
                buf.append(curLine.replaceAll("[<>]", "")
                        .replaceAll("&", "and"));
                buf.append(" ");

                if (counter < parAsDocSize) {
                    counter++;
                } else {
                    ner.processText(doc, buf.toString());
                    corpus.add(doc);
                    doc = new CorpusDocument(corpus, locationOfFile);

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
                ner.processText(doc, buf.toString());
                System.err.print(currentDoc + ",");
            }
            System.err.println();

            corpus.add(doc);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Function to process TEI (text encoding initiative) encoded XML files,
     * which are used to encode the PCL travel corpus. This function reads in
     * the files, ignores everything but the actual text, and runs the text
     * through a named entity recognizer (NER) to get toponym and non-toponym
     * tokens, the same way that the function processNER() does. By using the
     * named entity definitions that come with the dtd for pcl travel, all
     * encoding issues are handled within this function. The text is normalized
     * according to Unicode standards and then any characters not within the
     * ASCII range (0x00-0x7E) are stripped out.
     * 
     * @param locationOfFile
     *            Path of file to be processsed.
     * @param corpus
     *            Corpus to add newly created CorpusDocument to.
     * @param ner
     *            Named-entity recognizer to be used to process the text of the
     *            file.
     * @author tsmoon
     */
    public static void processTEIXML(String locationOfFile, Corpus corpus,
            NamedEntityRecognizer ner) {

        if (!locationOfFile.endsWith(".xml")) {
            return;
        }

        SAXBuilder builder = new SAXBuilder();
        File file = new File(locationOfFile);
        CorpusDocument cdoc = new CorpusDocument(corpus, locationOfFile);
        Document doc = null;
        int currentDoc = 0;
        try {
            doc = builder.build(file);
            Element element = doc.getRootElement();
            Element child = element.getChild("text").getChild("body");
            List<Element> divs = new ArrayList<Element>(
                    child.getChildren("div"));

            for (Element div : divs) {
                StringBuilder buf = new StringBuilder();
                List<Element> pars = new ArrayList<Element>(
                        div.getChildren("p"));
                for (Element par : pars) {
                    for (char c : Normalizer.normalize(par.getTextNormalize(),
                            Normalizer.Form.NFKC).toCharArray()) {
                        if (((int) c) < 0x7F) {
                            buf.append(c);
                        }
                    }
                    buf.append(System.getProperty("line.separator"));
                }
                String text = buf.toString().trim();
                if (!text.isEmpty()) {
                    ner.processText(cdoc, text);
                    currentDoc += 1;
                    System.err.print(currentDoc + ",");
                }
            }

        } catch (JDOMException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * 
     * @param locationOfFile Path of file to be processsed.
     * @param corpus Corpus to add newly created CorpusDocument to.
     */
    public static void processTR(String locationOfFile, Corpus corpus) {

        if (locationOfFile.endsWith("d663.tr")) {
            System.err.println(locationOfFile
                    + " has incorrect format; skipping.");
            return;
        }

        try {
            BufferedReader textIn = new BufferedReader(new FileReader(
                    locationOfFile));
            CorpusDocument doc = new CorpusDocument(corpus, locationOfFile);
            String curLine = null, cur = null;
            while (true) {
                curLine = textIn.readLine();
                if (curLine == null) {
                    break;
                }

                if ((curLine.startsWith("\t") && (!curLine.startsWith("\tc") && !curLine
                        .startsWith("\t>")))
                        || (curLine.startsWith("c") && curLine.length() >= 2 && Character
                                .isDigit(curLine.charAt(1)))) {
                    System.err.println(locationOfFile
                            + " has incorrect format; skipping.");
                    return;
                }

            }
            textIn.close();

            textIn = new BufferedReader(new FileReader(locationOfFile));

            System.out.println("Extracting gold standard toponym indices from "
                    + locationOfFile + " ...");

            curLine = null;

            int wordidx = 0;
            boolean lookingForGoldLoc = false;
            while (true) {
                curLine = textIn.readLine();
                if (curLine == null) {
                    break;
                }

                if (lookingForGoldLoc && curLine.startsWith("\t>")) {
                    Token tok = new Token(doc, wordidx, true);
                    tok.goldLocation = parseTRLocation(cur, curLine, wordidx);
                    doc.add(tok);
                    lookingForGoldLoc = false;
                    continue;
                } else if (curLine.startsWith("\t")) {
                    continue;
                } else if (lookingForGoldLoc && !curLine.startsWith("\t")) {
                    // there was no correct gold Location for this toponym
                    doc.add(new Token(doc, wordidx, true));
                    lookingForGoldLoc = false;
                    // continue;
                }

                String[] tokens = curLine.split("\\t");

                if (tokens.length < 2) {

                    continue;
                }

                cur = tokens[0].toLowerCase();

                wordidx = corpus.getLexicon().getOrAdd(cur);
                if (!tokens[1].equals("LOC")) {
                    doc.add(new Token(doc, wordidx, false));
                } else {
                    lookingForGoldLoc = true;
                    // gold standard Location will be added later, when line
                    // starting with tab followed by > occurs
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        System.err.println();
    }

    /**
     * 
     * @param token
     * @param line
     * @param wordidx
     * @return
     */
    public static Location parseTRLocation(String token, String line, int wordidx) {
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
     * 
     * @param locationOfFile Path of file to be processsed.
     * @param corpus Corpus to add newly created CorpusDocument to.
     */
    public static void processXML(String locationOfFile, Corpus corpus) {
        CorpusDocument doc = new CorpusDocument(corpus, locationOfFile);
        doc.loadFromXML(locationOfFile);
        corpus.add(doc);
    }
}
