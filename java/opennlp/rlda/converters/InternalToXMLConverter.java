///////////////////////////////////////////////////////////////////////////////
//  Copyright 2010 Taesun Moon <tsunmoon@gmail.com>.
// 
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  under the License.
///////////////////////////////////////////////////////////////////////////////
package opennlp.rlda.converters;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import opennlp.rlda.apps.ConverterExperimentParameters;
import opennlp.rlda.textstructs.*;
import opennlp.rlda.topostructs.*;
import opennlp.rlda.wrapper.io.*;
import org.jdom.Attribute;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class InternalToXMLConverter {

    /**
     *
     */
    protected String pathToInput;
    /**
     *
     */
    protected String pathToOutput;
    /**
     * 
     */
    protected Lexicon lexicon;
    /**
     *
     */
    protected int degreesPerRegion;
    /**
     *
     */
    protected ConverterExperimentParameters converterExperimentParameters;
    /**
     *
     */
    protected Region[][] regionMatrix;
    /**
     * 
     */
    protected HashMap<Integer, Region> regionIdToRegionMap;
    /**
     * 
     */
    protected ToponymToRegionIDsMap toponymToRegionIDsMap;

    /**
     *
     * @param _converterExperimentParameters
     */
    public InternalToXMLConverter(
          ConverterExperimentParameters _converterExperimentParameters) {
        converterExperimentParameters = _converterExperimentParameters;

        pathToInput = converterExperimentParameters.getInputPath();
        pathToOutput = converterExperimentParameters.getOutputPath();
    }

    /**
     * 
     */
    public void convert() {
        convert(pathToInput);
    }

    public void convert(String TRXMLPath) {
        /**
         * initialize various fields
         */
        regionIdToRegionMap = new HashMap<Integer, Region>();

        /**
         * Read in binary data
         */
        InputReader inputReader = new BinaryInputReader(converterExperimentParameters);

        lexicon = inputReader.readLexicon();
        regionMatrix = inputReader.readRegions();

        for (Region[] regions : regionMatrix) {
            for (Region region : regions) {
                if (region != null) {
                    regionIdToRegionMap.put(region.id, region);
                }
            }
        }

        /**
         * Read in processed tokens
         */
        ArrayList<Integer> wordArray = new ArrayList<Integer>(),
              docArray = new ArrayList<Integer>(),
              toponymArray = new ArrayList<Integer>(),
              stopwordArray = new ArrayList<Integer>(),
              regionArray = new ArrayList<Integer>();

        try {
            while (true) {
                int[] record = inputReader.nextTokenArrayRecord();
                if (record != null) {
                    int wordid = record[0];
                    wordArray.add(wordid);
                    int docid = record[1];
                    docArray.add(docid);
                    int topstatus = record[2];
                    toponymArray.add(topstatus);
                    int stopstatus = record[3];
                    stopwordArray.add(stopstatus);
                    int regid = record[4];
                    regionArray.add(regid);
                }
            }
        } catch (EOFException ex) {
        } catch (IOException ex) {
            Logger.getLogger(InternalToXMLConverter.class.getName()).log(Level.SEVERE, null, ex);
        }

        /**
         * read in xml
         */
        File TRXMLPathFile = new File(TRXMLPath);

        SAXBuilder builder = new SAXBuilder();
        Document indoc = null;
        Document outdoc = new Document();
        try {
            indoc = builder.build(TRXMLPathFile);
        } catch (JDOMException ex) {
            Logger.getLogger(InternalToXMLConverter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(InternalToXMLConverter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }

        Element inroot = indoc.getRootElement();
        Element outroot = new Element(inroot.getName());
        outdoc.addContent(outroot);

        int counter = 0;
        int docid = 0;

        ArrayList<Element> documents = new ArrayList<Element>(inroot.getChildren());
        for (Element document : documents) {
            Element outdocument = new Element(document.getName());
            copyAttributes(document, outdocument);
            outroot.addContent(outdocument);

            ArrayList<Element> sentences = new ArrayList<Element>(document.getChildren());
            for (Element sentence : sentences) {

                Element outsentence = new Element(sentence.getName());
                copyAttributes(sentence, outsentence);
                outdocument.addContent(outsentence);

                ArrayList<Element> tokens = new ArrayList<Element>(sentence.getChildren());
                for (Element token : tokens) {

                    Element outtoken = new Element(token.getName());
                    copyAttributes(token, outtoken);
                    outsentence.addContent(outtoken);

                    int isstopword = stopwordArray.get(counter);
                    int regid = regionArray.get(counter);
                    int wordid = wordArray.get(counter);
                    String word = "";
                    if (token.getName().equals("w")) {
                        word = token.getAttributeValue("tok").toLowerCase();
                        if (isstopword == 0) {
                            Region reg = regionIdToRegionMap.get(regid);
                            outtoken.setAttribute("long", String.format("%.2f", reg.centLon));
                            outtoken.setAttribute("lat", String.format("%.2f", reg.centLat));
                        }
                        counter += 1;
                    } else if (token.getName().equals("toponym")) {
                        word = token.getAttributeValue("term").toLowerCase();
                        ArrayList<Element> candidates = new ArrayList<Element>(token.getChild("candidates").getChildren());
                        if (!candidates.isEmpty()) {
                            Coordinate coord = matchCandidate(candidates, regid);
                            outtoken.setAttribute("long", String.format("%.2f", coord.longitude));
                            outtoken.setAttribute("lat", String.format("%.2f", coord.latitude));
                        }
                        counter += 1;
                    } else {
                        continue;
                    }

                    String outword = lexicon.getWordForInt(wordid);
                    if (!word.equals(outword)) {
                        String did = document.getAttributeValue("id");
                        String sid = sentence.getAttributeValue("id");
                        int outdocid = docArray.get(counter);
                        System.err.println(String.format("Mismatch between "
                              + "tokens. Occurred at source document %s, "
                              + "sentence %s, token %s and target document %d, "
                              + "offset %d, token %s, token id %d",
                              did, sid, word, outdocid, counter, outword, wordid));
                        System.exit(1);
                    }
                }
            }
            docid += 1;
        }

        try {
            XMLOutputter xout = new XMLOutputter(Format.getPrettyFormat());
            xout.output(outdoc, new FileOutputStream(new File(pathToOutput)));
        } catch (IOException ex) {
            Logger.getLogger(InternalToXMLConverter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    protected Coordinate matchCandidate(ArrayList<Element> _candidates,
          int _regionid) {
        Region candregion = regionIdToRegionMap.get(_regionid);
        Coordinate candcoord = new Coordinate(candregion.centLon, candregion.centLat);

        for (Element candidate : _candidates) {
            double lon = Double.parseDouble(candidate.getAttributeValue("long"));
            double lat = Double.parseDouble(candidate.getAttributeValue("lat"));

            if (lon <= candregion.maxLon && lon >= candregion.minLon) {
                if (lat <= candregion.maxLat && lat >= candregion.minLat) {
                    candcoord = new Coordinate(lon, lat);
                }
            }
        }

        return candcoord;
    }

    /**
     * Create two normalized probability tables, {@link #TopWordsPerTopic} and
     * {@link #topicProbs}. {@link #topicProbs} overwrites previous values.
     * {@link #TopWordsPerTopic} only retains first {@link #outputPerTopic}
     * words and values.
     */
    public void normalizeWordByRegion() {

        wordByRegionProbs = new double[W * R];

        topWordsPerRegion = new IntDoublePair[R][];
        for (int i = 0; i < R; ++i) {
            topWordsPerRegion[i] = new IntDoublePair[outputPerClass];
        }

        regionProbs = new double[R];

        Double sum = 0.;
        for (int i = 0; i < R; ++i) {
            sum += regionProbs[i] = normalizedRegionCounts[i] + betaW;
            ArrayList<IntDoublePair> topWords = new ArrayList<IntDoublePair>();
            for (int j = 0; j < W; ++j) {
                topWords.add(new IntDoublePair(j, normalizedWordByRegionCounts[j * R + i] + beta));
                wordByRegionProbs[j * R + i] = (normalizedWordByRegionCounts[j * R + i] + beta) / regionProbs[i];
            }
            Collections.sort(topWords);
            int j = 0;
            try {
                for (; j < outputPerClass; ++j) {
                    topWordsPerRegion[i][j] =
                          new IntDoublePair(topWords.get(j).wordid, topWords.get(j).count / regionProbs[i]);
                }
            } catch (IndexOutOfBoundsException e) {
                for (; j < outputPerClass; ++j) {
                    topWordsPerRegion[i][j] = new IntDoublePair(-1, 0);
                }
            }
        }

        for (int i = 0; i < R; ++i) {
            regionProbs[i] /= sum;
        }
    }

    /**
     * Print the normalized sample counts for each topic to out. Print only the top {@link
     * #outputPerTopic} per given topic.
     *
     * @param out
     * @throws IOException
     */
    protected void printTopics(BufferedWriter out) throws IOException {
        int startt = 0, M = 4, endt = Math.min(M + startt, topicProbs.length);
        out.write("***** Word Probabilities by Topic *****\n\n");
        while (startt < T) {
            for (int i = startt; i < endt; ++i) {
                String header = "T_" + i;
                header = String.format("%25s\t%6.5f\t", header, topicProbs[i]);
                out.write(header);
            }

            out.newLine();
            out.newLine();

            for (int i = 0;
                  i < outputPerClass; ++i) {
                for (int c = startt; c < endt; ++c) {
                    String line = String.format("%25s\t%6.5f\t",
                          topWordsPerTopic[c][i].stringValue,
                          topWordsPerTopic[c][i].doubleValue);
                    out.write(line);
                }
                out.newLine();
            }
            out.newLine();
            out.newLine();

            startt = endt;
            endt = java.lang.Math.min(T, startt + M);
        }
    }

    protected void copyAttributes(Element src, Element trg) {
        for (Attribute attr : new ArrayList<Attribute>(src.getAttributes())) {
            trg.setAttribute(attr.getName(), attr.getValue());
        }
    }
}
