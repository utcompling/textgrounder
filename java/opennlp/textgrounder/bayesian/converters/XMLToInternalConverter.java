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
package opennlp.textgrounder.bayesian.converters;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import opennlp.textgrounder.bayesian.apps.ConverterExperimentParameters;
import opennlp.textgrounder.bayesian.converters.callbacks.*;
import opennlp.textgrounder.bayesian.wrapper.io.*;
import opennlp.textgrounder.bayesian.textstructs.*;
import opennlp.textgrounder.bayesian.topostructs.*;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class XMLToInternalConverter {

    /**
     *
     */
    protected String pathToInput;
    /**
     *
     */
    protected TokenArrayBuffer tokenArrayBuffer;
    /**
     * 
     */
    protected Lexicon lexicon;
    /**
     *
     */
    protected StopwordList stopwordList;
    /**
     * 
     */
    protected TrainingMaterialCallback trainingMaterialCallback;
    /**
     *
     */
    protected int activeRegions;
    /**
     *
     */
    protected int degreesPerRegion;
    /**
     *
     */
    protected int countCutoff = -1;
    /**
     *
     */
    protected int maxvalidid = 0;
    /**
     *
     */
    protected ConverterExperimentParameters converterExperimentParameters;
    /**
     *
     */
    protected Region[][] regionArray;
    /**
     * 
     */
    protected ToponymToRegionIDsMap toponymToRegionIDsMap;
    /**
     *
     */
    protected ToponymToCoordinateMap toponymToCoordinateMap;
    /**
     * 
     */
    protected Pattern wordPattern = Pattern.compile("[a-z]+");

    /**
     * 
     * @param _path
     */
    public XMLToInternalConverter(String _path) {
        pathToInput = _path;
        lexicon = new Lexicon();
        tokenArrayBuffer = new TokenArrayBuffer(lexicon);
        stopwordList = new StopwordList();
        trainingMaterialCallback = new TrainingMaterialCallback(lexicon);
    }

    /**
     *
     * @param _converterExperimentParameters
     */
    public XMLToInternalConverter(
          ConverterExperimentParameters _converterExperimentParameters) {
        converterExperimentParameters = _converterExperimentParameters;

        pathToInput = converterExperimentParameters.getInputPath();

        lexicon = new Lexicon();
        tokenArrayBuffer = new TokenArrayBuffer(lexicon);
        stopwordList = new StopwordList();
        trainingMaterialCallback = new TrainingMaterialCallback(lexicon);

        degreesPerRegion = converterExperimentParameters.getDegreesPerRegion();
        countCutoff = converterExperimentParameters.getCountCutoff();

        toponymToRegionIDsMap = new ToponymToRegionIDsMap();
        toponymToCoordinateMap = new ToponymToCoordinateMap();
        activeRegions = 0;
    }

    /**
     * Initialize the region array to be of the right size and contain null
     * pointers.
     */
    public void initializeRegionArray() {
        activeRegions = 0;

        int regionArrayWidth = 360 / (int) degreesPerRegion;
        int regionArrayHeight = 180 / (int) degreesPerRegion;

        regionArray = new Region[regionArrayWidth][regionArrayHeight];

        for (int w = 0; w < regionArrayWidth; w++) {
            for (int h = 0; h < regionArrayHeight; h++) {
                regionArray[w][h] = null;
            }
        }
    }

    /**
     * Add a single location to the Region object in the region array that
     * corresponds to the latitude and longitude stored in the location object.
     * Create the Region object if necessary. 
     *
     * @param loc
     */
    protected Region getRegion(Coordinate coord) {
        int curX = ((int) Math.floor(coord.longitude + 180)) / degreesPerRegion;
        int curY = ((int) Math.floor(coord.latitude + 90)) / degreesPerRegion;

        if (regionArray[curX][curY] == null) {
            double minLon = coord.longitude - (coord.longitude + 180) % degreesPerRegion;
            double maxLon = minLon + degreesPerRegion;
            double minLat = coord.latitude - (coord.latitude + 90) % degreesPerRegion;
            double maxLat = minLat + degreesPerRegion;
            regionArray[curX][curY] = new Region(activeRegions, minLon, maxLon, minLat, maxLat);
            activeRegions += 1;
        }
        return regionArray[curX][curY];
    }

    /**
     * 
     */
    public void convert() {
        initializeRegionArray();
        preprocess(pathToInput);
        convert(pathToInput);
    }

    /**
     * 
     * @param TRXMLPath
     */
    public void convert(String TRXMLPath) {
        File TRXMLPathFile = new File(TRXMLPath);

        SAXBuilder builder = new SAXBuilder();
        Document trdoc = null;
        try {
            trdoc = builder.build(TRXMLPathFile);
        } catch (JDOMException ex) {
            Logger.getLogger(XMLToInternalConverter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(XMLToInternalConverter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }

        int docid = 0;
        Element root = trdoc.getRootElement();
        ArrayList<Element> documents = new ArrayList<Element>(root.getChildren());
        for (Element document : documents) {
            ArrayList<Element> sentences = new ArrayList<Element>(document.getChildren());
            for (Element sentence : sentences) {
                ArrayList<Element> tokens = new ArrayList<Element>(sentence.getChildren());
                for (Element token : tokens) {
                    int istoponym = 0, isstopword = 0;
                    int wordid = 0;
                    String word = "";
                    if (token.getName().equals("w")) {
                        word = token.getAttributeValue("tok").toLowerCase();
                        wordid = lexicon.addOrGetWord(word);
                        isstopword = (stopwordList.isStopWord(word) ? 1 : 0) | (wordid > maxvalidid
                              ? 1 : 0);
                    } else if (token.getName().equals("toponym")) {
                        word = token.getAttributeValue("term").toLowerCase();
                        istoponym = 1;
                        wordid = lexicon.addOrGetWord(word);
                        ArrayList<Element> candidates = new ArrayList<Element>(token.getChild("candidates").getChildren());
                        if (!candidates.isEmpty()) {
                            for (Element candidate : candidates) {
                                double lon = Double.parseDouble(candidate.getAttributeValue("long"));
                                double lat = Double.parseDouble(candidate.getAttributeValue("lat"));
                                Coordinate coord = new Coordinate(lon, lat);

                                Region region = getRegion(coord);

                                int regid = region.id;

                                if (!toponymToRegionIDsMap.containsKey(wordid)) {
                                    toponymToRegionIDsMap.put(wordid, new HashSet<Integer>());
                                    toponymToCoordinateMap.put(wordid, new HashSet<Coordinate>());
                                }

                                toponymToRegionIDsMap.get(wordid).add(regid);
                                toponymToCoordinateMap.get(wordid).add(coord);
                            }
                        } else {
                            istoponym = 0;
                            isstopword = (stopwordList.isStopWord(word) ? 1 : 0) | (wordid > maxvalidid
                                  ? 1 : 0);
                        }
                    } else {
                        continue;
                    }
                    tokenArrayBuffer.addElement(wordid, docid, istoponym, isstopword);
                }
            }
            docid += 1;
        }
    }

    /**
     * 
     * @param TRXMLPath
     */
    protected void preprocess(String TRXMLPath) {
        HashMap<String, Integer> countLexicon = new HashMap<String, Integer>();
        HashSet<String> toponymSet = new HashSet<String>();

        File TRXMLPathFile = new File(TRXMLPath);

        SAXBuilder builder = new SAXBuilder();
        Document trdoc = null;
        try {
            trdoc = builder.build(TRXMLPathFile);
        } catch (JDOMException ex) {
            Logger.getLogger(XMLToInternalConverter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(XMLToInternalConverter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }

        Element root = trdoc.getRootElement();
        ArrayList<Element> documents = new ArrayList<Element>(root.getChildren());
        for (Element document : documents) {
            ArrayList<Element> sentences = new ArrayList<Element>(document.getChildren());
            for (Element sentence : sentences) {
                ArrayList<Element> tokens = new ArrayList<Element>(sentence.getChildren());
                for (Element token : tokens) {
                    String word = "";
                    boolean istoponym = false;
                    if (token.getName().equals("w")) {
                        word = token.getAttributeValue("tok").toLowerCase();
                    } else if (token.getName().equals("toponym")) {
                        word = token.getAttributeValue("term").toLowerCase();
                        ArrayList<Element> candidates = new ArrayList<Element>(token.getChild("candidates").getChildren());
                        if (!candidates.isEmpty()) {
                            toponymSet.add(word);
                            istoponym = true;
                        }
                    } else {
                        continue;
                    }

                    Matcher matcher = wordPattern.matcher(word);
                    boolean found = matcher.find();
                    if ((!stopwordList.isStopWord(word) || istoponym) && found) {
                        if (countLexicon.containsKey(word)) {
                            int count = countLexicon.get(word) + 1;
                            countLexicon.put(word, count);
                        } else {
                            countLexicon.put(word, 1);
                        }
                    }
                }
            }
        }

        for (Map.Entry<String, Integer> entry : countLexicon.entrySet()) {
            if (entry.getValue() > countCutoff) {
                lexicon.addOrGetWord(entry.getKey());
            }
        }

        for (String word : toponymSet) {
            lexicon.addOrGetWord(word);
        }

        maxvalidid = lexicon.getDictionarySize() - 1;
    }

    /**
     * 
     */
    public void writeToFiles() {
        OutputWriter outputWriter = null;
        switch (converterExperimentParameters.getInputFormat()) {
            case TEXT:
                outputWriter = new TextOutputWriter(converterExperimentParameters);
                break;
            case BINARY:
                outputWriter = new BinaryOutputWriter(converterExperimentParameters);
                break;
        }

        outputWriter.writeTokenArray(tokenArrayBuffer);
        outputWriter.writeToponymRegion(toponymToRegionIDsMap);
        outputWriter.writeToponymCoordinate(toponymToCoordinateMap);
        outputWriter.writeLexicon(lexicon);
        outputWriter.writeRegions(regionArray);
    }
}
