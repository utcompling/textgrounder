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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import opennlp.rlda.apps.ConverterExperimentParameters;
import opennlp.rlda.converters.callbacks.*;
import opennlp.rlda.wrapper.io.OutputWriter;
import opennlp.rlda.wrapper.io.TextOutputWriter;
import opennlp.rlda.textstructs.*;
import opennlp.rlda.topostructs.*;
import opennlp.rlda.wrapper.io.BinaryOutputWriter;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class Converter {

    protected String pathToInput;
    protected TokenArrayBuffer tokenArrayBuffer;
    protected Lexicon lexicon;
    protected StopwordList stopwordList;
    protected TrainingMaterialCallback trainingMaterialCallback;
    protected int activeRegions;
    protected int degreesPerRegion;
    protected int countCutoff = -1;
    protected ConverterExperimentParameters converterExperimentParameters;
    protected Region[][] regionArray;
    protected ToponymToRegionIDsMap toponymToRegionIDsMap;

    public Converter(String _path) {
        pathToInput = _path;
        lexicon = new Lexicon();
        tokenArrayBuffer = new TokenArrayBuffer(lexicon);
        stopwordList = new StopwordList();
        trainingMaterialCallback = new TrainingMaterialCallback(lexicon);
    }

    public Converter(
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
    protected Region addOrGetLocationsToRegionArray(Location loc) {
        int curX = (int) (loc.coord.latitude + 180) / (int) degreesPerRegion;
        int curY = (int) (loc.coord.longitude + 90) / (int) degreesPerRegion;

        if (regionArray[curX][curY] == null) {
            double minLon = loc.coord.longitude - loc.coord.longitude % degreesPerRegion;
            double maxLon = minLon + (loc.coord.longitude < 0 ? -1 : 1) * degreesPerRegion;
            double minLat = loc.coord.latitude - loc.coord.latitude % degreesPerRegion;
            double maxLat = minLat + (loc.coord.latitude < 0 ? -1 : 1) * degreesPerRegion;
            activeRegions += 1;
            regionArray[curX][curY] = new Region(activeRegions, minLon, maxLon, minLat, maxLat);
        }
        return regionArray[curX][curY];
    }

    public void convert() {
        initializeRegionArray();
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
            Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, ex);
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
                        word = token.getAttributeValue("tok");
                        wordid = lexicon.addOrGetWord(word);
                        isstopword = stopwordList.isStopWord(word) ? 1 : 0;
                    } else if (token.getName().equals("toponym")) {
                        word = token.getAttributeValue("term");
                        istoponym = 1;
                        wordid = lexicon.addOrGetWord(word);
                        ArrayList<Element> candidates = new ArrayList<Element>(token.getChildren());
                        if (candidates.isEmpty()) {
                            for (Element candidate : candidates) {
                                double lon = Double.parseDouble(candidate.getAttributeValue("long"));
                                double lat = Double.parseDouble(candidate.getAttributeValue("lat"));
                                Location loc = new Location(wordid, new Coordinate(lon, lat));

                                Region region = addOrGetLocationsToRegionArray(loc);

                                int regid = region.id;

                                if (!toponymToRegionIDsMap.containsKey(wordid)) {
                                    toponymToRegionIDsMap.put(wordid, new HashSet<Integer>());
                                }

                                toponymToRegionIDsMap.get(wordid).add(regid);
                            }
                        } else {
                            istoponym = 0;
                            isstopword = stopwordList.isStopWord(word) ? 1 : 0;
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

    protected void preprocess(String TRXMLPath) {
        HashMap<String, Integer> countLexicon = new HashMap<String, Integer>();

        File TRXMLPathFile = new File(TRXMLPath);

        SAXBuilder builder = new SAXBuilder();
        Document trdoc = null;
        try {
            trdoc = builder.build(TRXMLPathFile);
        } catch (JDOMException ex) {
            Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, ex);
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
                    if (token.getName().equals("w")) {
                        word = token.getAttributeValue("tok");
                    } else if (token.getName().equals("toponym")) {
                        word = token.getAttributeValue("term");
                    } else {
                        continue;
                    }

                    if (stopwordList.isStopWord(word)) {
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
        }

        outputWriter.writeTokenArrayWriter(tokenArrayBuffer);
        outputWriter.writeToponymRegionWriter(toponymToRegionIDsMap);
    }

    public void loadParameters(String _filename) {
        ObjectInputStream lexiconIn = null;
        try {
            lexiconIn = new ObjectInputStream(new GZIPInputStream(new FileInputStream(_filename)));
            lexicon = (Lexicon) lexiconIn.readObject();
        } catch (IOException ex) {
            Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    public void saveParameters(String _filename) {

        if (!_filename.endsWith(".gz")) {
            _filename += ".gz";
        }
        ObjectOutputStream modelOut = null;
        try {
            modelOut = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(_filename)));
            modelOut.writeObject(this);
            modelOut.close();
        } catch (IOException ex) {
            Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }
}
