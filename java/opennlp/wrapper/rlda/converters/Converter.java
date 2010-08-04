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
package opennlp.wrapper.rlda.converters;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import opennlp.wrapper.rlda.apps.ExperimentParameters;
import opennlp.wrapper.rlda.converters.callbacks.*;
import opennlp.wrapper.rlda.io.OutputWriter;
import opennlp.wrapper.rlda.io.TextOutputWriter;
import opennlp.wrapper.rlda.textstructs.*;
import opennlp.wrapper.rlda.topostructs.*;
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
    protected ExperimentParameters experimentParameters;
    protected Region[][] regionArray;
    protected ToponymToRegionIDsMap toponymToRegionIDsMap;

    public Converter(String _path) {
        pathToInput = _path;
        lexicon = new Lexicon();
        tokenArrayBuffer = new TokenArrayBuffer(lexicon);
        stopwordList = new StopwordList();
        trainingMaterialCallback = new TrainingMaterialCallback(lexicon);
    }

    public Converter(ExperimentParameters _experimentParameters) {
        experimentParameters = _experimentParameters;

        pathToInput = experimentParameters.getInputPath();

        lexicon = new Lexicon();
        tokenArrayBuffer = new TokenArrayBuffer(lexicon);
        stopwordList = new StopwordList();
        trainingMaterialCallback = new TrainingMaterialCallback(lexicon);

        degreesPerRegion = experimentParameters.getDegreesPerRegion();

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
     */
    public void writeToFiles() {
        OutputWriter outputWriter = new TextOutputWriter(experimentParameters);
        outputWriter.writeTokenArrayWriter(tokenArrayBuffer);
        outputWriter.writeToponymRegionWriter(toponymToRegionIDsMap);
    }
}
