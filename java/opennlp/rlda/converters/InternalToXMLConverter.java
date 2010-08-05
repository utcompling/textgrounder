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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import opennlp.rlda.apps.ConverterExperimentParameters;
import opennlp.rlda.textstructs.*;
import opennlp.rlda.topostructs.*;
import opennlp.rlda.wrapper.io.*;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

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

        degreesPerRegion = converterExperimentParameters.getDegreesPerRegion();
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

        inputReader.readLexicon(lexicon);
        inputReader.readRegions(regionMatrix);

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
            outdocument.setAttributes(document.getAttributes());
            outroot.addContent(outdocument);

            ArrayList<Element> sentences = new ArrayList<Element>(document.getChildren());
            for (Element sentence : sentences) {

                Element outsentence = new Element(sentence.getName());
                outsentence.setAttributes(sentence.getAttributes());
                outdocument.addContent(outsentence);

                ArrayList<Element> tokens = new ArrayList<Element>(sentence.getChildren());
                for (Element token : tokens) {

                    Element outtoken = new Element(token.getName());
                    outtoken.setAttributes(token.getAttributes());
                    outsentence.addContent(outtoken);

                    int istoponym = toponymArray.get(counter);
                    int isstopword = stopwordArray.get(counter);
                    int wordid = 0;
                    String word = "";
                    if (token.getName().equals("w")) {
                        word = token.getAttributeValue("tok").toLowerCase();
                        wordid = lexicon.addOrGetWord(word);

                        counter += 1;
                    } else if (token.getName().equals("toponym")) {
                        word = token.getAttributeValue("term").toLowerCase();
                        istoponym = 1;
                        wordid = lexicon.addOrGetWord(word);
                        ArrayList<Element> candidates = new ArrayList<Element>(token.getChild("candidates").getChildren());
                        if (!candidates.isEmpty()) {
                            for (Element candidate : candidates) {
                                double lon = Double.parseDouble(candidate.getAttributeValue("long"));
                                double lat = Double.parseDouble(candidate.getAttributeValue("lat"));
                                Location loc = new Location(wordid, new Coordinate(lon, lat));

                                if (!toponymToRegionIDsMap.containsKey(wordid)) {
                                    toponymToRegionIDsMap.put(wordid, new HashSet<Integer>());
                                }

                                toponymToRegionIDsMap.get(wordid).add(regid);
                            }
                        } else {
                        }

                        counter += 1;
                    } else {
                        continue;
                    }
                }
            }
            docid += 1;
        }
    }
}
