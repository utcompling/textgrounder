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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import opennlp.textgrounder.bayesian.apps.ConverterExperimentParameters;
import opennlp.textgrounder.bayesian.converters.callbacks.*;
import opennlp.textgrounder.bayesian.textstructs.*;
import opennlp.textgrounder.bayesian.topostructs.*;
import opennlp.textgrounder.text.Sentence;
import opennlp.textgrounder.text.Token;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public abstract class XMLToInternalConverter implements ConverterInterface {

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
    protected int countCutoff = -1;
    /**
     *
     */
    protected int validwords = 0;
    /**
     * 
     */
    protected int validtoponyms = 0;
    /**
     *
     */
    protected ConverterExperimentParameters converterExperimentParameters;
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

        countCutoff = converterExperimentParameters.getCountCutoff();
    }

    /**
     * 
     */
    public void convert() {
        initializeRegionArray();
        preprocess(pathToInput);
        try {
            convert(pathToInput);
        } catch (XMLStreamException ex) {
            Logger.getLogger(XMLToInternalConverter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(XMLToInternalConverter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    /**
     * 
     * @param TRXMLPath
     */
    public void convert(String TRXMLPath) throws XMLStreamException, FileNotFoundException {

        /**
         * read in xml
         */
        XMLSource xmlSource;
        try {
            xmlSource = new XMLSource(new BufferedReader(new FileReader(new File(TRXMLPath))), this);
            while (xmlSource.hasNext()) {
            }
            xmlSource.close();
        } catch (XMLStreamException ex) {
            Logger.getLogger(InternalToXMLConverter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(InternalToXMLConverter.class.getName()).log(Level.SEVERE, null, ex);
        }

//        XMLInputFactory factory = XMLInputFactory.newInstance();
//        XMLStreamReader xmlStreamReader = null;
//        xmlStreamReader = factory.createXMLStreamReader(new BufferedReader(new FileReader(new File(TRXMLPath))));
//        while (xmlStreamReader.hasNext() && xmlStreamReader.next() != XMLStreamReader.START_ELEMENT) {
//        }
//
//        xmlStreamReader.next();
//
//        File TRXMLPathFile = new File(TRXMLPath);
//
//        SAXBuilder builder = new SAXBuilder();
//        Document trdoc = null;
//        try {
//            trdoc = builder.build(TRXMLPathFile);
//        } catch (JDOMException ex) {
//            Logger.getLogger(XMLToInternalConverter.class.getName()).log(Level.SEVERE, null, ex);
//            System.exit(1);
//        } catch (IOException ex) {
//            Logger.getLogger(XMLToInternalConverter.class.getName()).log(Level.SEVERE, null, ex);
//            System.exit(1);
//        }
//
//        int docid = 0;
//        Element root = trdoc.getRootElement();
//        ArrayList<Element> documents = new ArrayList<Element>(root.getChildren());
//        for (Element document : documents) {
//            ArrayList<Element> sentences = new ArrayList<Element>(document.getChildren());
//            for (Element sentence : sentences) {
//                ArrayList<Element> tokens = new ArrayList<Element>(sentence.getChildren());
//                for (Element token : tokens) {
//                    int istoponym = 0, isstopword = 0;
//                    int wordid = 0;
//                    String word = "";
//                    if (token.getName().equals("w")) {
//                        word = token.getAttributeValue("tok").toLowerCase();
//                        wordid = lexicon.addOrGetWord(word);
//                        isstopword = (stopwordList.isStopWord(word) ? 1 : 0) | (wordid >= validwords
//                              ? 1 : 0);
//                    } else if (token.getName().equals("toponym")) {
//                        word = token.getAttributeValue("term").toLowerCase();
//                        istoponym = 1;
//                        wordid = lexicon.addOrGetWord(word);
//                        ArrayList<Element> candidates = new ArrayList<Element>(token.getChild("candidates").getChildren());
//                        if (!candidates.isEmpty()) {
//                            for (Element candidate : candidates) {
//                                double lon = Double.parseDouble(candidate.getAttributeValue("long"));
//                                double lat = Double.parseDouble(candidate.getAttributeValue("lat"));
//                                Coordinate coord = new Coordinate(lon, lat);
//
//                                addToTopoStructs(wordid, coord);
//                            }
//                        } else {
//                            istoponym = 0;
//                            isstopword = (stopwordList.isStopWord(word) ? 1 : 0) | (wordid >= validwords
//                                  ? 1 : 0);
//                        }
//                    } else {
//                        continue;
//                    }
//                    tokenArrayBuffer.addElement(wordid, docid, istoponym, isstopword);
//                }
//            }
//            docid += 1;
//        }
    }

    /**
     * 
     * @param TRXMLPath
     */
    protected void preprocess(String TRXMLPath) {

        XMLSource<XMLToInternalConverter> xmlSource;
        try {
            xmlSource = new XMLSource<XMLToInternalConverter>(new BufferedReader(new FileReader(new File(TRXMLPath))), this);
            while (xmlSource.hasNext()) {
                opennlp.textgrounder.text.Document<Token> doc = xmlSource.next();
                Iterator<Sentence<Token>> t = doc.iterator();
                while (t.hasNext()) {
                    Sentence<Token> s = t.next();
                    Iterator<Token> it = s.iterator();
                    while(it.hasNext()) {
                        it.next();
                    }
                }
            }
            xmlSource.close();
        } catch (XMLStreamException ex) {
            Logger.getLogger(InternalToXMLConverter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(InternalToXMLConverter.class.getName()).log(Level.SEVERE, null, ex);
        }


//        HashMap<String, Integer> countLexicon = new HashMap<String, Integer>();
//        HashSet<String> toponymSet = new HashSet<String>();
//
//        File TRXMLPathFile = new File(TRXMLPath);
//
//        SAXBuilder builder = new SAXBuilder();
//        Document trdoc = null;
//        try {
//            trdoc = builder.build(TRXMLPathFile);
//        } catch (JDOMException ex) {
//            Logger.getLogger(XMLToInternalConverter.class.getName()).log(Level.SEVERE, null, ex);
//            System.exit(1);
//        } catch (IOException ex) {
//            Logger.getLogger(XMLToInternalConverter.class.getName()).log(Level.SEVERE, null, ex);
//            System.exit(1);
//        }
//
//        Element root = trdoc.getRootElement();
//        ArrayList<Element> documents = new ArrayList<Element>(root.getChildren());
//        for (Element document : documents) {
//            ArrayList<Element> sentences = new ArrayList<Element>(document.getChildren());
//            for (Element sentence : sentences) {
//                ArrayList<Element> tokens = new ArrayList<Element>(sentence.getChildren());
//                for (Element token : tokens) {
//                    String word = "";
//                    boolean istoponym = false;
//                    if (token.getName().equals("w")) {
//                        word = token.getAttributeValue("tok").toLowerCase();
//                    } else if (token.getName().equals("toponym")) {
//                        word = token.getAttributeValue("term").toLowerCase();
//                        ArrayList<Element> candidates = new ArrayList<Element>(token.getChild("candidates").getChildren());
//                        if (!candidates.isEmpty()) {
//                            toponymSet.add(word);
//                            istoponym = true;
//                        }
//                    } else {
//                        continue;
//                    }
//
//                    Matcher matcher = wordPattern.matcher(word);
//                    boolean found = matcher.find();
//                    if ((!stopwordList.isStopWord(word) || istoponym) && found) {
//                        if (countLexicon.containsKey(word)) {
//                            int count = countLexicon.get(word) + 1;
//                            countLexicon.put(word, count);
//                        } else {
//                            countLexicon.put(word, 1);
//                        }
//                    }
//                }
//            }
//        }
//
//        for (String word : toponymSet) {
//            lexicon.addOrGetWord(word);
//        }
//        validtoponyms = lexicon.getDictionarySize();
//
//        for (Map.Entry<String, Integer> entry : countLexicon.entrySet()) {
//            if (entry.getValue() > countCutoff) {
//                lexicon.addOrGetWord(entry.getKey());
//            }
//        }
//
//        validwords = lexicon.getDictionarySize();
    }

    /**
     * 
     */
    public abstract void writeToFiles();

    protected abstract void addToTopoStructs(int _wordid, Coordinate _coord);

    protected abstract void initializeRegionArray();
}
