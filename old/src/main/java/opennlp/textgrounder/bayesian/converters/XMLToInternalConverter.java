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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

import opennlp.textgrounder.bayesian.apps.ConverterExperimentParameters;
import opennlp.textgrounder.bayesian.textstructs.*;
import opennlp.textgrounder.bayesian.topostructs.*;
import opennlp.textgrounder.text.Sentence;
import opennlp.textgrounder.text.Token;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public abstract class XMLToInternalConverter implements XMLToInternalConverterInterface {

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
    protected int docid = 0;
    /**
     *
     */
    protected ConverterExperimentParameters converterExperimentParameters;

    /**
     * 
     * @param _path
     */
    public XMLToInternalConverter(String _path) {
        pathToInput = _path;
        lexicon = new Lexicon();
        tokenArrayBuffer = new TokenArrayBuffer(lexicon);
        stopwordList = new StopwordList();
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
        try {
            docid = 0;
            XMLToInternalSource<XMLToInternalConverter> xmlSource = new XMLToInternalSource<XMLToInternalConverter>(new BufferedReader(new FileReader(new File(TRXMLPath))), this);
            while (xmlSource.hasNext()) {
                Iterator<Sentence<Token>> sentit = xmlSource.next().iterator();
                while (sentit.hasNext()) {
                    sentit.next();
                }
                xmlSource.nextTag();
                docid += 1;
            }
            xmlSource.close();
        } catch (XMLStreamException ex) {
            Logger.getLogger(InternalToXMLConverter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(InternalToXMLConverter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * 
     * @param TRXMLPath
     */
    protected void preprocess(String TRXMLPath) {

        try {
            Preprocessor preprocessor = new Preprocessor();
            XMLToInternalSource<Preprocessor> xmlSource = new XMLToInternalSource<Preprocessor>(new BufferedReader(new FileReader(new File(TRXMLPath))), preprocessor);
            while (xmlSource.hasNext()) {
                Iterator<Sentence<Token>> sentit = xmlSource.next().iterator();
                while (sentit.hasNext()) {
                    sentit.next();
                }
                xmlSource.nextTag();
            }
            xmlSource.close();
            preprocessor.wrapUp();
        } catch (XMLStreamException ex) {
            Logger.getLogger(InternalToXMLConverter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(InternalToXMLConverter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * 
     */
    public abstract void writeToFiles();

    protected abstract void addToTopoStructs(int _wordid, Coordinate _coord);

    protected abstract void initializeRegionArray();

    public class Preprocessor implements XMLToInternalConverterInterface {

        /**
         *
         */
        Pattern wordPattern = Pattern.compile("[a-z]+");
        HashMap<String, Integer> countLexicon = new HashMap<String, Integer>();
        HashSet<String> toponymSet = new HashSet<String>();
        boolean coordAdded = false;

        public void wrapUp() {
            for (String word : toponymSet) {
                lexicon.addOrGetWord(word);
            }
            validtoponyms = lexicon.getDictionarySize();

            for (Map.Entry<String, Integer> entry : countLexicon.entrySet()) {
                if (entry.getValue() > countCutoff) {
                    lexicon.addOrGetWord(entry.getKey());
                }
            }

            validwords = lexicon.getDictionarySize();
        }

        @Override
        public void addToken(String _string) {
            Matcher matcher = wordPattern.matcher(_string);
            boolean found = matcher.find();

            if (!stopwordList.isStopWord(_string) && found) {
                if (countLexicon.containsKey(_string)) {
                    int count = countLexicon.get(_string) + 1;
                    countLexicon.put(_string, count);
                } else {
                    countLexicon.put(_string, 1);
                }
            }
        }

        @Override
        public void addToponym(String _string) {
            if (coordAdded) {
                if (countLexicon.containsKey(_string)) {
                    int count = countLexicon.get(_string) + 1;
                    countLexicon.put(_string, count);
                } else {
                    countLexicon.put(_string, 1);
                }
                toponymSet.add(_string);
            }
            coordAdded = false;
        }

        @Override
        public void addCoordinate(double _long, double _lat) {
            coordAdded = true;
        }

        @Override
        public void addCandidate(double _long, double _lat) {
            return;
        }

        @Override
        public void addRepresentative(double _long, double _lat) {
            return;
        }

        @Override
        public void setCurrentToponym(String _string) {
            return;
        }
    }
}
