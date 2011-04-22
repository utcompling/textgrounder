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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.stream.XMLStreamException;

import opennlp.textgrounder.bayesian.apps.ConverterExperimentParameters;
import opennlp.textgrounder.bayesian.textstructs.*;
import opennlp.textgrounder.text.Sentence;
import opennlp.textgrounder.text.Token;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public abstract class InternalToXMLConverter implements InternalToXMLConverterInterface {

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
    protected ConverterExperimentParameters converterExperimentParameters;
    /**
     * 
     */
    protected ArrayList<Integer> wordArray;
    protected ArrayList<Integer> docArray;
    protected ArrayList<Integer> toponymArray;
    protected ArrayList<Integer> stopwordArray;
    protected ArrayList<Integer> regionArray;
    /**
     *
     */
    protected String currentDocumentID;
    /**
     *
     */
    protected String currentSentenceID;
    /**
     *
     */
    protected String currentWord;
    /**
     *
     */
    protected int currentWordID;
    /**
     *
     */
    protected int offset = 0;
    /**
     * 
     */
    protected boolean candidateSelected = false;
    /**
     *
     */
    protected boolean needToSelectCandidates = false;

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
        convert(pathToInput, pathToOutput);
    }

    public abstract void initialize();

    public void convert(String XMLInputPath, String XMLOutputPath) {

        /**
         * read in xml
         */
        try {
            InternalToXMLSource<InternalToXMLConverter> xmlSource =
                  new InternalToXMLSource<InternalToXMLConverter>(new BufferedReader(new FileReader(new File(XMLInputPath))),
                  new BufferedWriter(new FileWriter(new File(XMLOutputPath))), this);
            while (xmlSource.hasNext()) {
                Iterator<Sentence<Token>> sentit = xmlSource.next().iterator();
                while (sentit.hasNext()) {
                    sentit.next();
                }
                xmlSource.writeEndElement();
                xmlSource.nextTag();
            }
            xmlSource.close();
        } catch (IOException ex) {
            Logger.getLogger(InternalToXMLConverter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (XMLStreamException ex) {
            Logger.getLogger(InternalToXMLConverter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void incrementOffset() {
        if (needToSelectCandidates) {
            int wordid = wordArray.get(offset);
            String word = lexicon.getWordForInt(wordid);

            System.err.println("A candidate has not been selected!");
            System.err.println("This occurred with the word: " + word);
            System.err.println("At offset: " + offset);
            needToSelectCandidates = false;
//            System.err.println("Terminating prematurely");
//            System.exit(1);
        }
        offset += 1;
    }

    @Override
    public void setCurrentWord(String _string) {
        currentWord = _string;
        currentWordID = lexicon.addOrGetWord(_string);
    }
}
