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
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import opennlp.textgrounder.bayesian.apps.ConverterExperimentParameters;
import opennlp.textgrounder.bayesian.textstructs.*;
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
public abstract class InternalToXMLConverter implements ConverterInterface {

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

    public abstract void initialize();

    protected abstract void setTokenAttribute(Element _token, int _wordid, int _regid, int _coordid);

    protected abstract void setToponymAttribute(ArrayList<Element> _candidates, Element _token, int _wordid, int _regid, int _coordid, int _offset);

    public void convert(String TRXMLPath) {

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




//            XMLInputFactory factory = XMLInputFactory.newInstance();
//            XMLStreamReader xmlStreamReader = null;
//            xmlStreamReader = factory.createXMLStreamReader(new BufferedReader(new FileReader(new File(TRXMLPath))));
//            while (xmlStreamReader.hasNext() && xmlStreamReader.next() != XMLStreamReader.START_ELEMENT) {
//            }
//
//            xmlStreamReader.next();
//
//
//
//            Element inroot = indoc.getRootElement();
//            Element outroot = new Element(inroot.getName());
//            outdoc.addContent(outroot);
//
//            int counter = 0;
//            int docid = 0;
//
//            ArrayList<Element> documents = new ArrayList<Element>(inroot.getChildren());
//            for (Element document : documents) {
//                Element outdocument = new Element(document.getName());
//                copyAttributes(document, outdocument);
//                outroot.addContent(outdocument);
//
//                ArrayList<Element> sentences = new ArrayList<Element>(document.getChildren());
//                for (Element sentence : sentences) {
//
//                    Element outsentence = new Element(sentence.getName());
//                    copyAttributes(sentence, outsentence);
//                    outdocument.addContent(outsentence);
//
//                    ArrayList<Element> tokens = new ArrayList<Element>(sentence.getChildren());
//                    for (Element token : tokens) {
//
//                        Element outtoken = new Element(token.getName());
//                        copyAttributes(token, outtoken);
//                        outsentence.addContent(outtoken);
//
//                        int isstopword = stopwordArray.get(counter);
//                        int regid = regionArray.get(counter);
//                        int wordid = wordArray.get(counter);
//                        String word = "";
//                        if (token.getName().equals("w")) {
//                            word = token.getAttributeValue("tok").toLowerCase();
//                            if (isstopword == 0) {
//                                setTokenAttribute(outtoken, wordid, regid, 0);
//                            }
//                            counter += 1;
//                        } else if (token.getName().equals("toponym")) {
//                            word = token.getAttributeValue("term").toLowerCase();
//                            ArrayList<Element> candidates = new ArrayList<Element>(token.getChild("candidates").getChildren());
//                            setToponymAttribute(candidates, outtoken, wordid, regid, 0, counter);
//                            counter += 1;
//                        } else {
//                            continue;
//                        }
//
//                        String outword = lexicon.getWordForInt(wordid);
//                        if (!word.equals(outword)) {
//                            String did = document.getAttributeValue("id");
//                            String sid = sentence.getAttributeValue("id");
//                            int outdocid = docArray.get(counter);
//                            System.err.println(String.format("Mismatch between "
//                                  + "tokens. Occurred at source document %s, "
//                                  + "sentence %s, token %s and target document %d, "
//                                  + "offset %d, token %s, token id %d",
//                                  did, sid, word, outdocid, counter, outword, wordid));
//                            System.exit(1);
//                        }
//                    }
//                }
//                docid += 1;
//            }
//
//        } catch (XMLStreamException ex) {
//            Logger.getLogger(InternalToXMLConverter.class.getName()).log(Level.SEVERE, null, ex);
//            System.exit(1);
//        } catch (FileNotFoundException ex) {
//            Logger.getLogger(InternalToXMLConverter.class.getName()).log(Level.SEVERE, null, ex);
//            System.exit(1);
//        }
//
//        try {
//            XMLOutputter xout = new XMLOutputter(Format.getPrettyFormat());
//            xout.output(outdoc, new FileOutputStream(new File(pathToOutput)));
//        } catch (IOException ex) {
//            Logger.getLogger(InternalToXMLConverter.class.getName()).log(Level.SEVERE, null, ex);
//            System.exit(1);
//        }
    }

    protected void copyAttributes(Element src, Element trg) {
        for (Attribute attr : new ArrayList<Attribute>(src.getAttributes())) {
            trg.setAttribute(attr.getName(), attr.getValue());
        }
    }
}
