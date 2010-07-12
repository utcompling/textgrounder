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
import java.util.logging.Level;
import java.util.logging.Logger;
import opennlp.wrapper.rlda.converters.callbacks.TrainingMaterialCallback;
import opennlp.wrapper.rlda.textstructs.Lexicon;
import opennlp.wrapper.rlda.textstructs.StopwordList;
import opennlp.wrapper.rlda.textstructs.TokenArrayBuffer;
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

    public Converter(String _path) {
        pathToInput = _path;
        lexicon = new Lexicon();
        tokenArrayBuffer = new TokenArrayBuffer(lexicon);
        stopwordList = new StopwordList();
        trainingMaterialCallback = new TrainingMaterialCallback(lexicon);
    }

    public void convert() {
        convert(pathToInput);
    }

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

        int docidx = 0;
        Element root = trdoc.getRootElement();
        ArrayList<Element> documents = new ArrayList<Element>(root.getChildren());
        for (Element document : documents) {
            ArrayList<Element> sentences = new ArrayList<Element>(document.getChildren());
            for (Element sentence : sentences) {
                ArrayList<Element> tokens = new ArrayList<Element>(sentence.getChildren());
                for (Element token : tokens) {
                    int istoponym = 0, isstopword = 0;
                    int wordidx = 0;
                    String word = "";
                    if (token.getName().equals("w")) {
                        word = token.getAttributeValue("tok");
                        wordidx = lexicon.addOrGetWord(word);
                        tokenArrayBuffer.addElement(wordidx, docidx, istoponym, stopwordList.isStopWord(word)
                              ? 1 : 0);
                    } else if (token.getName().equals("toponym")) {
                        word = token.getAttributeValue("term");
                        istoponym = 1;
                        wordidx = lexicon.addOrGetWord(word);
                        tokenArrayBuffer.addElement(wordidx, docidx, istoponym, 0);
                        ArrayList<Element> candidates = new ArrayList<Element>(token.getChildren());
                        for (Element candidate : candidates) {
                            
                        }
                    } else {
                        continue;
                    }
                }
            }
            docidx += 1;
        }
    }
}
