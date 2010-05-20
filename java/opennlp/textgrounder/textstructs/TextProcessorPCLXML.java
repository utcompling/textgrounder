///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.textstructs;

import edu.stanford.nlp.ling.CoreAnnotations.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;
import org.jdom.xpath.XPath;

/**
 * 
 * @author 
 */
public class TextProcessorPCLXML extends TextProcessor {

    /**
     * Default constructor. Instantiate CRFClassifier.
     * 
     * @param lexicon lookup table for words and indexes
     */
    public TextProcessorPCLXML(Lexicon lexicon) throws
          ClassCastException, IOException, ClassNotFoundException {
        super(lexicon);
    }

    /**
     * Identify toponyms and populate lexicon from input file.
     * 
     * This method only splits any incoming document into smaller 
     * subdocuments based on Lexicon.parAsDocSize. The actual work of
     * identifying toponyms, converting tokens to indexes, and populating
     * arrays is handled in addToponymSpans.
     * 
     * @param locationOfFile path to input. must be a single file
     * @param lexicon the Lexicon instance that contains both the sequence
     * of token indexes and the lexicon.
     * @param tokenArrayBuffer buffer that holds the array of token indexes,
     * document indexes, and the toponym indexes. If this class object is
     * not needed from the calling class, then a NullTokenArrayBuffer is
     * instantiated and passed. Nothing happens inside this object.
     * @param stopwordList table that contains the list of stopwords. if
     * this is an instance of NullStopwordList, it will return false
     * through stopwordList.isStopWord for every string token examined
     * (i.e. the token is not a stopword).
     * @throws FileNotFoundException
     * @throws IOException
     */
    @Override
    public void addToponymsFromFile(String locationOfFile,
          TokenArrayBuffer tokenArrayBuffer, StopwordList stopwordList) throws
          FileNotFoundException, IOException {

        if (!locationOfFile.endsWith(".xml")) {
            return;
        }
        File file = new File(locationOfFile);
        SAXBuilder builder = new SAXBuilder();
        Document doc = null;
        try {
            doc = builder.build(file);
            Element element = doc.getRootElement();
            Element child = element.getChild("text").getChild("body");
            List<Element> divs = new ArrayList<Element>(child.getChildren("div"));

            for (Element div : divs) {
                StringBuffer buf = new StringBuffer();
                List<Element> pars = new ArrayList<Element>(div.getChildren("p"));
                for (Element par : pars) {
                    buf.append(par.getText());
                }
                String text = buf.toString().trim();
                if (!text.isEmpty()) {
                    addToponymSpans(text, tokenArrayBuffer, stopwordList);
                    currentDoc += 1;
                    System.err.print(currentDoc + ",");
                }
            }

        } catch (JDOMException ex) {
            Logger.getLogger(TextProcessorPCLXML.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
