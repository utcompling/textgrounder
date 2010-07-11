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
package opennlp.wrapper.rlda.textstructs;

import java.io.*;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

/**
 * Text processing class for TEI (text encoding initiative) encoded XML files.
 *
 * The PCL travel corpus is encoded in TEI format and this class is to be
 * used with pcl travel. By using the named entity definitions that come
 * with the dtd for pcl travel, all encoding issues are handled within this
 * class. Any non-lower ascii characters are normalized by first normalizing
 * according to unicode standards and then stripping non-lower ascii portions.
 *
 * @author tsmoon
 */
public class TextProcessor {

    protected static SAXBuilder builder = new SAXBuilder();
    protected Lexicon lexicon;
    protected static String newline = System.getProperty("line.separator");

    /**
     * Default constructor. Instantiate CRFClassifier.
     * 
     * @param lexicon lookup table for words and indexes
     */
    public TextProcessor(Lexicon _lexicon) throws
          ClassCastException, IOException, ClassNotFoundException {
        lexicon = _lexicon;
    }

    /**
     * Process texts, identify toponyms and store in arrays.
     *
     * @param locationOfFile path to input
     * @param tokenArrayBuffer collection of arrays for storing word ids, toponym
     * status, document ids and stopword status
     * @param stopwordList list of stopwords
     * @throws FileNotFoundException
     * @throws IOException
     */
    public void processFile(String locationOfFile,
          TokenArrayBuffer tokenArrayBuffer, StopwordList stopwordList) throws
          FileNotFoundException, IOException {

        if (!locationOfFile.endsWith(".xml")) {
            return;
        }
        File file = new File(locationOfFile);
        Document doc = null;
        try {
            doc = builder.build(file);
            Element element = doc.getRootElement();
            Element child = element.getChild("text").getChild("body");
            List<Element> divs = new ArrayList<Element>(child.getChildren("div"));

            for (Element div : divs) {
                StringBuilder buf = new StringBuilder();
                List<Element> pars = new ArrayList<Element>(div.getChildren("p"));
                for (Element par : pars) {
                    for (char c :
                          Normalizer.normalize(par.getTextNormalize(), Normalizer.Form.NFKC).toCharArray()) {
                        if (((int) c) < 0x7F) {
                            buf.append(c);
                        }
                    }
                    buf.append(newline);
                }
                String text = buf.toString().trim();
            }

        } catch (JDOMException ex) {
            Logger.getLogger(TextProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
