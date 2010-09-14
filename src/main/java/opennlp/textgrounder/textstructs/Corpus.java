///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Ben Wing, The University of Texas at Austin
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

//import opennlp.textgrounder.textstructs.old.Lexicon;
import opennlp.textgrounder.gazetteers.old.Gazetteer;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.jdom.*;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import com.google.inject.Inject;

/**
 * Class that stores data about a corpus (sequence of documents).
 * 
 * @author benwing
 */
public class Corpus extends ArrayList<CorpusDocument> {
    static private final long serialVersionUID = 1L;
    
    private final Gazetteer gazetteer;
    private final Lexicon<String> lexicon;

    @Inject
    public Corpus(Gazetteer gazetteer, Lexicon<String> lexicon) {
      this.gazetteer = gazetteer;
      this.lexicon = lexicon;
    }

    public Gazetteer getGazetteer() {
      return this.gazetteer;
    }

    public Lexicon<String> getLexicon() {
      return this.lexicon;
    }

    /**
     * Output the corpus in XML to the given file using an XML output stream to
     * conserve memory.
     * 
     * @param file
     * @throws IOException
     */
    public void writeXML(File file) throws IOException, XMLStreamException {
      XMLOutputFactory outFactory = XMLOutputFactory.newInstance();
      XMLStreamWriter out = outFactory.createXMLStreamWriter(new BufferedWriter(new FileWriter(file)));

      out.writeStartDocument("UTF-8", "1.0");
      out.writeStartElement("corpus");

      for (CorpusDocument cdoc : this) {
        System.out.println("Outputting XML for " + cdoc);
        cdoc.writeElement(out);
      }

      out.writeEndElement();
      out.close();
    }
}

