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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.jdom.*;
import org.jdom.input.SAXBuilder;

/**
 * Class that stores data about a document (sequence of tokens) in a corpus. I
 * would call this simply `Document', but that conflicts with org.jdom.Document.
 * Larger divisions of a document, e.g. sentences and chapters, are stored
 * simply by noting the start and end indices of the tokens making up the
 * division. This would be inconvenient if we had to insert tokens in the middle
 * of a document, but in practice this doesn't happen. By storing all the tokens
 * of a document together:
 * <ol>
 * <li>We make it possible for code that e.g. wants to retrieve the context
 * around a token without regard for sentences or paragraphs or chapters or
 * whatever to do so.
 * <li>We also allow for documents with or without explicitly marked sentences,
 * chapters, etc. If a document doesn't include the given divisions, the
 * corresponding list is simply empty.
 * </ol>
 * Code that wants to process sentences and chapters separately can do so by
 * iterating over chapters and then over sentences within chapters.
 * 
 * @author benwing
 */
public class CorpusDocument extends DocumentComponent {
    static private final long serialVersionUID = 1L;

    protected String name; // Name of document
    protected Corpus corpus; // Corpus that we're in
    protected List<Token> tokens;

    public CorpusDocument(Corpus corpus, String name) {
        /* For some reason you can't say `super(this, "doc");'. */
        super(null, "doc");
        this.document = this;
        this.corpus = corpus;
        this.name = name;
        tokens = new ArrayList<Token>();
    }

    public Corpus getCorpus() {
      return this.corpus;
    }

    protected void writeElement(XMLStreamWriter w) throws XMLStreamException {
      w.writeStartElement(this.type);
      /* This is a hack. */
      w.writeStartElement("s");
      for (String name : this.props.keySet()) {
        w.writeAttribute(name, props.get(name));
      }
      for (DocumentComponent child : this) {
        child.writeElement(w);
      }
      w.writeEndElement();
      w.writeEndElement();
    }

    public void processElement(Element e) {
        if (!e.getName().equals("doc"))
            throw new RuntimeException("Unknown top-level element type: " + e.getName());
        super.processElement(e);
    }
    
    public void loadFromXML(String path) {
        File file = new File(path);
        Document doc = null;
        SAXBuilder builder = new SAXBuilder();
        try {
            doc = builder.build(file);
            processElement(doc.getRootElement());
        } catch (JDOMException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
