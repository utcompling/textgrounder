///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Travis Brown, The University of Texas at Austin
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
package opennlp.textgrounder.text.io;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.Writer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import opennlp.textgrounder.text.Corpus;
import opennlp.textgrounder.text.Document;
import opennlp.textgrounder.text.Sentence;
import opennlp.textgrounder.text.Token;
import opennlp.textgrounder.text.Toponym;

public class CorpusDocumentXMLWriter extends CorpusXMLWriter {
  private final String prefix;

  public CorpusDocumentXMLWriter(Corpus corpus) {
    this(corpus, "doc-");
  }

  public CorpusDocumentXMLWriter(Corpus corpus, String prefix) {
    super(corpus);
    this.prefix = prefix;
  }

  public void write(File file) {
    if (!file.isDirectory()) {
      throw new UnsupportedOperationException("The document writer can only write to a directory.");
    } else {
      int idx = 0;
      for (Document document : this.corpus) {
        try {
          File docFile = new File(file, String.format("%s%06d.xml", this.prefix, idx));
          OutputStream stream = new BufferedOutputStream(new FileOutputStream(docFile));
          XMLStreamWriter out = this.createXMLStreamWriter(stream);
          out.writeStartDocument();
          out.writeStartElement("corpus");
          out.writeAttribute("created", this.getCalendar().toString());
          this.writeDocument(out, document);
          out.writeEndElement();
          stream.close();
        } catch (XMLStreamException e) {
          System.err.println(e);
          System.exit(1);
        } catch (IOException e) {
          System.err.println(e);
          System.exit(1);
        }
        idx++;
      }
    }    
  }

  public void write(OutputStream stream) {
    throw new UnsupportedOperationException("The document writer can only write to a directory.");
  }

  public void write(Writer writer) {
    throw new UnsupportedOperationException("The document writer can only write to a directory.");
  }
}

