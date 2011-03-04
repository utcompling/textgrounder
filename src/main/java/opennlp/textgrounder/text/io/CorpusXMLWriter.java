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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.GregorianCalendar;
import java.util.List;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import opennlp.textgrounder.text.Corpus;
import opennlp.textgrounder.text.Document;
import opennlp.textgrounder.text.Sentence;
import opennlp.textgrounder.text.Token;
import opennlp.textgrounder.text.Toponym;

import opennlp.textgrounder.topo.Location;
import opennlp.textgrounder.topo.Coordinate;

public class CorpusXMLWriter {
  protected final Corpus<? extends Token> corpus;
  private final XMLOutputFactory factory;

  public CorpusXMLWriter(Corpus<? extends Token> corpus) {
    this.corpus = corpus;
    this.factory = XMLOutputFactory.newInstance();
  }

  protected XMLGregorianCalendar getCalendar() {
    return this.getCalendar(new Date());
  }

  protected XMLGregorianCalendar getCalendar(Date time) {
    XMLGregorianCalendar xgc = null;
    GregorianCalendar gc = new GregorianCalendar();
    gc.setTime(time);
    try {
      xgc = DatatypeFactory.newInstance().newXMLGregorianCalendar(gc);
    } catch (DatatypeConfigurationException e) {
      System.err.println(e);
      System.exit(1);
    }
    return xgc;
  }

  protected XMLStreamWriter createXMLStreamWriter(Writer writer) throws XMLStreamException {
    return this.factory.createXMLStreamWriter(writer);
  }

  protected XMLStreamWriter createXMLStreamWriter(OutputStream stream) throws XMLStreamException {
    return this.factory.createXMLStreamWriter(stream, "UTF-8");
  }

  protected void writeDocument(XMLStreamWriter out, Document<Token> document) throws XMLStreamException {
    out.writeStartElement("doc");
    if (document.getId() != null) {
      out.writeAttribute("id", document.getId());
    }
    for (Sentence<Token> sentence : document) {
      this.writeSentence(out, sentence);
    }
    out.writeEndElement();
  }

  protected void writeSentence(XMLStreamWriter out, Sentence<Token> sentence) throws XMLStreamException {
    out.writeStartElement("s");
    if (sentence.getId() != null) {
      out.writeAttribute("id", sentence.getId());
    }
    for (Token token : sentence) {
      if (token.isToponym()) {
        this.writeToponym(out, (Toponym) token);
      } else {
        this.writeToken(out, token);
      }
    }
    out.writeEndElement();
  }

  protected void writeToken(XMLStreamWriter out, Token token) throws XMLStreamException {
    out.writeStartElement("w");
    out.writeAttribute("tok", token.getForm());
    out.writeEndElement();
  }

  protected void writeToponym(XMLStreamWriter out, Toponym toponym) throws XMLStreamException {
    out.writeStartElement("toponym");
    out.writeAttribute("term", toponym.getForm());
    out.writeStartElement("candidates");
    Location gold = toponym.hasGold() ? toponym.getGold() : null;
    Location selected = toponym.hasSelected() ? toponym.getSelected() : null;

    for (Location location : toponym) {
      this.writeLocation(out, location, gold, selected);
    }
    out.writeEndElement();
    out.writeEndElement();
  }

  protected void writeLocation(XMLStreamWriter out, Location location, Location gold, Location selected) throws XMLStreamException {
    out.writeStartElement("cand");
    out.writeAttribute("id", String.format("c%d", location.getId()));
    out.writeAttribute("lat", String.format("%f", location.getRegion().getCenter().getLatDegrees()));
    out.writeAttribute("long", String.format("%f", location.getRegion().getCenter().getLngDegrees()));
    out.writeAttribute("type", String.format("%s", location.getType()));
    out.writeAttribute("admin1code", String.format("%s", location.getAdmin1Code()));
    int population = location.getPopulation();
    if (population > 0) {
      out.writeAttribute("population", String.format("%d", population));
    }
    if (location == gold) {
      out.writeAttribute("gold", "true");
    }
    if (location == selected) {
      out.writeAttribute("selected", "true");
    }
    
    out.writeStartElement("representatives");
    for(Coordinate coord : location.getRegion().getRepresentatives()) {
        out.writeStartElement("rep");
        out.writeAttribute("lat", String.format("%f", coord.getLatDegrees()));
        out.writeAttribute("long", String.format("%f", coord.getLngDegrees()));
        out.writeEndElement();
    }
    out.writeEndElement();
    
    out.writeEndElement();
  }

  public void write(File file) {
    this.write(file, "doc-");
  }

  public void write(File file, String prefix) {
    try {
      if (file.isDirectory()) {
        int idx = 0;
        for (Document document : this.corpus) {
          File docFile = new File(file, String.format("%s%06d.xml", prefix, idx));
          OutputStream stream = new BufferedOutputStream(new FileOutputStream(docFile));
          XMLStreamWriter out = this.createXMLStreamWriter(stream);
          out.writeStartDocument("UTF-8", "1.0");
          out.writeStartElement("corpus");
          out.writeAttribute("created", this.getCalendar().toString());
          this.writeDocument(out, document);
          out.writeEndElement();
          out.close();
          stream.close();
          idx++;
        }
      } else {
        OutputStream stream = new BufferedOutputStream(new FileOutputStream(file));
        this.write(this.createXMLStreamWriter(stream));
        stream.close();
      }
    } catch (XMLStreamException e) {
      System.err.println(e);
      System.exit(1);
    } catch (IOException e) {
      System.err.println(e);
      System.exit(1);
    }
  }

  public void write(OutputStream stream) {
    try {
      this.write(this.createXMLStreamWriter(stream));
    } catch (XMLStreamException e) {
      System.err.println(e);
      System.exit(1);
    }
  }

  public void write(Writer writer) {
    try {
      this.write(this.createXMLStreamWriter(writer));
    } catch (XMLStreamException e) {
      System.err.println(e);
      System.exit(1);
    }
  }

  protected void write(XMLStreamWriter out) {
    try {
      out.writeStartDocument("UTF-8", "1.0");
      out.writeStartElement("corpus");
      out.writeAttribute("created", this.getCalendar().toString());
      for (Document document : this.corpus) {
        this.writeDocument(out, document);
      }
      out.writeEndElement();
      out.close();
    } catch (XMLStreamException e) {
      System.err.println(e);
      System.exit(1);
    }
  }
}

