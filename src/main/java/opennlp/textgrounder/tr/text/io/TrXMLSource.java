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
package opennlp.textgrounder.tr.text.io;

import java.io.Reader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import opennlp.textgrounder.tr.text.Corpus;
import opennlp.textgrounder.tr.text.Document;
import opennlp.textgrounder.tr.text.DocumentSource;
import opennlp.textgrounder.tr.text.Sentence;
import opennlp.textgrounder.tr.text.SimpleSentence;
import opennlp.textgrounder.tr.text.SimpleToken;
import opennlp.textgrounder.tr.text.SimpleToponym;
import opennlp.textgrounder.tr.text.Token;
import opennlp.textgrounder.tr.text.Toponym;
import opennlp.textgrounder.tr.text.prep.Tokenizer;
import opennlp.textgrounder.tr.topo.Coordinate;
import opennlp.textgrounder.tr.topo.Location;
import opennlp.textgrounder.tr.topo.PointRegion;
import opennlp.textgrounder.tr.topo.Region;
import opennlp.textgrounder.tr.util.Span;

public class TrXMLSource extends DocumentSource {
  private final XMLStreamReader in;
  private final Tokenizer tokenizer;
  private boolean corpusWrapped = false;
  private int sentsPerDocument;
  private int partOfDoc = 0;
  private String curDocId;

  public TrXMLSource(Reader reader, Tokenizer tokenizer) throws XMLStreamException {
    this(reader, tokenizer, -1);
  }

  public TrXMLSource(Reader reader, Tokenizer tokenizer, int sentsPerDocument) throws XMLStreamException {
    this.tokenizer = tokenizer;
    this.sentsPerDocument = sentsPerDocument;

    XMLInputFactory factory = XMLInputFactory.newInstance();
    this.in = factory.createXMLStreamReader(reader);

    while (this.in.hasNext() && this.in.next() != XMLStreamReader.START_ELEMENT) {}
    if (this.in.getLocalName().equals("corpus")) {
      this.in.nextTag();
      this.corpusWrapped = true;
    }
  }

  private void nextTag() {
    try {
      this.in.nextTag();
    } catch (XMLStreamException e) {
      System.err.println("Error while advancing TR-XML file.");
    }
  }

  public void close() {
    try {
      this.in.close();
    } catch (XMLStreamException e) {
      System.err.println("Error while closing TR-XML file.");
    }
  }

  public boolean hasNext() {
      //try {
          if(this.in.isEndElement() && this.in.getLocalName().equals("doc") && this.corpusWrapped) {
              this.nextTag();
          }
          //} catch(XMLStreamException e) {
          //System.err.println("Error while closing TR-XML file.");
          //}
          if(this.in.getLocalName().equals("doc"))
              TrXMLSource.this.partOfDoc = 0;
          return this.in.isStartElement() && (this.in.getLocalName().equals("doc") || this.in.getLocalName().equals("s"));
  }

  public Document<Token> next() {
    //assert this.in.isStartElement() && this.in.getLocalName().equals("doc");
    String id;
    if(TrXMLSource.this.sentsPerDocument <= 0)
        id = TrXMLSource.this.in.getAttributeValue(null, "id");
    else {
        if(TrXMLSource.this.partOfDoc == 0)
            TrXMLSource.this.curDocId = TrXMLSource.this.in.getAttributeValue(null, "id");

        id = TrXMLSource.this.curDocId + ".p" + TrXMLSource.this.partOfDoc;
    }
    if(this.in.getLocalName().equals("doc"))
        TrXMLSource.this.nextTag();

    return new Document(id) {
      private static final long serialVersionUID = 42L;
      public Iterator<Sentence<Token>> iterator() {
        return new SentenceIterator() {
          int sentNumber = 0;
          public boolean hasNext() {
            if (TrXMLSource.this.in.isStartElement() &&
                TrXMLSource.this.in.getLocalName().equals("s") &&
                (TrXMLSource.this.sentsPerDocument <= 0 ||
                 sentNumber < TrXMLSource.this.sentsPerDocument)) {
              return true;
            } else {
              return false;
            }
          }

          public Sentence<Token> next() {
            String id = TrXMLSource.this.in.getAttributeValue(null, "id");
            List<Token> tokens = new ArrayList<Token>();
            List<Span<Toponym>> toponymSpans = new ArrayList<Span<Toponym>>();

            try {
              while (TrXMLSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT &&
                    (TrXMLSource.this.in.getLocalName().equals("w") ||
                     TrXMLSource.this.in.getLocalName().equals("toponym"))) {
                String name = TrXMLSource.this.in.getLocalName();
 
                if (name.equals("w")) {
                  tokens.add(new SimpleToken(TrXMLSource.this.in.getAttributeValue(null, "tok")));
                } else {
                  int spanStart = tokens.size();
                  String form = TrXMLSource.this.in.getAttributeValue(null, "term");
                  List<String> formTokens = TrXMLSource.this.tokenizer.tokenize(form);

                  for (String formToken : TrXMLSource.this.tokenizer.tokenize(form)) {
                    tokens.add(new SimpleToken(formToken));
                  }

                  ArrayList<Location> locations = new ArrayList<Location>();
                  int goldIdx = -1;

                  if (TrXMLSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT &&
                      TrXMLSource.this.in.getLocalName().equals("candidates")) {
                    while (TrXMLSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT &&
                           TrXMLSource.this.in.getLocalName().equals("cand")) {
                      String selected = TrXMLSource.this.in.getAttributeValue(null, "selected");
                      if (selected != null && selected.equals("yes")) {
                        goldIdx = locations.size();
                      }

                      double lat = Double.parseDouble(TrXMLSource.this.in.getAttributeValue(null, "lat"));
                      double lng = Double.parseDouble(TrXMLSource.this.in.getAttributeValue(null, "long"));
                      Region region = new PointRegion(Coordinate.fromDegrees(lat, lng));
                      locations.add(new Location(form, region));
                      TrXMLSource.this.nextTag();
                      assert TrXMLSource.this.in.isEndElement() &&
                             TrXMLSource.this.in.getLocalName().equals("cand");
                    }
                  }

                  if (locations.size() > 0 && goldIdx > -1) {
                    Toponym toponym = new SimpleToponym(form, locations, goldIdx);
		    if(toponym.getGoldIdx() >= toponym.getCandidates().size())
			System.out.println(toponym.getForm()+": "+toponym.getGoldIdx()+"/"+toponym.getCandidates().size());
                    toponymSpans.add(new Span<Toponym>(spanStart, tokens.size(), toponym));
                  }
                }
                TrXMLSource.this.nextTag();
              }
            } catch (XMLStreamException e) {
              System.err.println("Error while reading TR-XML file.");
            }

            TrXMLSource.this.nextTag();
            sentNumber++;
            if(sentNumber == TrXMLSource.this.sentsPerDocument)
                TrXMLSource.this.partOfDoc++;
            return new SimpleSentence(id, tokens, toponymSpans);           
          }
        };
      }
    };
  }
}

