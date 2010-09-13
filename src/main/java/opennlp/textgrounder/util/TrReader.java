///////////////////////////////////////////////////////////////////////////////
//  Copyright 2010 Travis Brown <travis.brown@mail.utexas.edu>.
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
package opennlp.textgrounder.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import opennlp.textgrounder.topostructs.Coordinate;

public class TrReader implements Iterable<TrReader.Document>, Iterator<TrReader.Document> {
  protected XMLStreamReader in;

  public TrReader(String filename)
    throws IOException, FileNotFoundException, XMLStreamException {
    this(new BufferedReader(new FileReader(filename)));
  }

  public TrReader(Reader reader) throws XMLStreamException {
    XMLInputFactory factory = XMLInputFactory.newInstance();
    this.in = factory.createXMLStreamReader(reader);
    while (this.in.hasNext() && this.in.next() != XMLStreamReader.START_ELEMENT) {}
    if (this.in.getLocalName().equals("corpus")) {
      this.in.nextTag();
    }
  }

  public Iterator<TrReader.Document> iterator() {
    return this;
  }

  public boolean hasNext() {
    return this.in.isStartElement() && this.in.getLocalName().equals("doc");
  }

  public TrReader.Document next() {
    try {
      return TrReader.this.new Document();
    } catch (XMLStreamException e) {
      throw new NoSuchElementException(e.getMessage());
    }
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }

  class Document implements Iterable<TrReader.Sentence>, Iterator<TrReader.Sentence> {
    private final String id;

    protected Document() throws XMLStreamException {
      assert TrReader.this.in.isStartElement() && TrReader.this.in.getLocalName().equals("doc");
      this.id = TrReader.this.in.getAttributeValue(null, "id");
      TrReader.this.in.nextTag();
    }

    public String getId() {
      return this.id;
    }
    
    public Iterator<TrReader.Sentence> iterator() {
      return this;
    }

    public boolean hasNext() {
      if (TrReader.this.in.isStartElement() && TrReader.this.in.getLocalName().equals("s")) {
        return true;
      } else {
        try {
          TrReader.this.in.nextTag();
        } catch (XMLStreamException e) {}
        return false;
      }
    }

    public TrReader.Sentence next() {
      try {
        return TrReader.this.new Sentence();
      } catch (XMLStreamException e) {
        throw new NoSuchElementException(e.getMessage());
      }
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  class Sentence implements Iterable<TrReader.Token>, Iterator<TrReader.Token> {
    protected Sentence() throws XMLStreamException {
      assert TrReader.this.in.isStartElement() && TrReader.this.in.getLocalName().equals("s");
      TrReader.this.in.nextTag();
    }

    public Iterator<TrReader.Token> iterator() {
      return this;
    }

    public boolean hasNext() {
      String name = TrReader.this.in.getLocalName();
      if (TrReader.this.in.isStartElement() && (name.equals("w") || name.equals("toponym"))) {
        return true;
      } else {
        try {
          TrReader.this.in.nextTag();
        } catch (XMLStreamException e) {}
        return false;
      }
    }

    public TrReader.Token next() {
      try {
        String name = TrReader.this.in.getLocalName();
        if (name.equals("w")) {
          TrReader.Token token = TrReader.this.new Token(TrReader.this.in.getAttributeValue(null, "tok"));
          TrReader.this.in.nextTag();
          TrReader.this.in.nextTag();
          return token;
        } else if (name.equals("toponym")) {
          TrReader.Toponym toponym = TrReader.this.new Toponym(TrReader.this.in.getAttributeValue(null, "term"));
          if (TrReader.this.in.nextTag() == XMLStreamReader.START_ELEMENT && TrReader.this.in.getLocalName().equals("candidates"))
          {
            while (TrReader.this.in.nextTag() == XMLStreamReader.START_ELEMENT && TrReader.this.in.getLocalName().equals("cand"))
            {
              String id = TrReader.this.in.getAttributeValue(null, "id");
              double lat = Double.parseDouble(TrReader.this.in.getAttributeValue(null, "lat"));
              double lng = Double.parseDouble(TrReader.this.in.getAttributeValue(null, "long"));
              toponym.addCandidate(TrReader.this.new Candidate(id, lat, lng));
              TrReader.this.in.nextTag();
              assert TrReader.this.in.isEndElement() && TrReader.this.in.getLocalName().equals("cand");
            }
            TrReader.this.in.nextTag();
          }
          TrReader.this.in.nextTag();
          return toponym;
        } else {
          throw new XMLStreamException(String.format("Invalid token element: %s.", name));
        }
      } catch (XMLStreamException e) {
        throw new NoSuchElementException(e.getMessage());
      }
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  class Token {
    private final String term;

    protected Token(String term) {
      this.term = term;
    }

    public String getTerm() {
      return this.term;
    }

    public boolean isToponym() {
      return false;
    }
  }

  class Toponym extends TrReader.Token implements Iterable<TrReader.Candidate> {
    private final List<TrReader.Candidate> candidates;

    protected Toponym(String term) {
      super(term);
      this.candidates = new ArrayList<TrReader.Candidate>();
    }

    protected void addCandidate(TrReader.Candidate candidate) {
      this.candidates.add(candidate);
    }

    public boolean isToponym() {
      return true;
    }

    public Iterator<TrReader.Candidate> iterator() {
      return this.candidates.iterator();
    }
  }

  protected class Candidate {
    private final String id;
    private final Coordinate coord;

    protected Candidate(String id, double lat, double lng) {
      this.id = id;
      this.coord = new Coordinate(lat, lng);
    }

    public Coordinate getCoordinate()
    {
      return this.coord;
    }
  }

  public static void main(String[] args)
    throws IOException, FileNotFoundException, XMLStreamException {
    TrReader reader = new TrReader(args[0]);
    for (TrReader.Document document : reader) {
      System.out.format("Reading document: %s\n", document.getId());
      for (TrReader.Sentence sentence : document) {
        System.out.println("  Reading sentence.");
        for (TrReader.Token token : sentence) {
          System.out.format("    Reading token: %s\n", token.getTerm());
          if (token.isToponym()) {
            TrReader.Toponym toponym = (TrReader.Toponym) token;
            for (TrReader.Candidate candidate : toponym) {
              System.out.format("      Candidate: %s\n", candidate.getCoordinate().toString());
            }
          }
        }
      }
    }
  }
}

