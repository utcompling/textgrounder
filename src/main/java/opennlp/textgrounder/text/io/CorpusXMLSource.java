package opennlp.textgrounder.text.io;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.util.*;

import opennlp.textgrounder.text.*;
import opennlp.textgrounder.text.prep.*;
import opennlp.textgrounder.util.*;
import opennlp.textgrounder.topo.*;

public class CorpusXMLSource extends DocumentSource {
  private final XMLStreamReader in;
  private final Tokenizer tokenizer;

  public CorpusXMLSource(Reader reader, Tokenizer tokenizer) throws XMLStreamException {
    this.tokenizer = tokenizer;

    XMLInputFactory factory = XMLInputFactory.newInstance();
    this.in = factory.createXMLStreamReader(reader);

    while (this.in.hasNext() && this.in.next() != XMLStreamReader.START_ELEMENT) {}
    if (this.in.getLocalName().equals("corpus")) {
      this.in.nextTag();
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
    return this.in.isStartElement() && this.in.getLocalName().equals("doc");
  }

  public Document<Token> next() {
    assert this.in.isStartElement() && this.in.getLocalName().equals("doc");
    String id = CorpusXMLSource.this.in.getAttributeValue(null, "id");
    CorpusXMLSource.this.nextTag();

    return new Document(id) {
      public Iterator<Sentence<Token>> iterator() {
        return new SentenceIterator() {
          public boolean hasNext() {
            if (CorpusXMLSource.this.in.isStartElement() &&
                CorpusXMLSource.this.in.getLocalName().equals("s")) {
              return true;
            } else {
              return false;
            }
          }

          public Sentence<Token> next() {
            String id = CorpusXMLSource.this.in.getAttributeValue(null, "id");
            List<Token> tokens = new ArrayList<Token>();
            List<Span<Toponym>> toponymSpans = new ArrayList<Span<Toponym>>();

            try {
              while (CorpusXMLSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT &&
                    (CorpusXMLSource.this.in.getLocalName().equals("w") ||
                     CorpusXMLSource.this.in.getLocalName().equals("toponym"))) {
                String name = CorpusXMLSource.this.in.getLocalName();
 
                if (name.equals("w")) {
                  tokens.add(new SimpleToken(CorpusXMLSource.this.in.getAttributeValue(null, "tok")));
                } else {
                  int spanStart = tokens.size();
                  String form = CorpusXMLSource.this.in.getAttributeValue(null, "term");
                  List<String> formTokens = CorpusXMLSource.this.tokenizer.tokenize(form);

                  for (String formToken : CorpusXMLSource.this.tokenizer.tokenize(form)) {
                    tokens.add(new SimpleToken(formToken));
                  }

                  ArrayList<Location> locations = new ArrayList<Location>();
                  int goldIdx = -1;
                  int selectedIdx = -1;

                  if (CorpusXMLSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT &&
                      CorpusXMLSource.this.in.getLocalName().equals("candidates")) {
                    while (CorpusXMLSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT &&
                           CorpusXMLSource.this.in.getLocalName().equals("cand")) {
                      String gold = CorpusXMLSource.this.in.getAttributeValue(null, "gold");
                      String selected = CorpusXMLSource.this.in.getAttributeValue(null, "selected");
                      if (selected != null && (selected.equals("yes") || selected.equals("true"))) {
                        selectedIdx = locations.size();
                      }
                      if (gold != null && (gold.equals("yes") || selected.equals("true"))) {
                        goldIdx = locations.size();
                      }

                      String locId = CorpusXMLSource.this.in.getAttributeValue(null, "id");
                      String type = CorpusXMLSource.this.in.getAttributeValue(null, "type");
                      String popString = CorpusXMLSource.this.in.getAttributeValue(null, "population");
                      Integer population = null;
                      if(popString != null)
                          population = Integer.parseInt(popString);
                      String admin1code = CorpusXMLSource.this.in.getAttributeValue(null, "admin1code");

                      ArrayList<Coordinate> representatives = new ArrayList<Coordinate>();
                      if(CorpusXMLSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT &&
                         CorpusXMLSource.this.in.getLocalName().equals("representatives")) {
                          while(CorpusXMLSource.this.in.nextTag() == XMLStreamReader.START_ELEMENT &&
                                CorpusXMLSource.this.in.getLocalName().equals("rep")) {
                              double lat = Double.parseDouble(CorpusXMLSource.this.in.getAttributeValue(null, "lat"));
                              double lng = Double.parseDouble(CorpusXMLSource.this.in.getAttributeValue(null, "long"));
                              representatives.add(Coordinate.fromDegrees(lat, lng));
                              CorpusXMLSource.this.nextTag();
                              assert CorpusXMLSource.this.in.isEndElement() &&
                                     CorpusXMLSource.this.in.getLocalName().equals("rep");
                          }
                      }

                      Region region = new PointSetRegion(representatives);
                      Location loc = new Location(locId, form, region, type, population, admin1code);
                      locations.add(loc);
                      CorpusXMLSource.this.nextTag();
                      assert CorpusXMLSource.this.in.isEndElement() &&
                             CorpusXMLSource.this.in.getLocalName().equals("cand");
                    }
                  }

                  if (locations.size() > 0 /*&& goldIdx > -1*/) {
                    Toponym toponym = new SimpleToponym(form, locations, goldIdx, selectedIdx);
                    toponymSpans.add(new Span<Toponym>(spanStart, tokens.size(), toponym));
                  }
                }
                CorpusXMLSource.this.nextTag();
                assert CorpusXMLSource.this.in.isStartElement() &&
                    (CorpusXMLSource.this.in.getLocalName().equals("w") ||
                     CorpusXMLSource.this.in.getLocalName().equals("toponym"));
              }
            } catch (XMLStreamException e) {
              System.err.println("Error while reading TR-XML file.");
            }

            CorpusXMLSource.this.nextTag();
            if(CorpusXMLSource.this.in.getLocalName().equals("doc")
               && CorpusXMLSource.this.in.isEndElement())
                CorpusXMLSource.this.nextTag();
            return new SimpleSentence(id, tokens, toponymSpans);           
          }
        };
      }
    };
  }

}
