package opennlp.textgrounder.text.io;

import java.io.*;
import java.util.*;

//import javax.xml.stream.XMLInputFactory;
//import javax.xml.stream.XMLStreamException;
//import javax.xml.stream.XMLStreamReader;

import opennlp.textgrounder.text.*;
import opennlp.textgrounder.text.prep.*;
import opennlp.textgrounder.topo.*;
import opennlp.textgrounder.util.*;

public class GeoTextSource extends DocumentSource {
  //private final XMLStreamReader in;
  private final Tokenizer tokenizer;
  private List<GeoTextDocument> documents;
  private int curDocIndex = 0;
  

  public GeoTextSource(Reader reader, Tokenizer tokenizer) throws Exception {
    BufferedReader breader = new BufferedReader(reader);
    this.tokenizer = tokenizer;

    this.documents = new ArrayList<GeoTextDocument>();

    String curLine;
    String prevDocId = "-1";
    GeoTextDocument curDoc = null;
    int sentIndex = -1;
    while(true) {
	curLine = breader.readLine();
	if(curLine == null)
	    break;
	
	String[] tokens = curLine.split("\t");

        if(tokens.length < 6)
            continue;

        String docId = tokens[0];

        if(!docId.equals(prevDocId)) {
            curDoc = new GeoTextDocument(docId, tokens[1],
                                         Double.parseDouble(tokens[3]),
                                         Double.parseDouble(tokens[4]));
            documents.add(curDoc);
            sentIndex = -1;
        }
        prevDocId = docId;

        String rawSent = tokens[5];
        sentIndex++;
        List<Token> wList = new ArrayList<Token>();

        for(String w : tokenizer.tokenize(rawSent)) {
            wList.add(new SimpleToken(w));
        }

        curDoc.addSentence(new SimpleSentence("" + sentIndex, wList));
    }

    //XMLInputFactory factory = XMLInputFactory.newInstance();
    //this.in = factory.createXMLStreamReader(reader);

    //while (this.in.hasNext() && this.in.next() != XMLStreamReader.START_ELEMENT) {}
    //if (this.in.getLocalName().equals("corpus")) {
    //  this.in.nextTag();
    //}
  }

    /*private void nextTag() {
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
    }*/

  public boolean hasNext() {
      return this.curDocIndex < this.documents.size();//this.in.isStartElement() && this.in.getLocalName().equals("doc");
  }

  public Document<Token> next() {

      return documents.get(curDocIndex++);
      /*return new Document(id) {
	  public Iterator<Sentence<Token>> iterator() {
	      return new SentenceIterator() {
		  public boolean hasNext() {
		      
		  }
	      }
	  }
	  }*/
    /*assert this.in.isStartElement() && this.in.getLocalName().equals("doc");
    String id = TrXMLSource.this.in.getAttributeValue(null, "id");
    TrXMLSource.this.nextTag();

    return new Document(id) {
      public Iterator<Sentence<Token>> iterator() {
        return new SentenceIterator() {
          public boolean hasNext() {
            if (TrXMLSource.this.in.isStartElement() &&
                TrXMLSource.this.in.getLocalName().equals("s")) {
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
                    toponymSpans.add(new Span<Toponym>(spanStart, tokens.size(), toponym));
                  }
                }
                TrXMLSource.this.nextTag();
              }
            } catch (XMLStreamException e) {
              System.err.println("Error while reading TR-XML file.");
            }

            TrXMLSource.this.nextTag();
            return new SimpleSentence(id, tokens, toponymSpans);           
          }
        };
      }
      };*/
  }
}

