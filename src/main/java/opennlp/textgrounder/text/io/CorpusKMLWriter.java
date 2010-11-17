package opennlp.textgrounder.text.io;

import java.io.*;

import java.util.*;

import javax.xml.datatype.*;

import javax.xml.stream.*;

import opennlp.textgrounder.text.*;

import opennlp.textgrounder.topo.*;

public class CorpusKMLWriter {
      protected final Corpus<? extends Token> corpus;
      private final XMLOutputFactory factory;

      private Map<opennlp.textgrounder.topo.Location, Integer> locationCounts;

      public CorpusKMLWriter(Corpus<? extends Token> corpus) {
        this.corpus = corpus;
        this.factory = XMLOutputFactory.newInstance();
        this.locationCounts = countLocations(corpus);
      }

      protected Map<opennlp.textgrounder.topo.Location, Integer> countLocations(Corpus<? extends Token> corpus) {
          Map<opennlp.textgrounder.topo.Location, Integer> locationCounts = new HashMap<opennlp.textgrounder.topo.Location, Integer>();

          for(Document<? extends Token> doc : corpus) {
            for(Sentence<? extends Token> sent : doc) {
                for(Toponym toponym : sent.getToponyms()) {
                    if(toponym.getAmbiguity() > 0 && toponym.hasSelected()) {
                        opennlp.textgrounder.topo.Location loc = toponym.getCandidates().get(toponym.getSelectedIdx());
                        Integer prevCount = locationCounts.get(loc);
                        if(prevCount == null)
                            prevCount = 0;
                        locationCounts.put(loc, prevCount + 1);
                    }
                }
            }
          }

          return locationCounts;
      }

      protected XMLGregorianCalendar getCalendar() throws Exception {
        return this.getCalendar(new Date());
      }

      protected XMLGregorianCalendar getCalendar(Date time) throws Exception {
        XMLGregorianCalendar xgc = null;
        GregorianCalendar gc = new GregorianCalendar();
        gc.setTime(time);

        xgc = DatatypeFactory.newInstance().newXMLGregorianCalendar(gc);

        return xgc;
      }

      protected XMLStreamWriter createXMLStreamWriter(Writer writer) throws XMLStreamException {
        return this.factory.createXMLStreamWriter(writer);
      }

      protected XMLStreamWriter createXMLStreamWriter(OutputStream stream) throws XMLStreamException {
        return this.factory.createXMLStreamWriter(stream, "UTF-8");
      }

      public void write(File file) throws Exception {
          assert(!file.isDirectory());
          OutputStream stream = new BufferedOutputStream(new FileOutputStream(file));
          this.write(this.createXMLStreamWriter(stream));
          stream.close();
      }

      public void write(OutputStream stream) throws Exception {
          this.write(this.createXMLStreamWriter(stream));
      }

      public void write(Writer writer) throws Exception {
          this.write(this.createXMLStreamWriter(writer));
      }

      protected void write(XMLStreamWriter out) throws Exception {
          out.writeStartDocument("UTF-8", "1.0");
          out.writeStartElement("kml");
          out.writeAttribute("created", this.getCalendar().toString());
          out.writeAttribute("xmlns", "http://www.opengis.net/kml/2.2");
          out.writeAttribute("xmlns:gx", "http://www.google.com/kml/ext/2.2");
          out.writeAttribute("xmlns:kml", "http://www.opengis.net/kml/2.2");
          out.writeAttribute("xmlns:atom", "http://www.w3.org/2005/Atom");

          out.writeStartElement("Document");

          this.writeStyleNode(out);

          out.writeStartElement("Folder");
          this.writeFolderHeading(out);
          
          for (opennlp.textgrounder.topo.Location loc : locationCounts.keySet()) {
              this.writePlacemarkNode(out, loc);
          }
          
          out.writeEndDocument();
          out.close();
      }

      protected void writePlacemarkNode(XMLStreamWriter out, opennlp.textgrounder.topo.Location loc) throws Exception {
          String name = loc.getName();
          Coordinate coord = loc.getRegion().getCenter();
          //int count = locationCounts.get(loc);

          out.writeStartElement("Placemark");
            out.writeStartElement("name"); out.writeCharacters(name); out.writeEndElement();
            //Region goes here
            //out.writeStartElement("styleUrl"); out.writeCharacters("#bar"); out.writeEndElement();
            out.writeStartElement("Point");
              out.writeStartElement("coordinates");
                out.writeCharacters(coord.getLngDegrees() + "," + coord.getLatDegrees()); // KML wants (lon,lat)
              out.writeEndElement();
            out.writeEndElement();
          out.writeEndElement();
      }

      protected void writeStyleNode(XMLStreamWriter out) throws Exception {
          out.writeStartElement("Style"); out.writeAttribute("id", "bar");
            out.writeStartElement("PolyStyle");
              out.writeStartElement("outline"); out.writeCharacters("0"); out.writeEndElement();
            out.writeEndElement();
            out.writeStartElement("IconStyle");
              out.writeStartElement("Icon"); out.writeEndElement();
            out.writeEndElement();
          out.writeEndElement();
      }

      protected void writeFolderHeading(XMLStreamWriter out) throws Exception {
          out.writeStartElement("name"); out.writeCharacters("Locations"); out.writeEndElement();
          out.writeStartElement("open"); out.writeCharacters("1"); out.writeEndElement();
          out.writeStartElement("description"); out.writeCharacters("Location distribution"); out.writeEndElement();
          out.writeStartElement("LookAt");
            out.writeStartElement("latitude"); out.writeCharacters("42"); out.writeEndElement();
            out.writeStartElement("longitude"); out.writeCharacters("-102"); out.writeEndElement();
            out.writeStartElement("altitude"); out.writeCharacters("0"); out.writeEndElement();
            out.writeStartElement("range"); out.writeCharacters("5000000"); out.writeEndElement();
            out.writeStartElement("tilt"); out.writeCharacters("53.454348562403"); out.writeEndElement();
            out.writeStartElement("heading"); out.writeCharacters("0"); out.writeEndElement();
          out.writeEndElement();
      }
}
