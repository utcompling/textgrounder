package opennlp.textgrounder.text.io;

import java.io.*;
import java.util.*;
import javax.xml.datatype.*;
import javax.xml.stream.*;

import opennlp.textgrounder.text.*;
import opennlp.textgrounder.topo.*;
import opennlp.textgrounder.util.*;

public class CorpusKMLWriter {
    
      private static double RADIUS = .2;
      private static int SIDES = 10;
      private static int BARSCALE = 50000;

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

          KMLUtil.writeHeader(out, "corpus");
          
          for (opennlp.textgrounder.topo.Location loc : locationCounts.keySet()) {
              this.writePlacemarkAndPolygon(out, loc);
          }

          KMLUtil.writeFooter(out);

          out.close();
      }

      protected void writePlacemarkAndPolygon(XMLStreamWriter out, opennlp.textgrounder.topo.Location loc) throws Exception {
          String name = loc.getName();
          Coordinate coord = loc.getRegion().getCenter();
          int count = locationCounts.get(loc);

          KMLUtil.writePolygon(out, name, coord, SIDES, RADIUS, Math.log(count) * BARSCALE);
      }
}
