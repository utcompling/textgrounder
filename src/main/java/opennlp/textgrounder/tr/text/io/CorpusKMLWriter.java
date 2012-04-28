package opennlp.textgrounder.tr.text.io;

import java.io.*;
import java.util.*;
import javax.xml.datatype.*;
import javax.xml.stream.*;

import opennlp.textgrounder.tr.text.*;
import opennlp.textgrounder.tr.topo.*;
import opennlp.textgrounder.tr.util.*;

public class CorpusKMLWriter {

      protected final Corpus<? extends Token> corpus;
      protected final XMLOutputFactory factory;

      protected Map<opennlp.textgrounder.tr.topo.Location, Integer> locationCounts;
      protected Map<opennlp.textgrounder.tr.topo.Location, List<String> > contexts;

    protected boolean outputGoldLocations;

    public CorpusKMLWriter(Corpus<? extends Token> corpus, boolean outputGoldLocations) {
        this.corpus = corpus;
        this.factory = XMLOutputFactory.newInstance();
        this.outputGoldLocations = outputGoldLocations;
      }

    public CorpusKMLWriter(Corpus<? extends Token> corpus) {
        this(corpus, false);
    }

      private void countLocationsAndPopulateContexts(Corpus<? extends Token> corpus) {
          locationCounts = new HashMap<opennlp.textgrounder.tr.topo.Location, Integer>();
          contexts = new HashMap<opennlp.textgrounder.tr.topo.Location, List<String> >();

          for(Document<? extends Token> doc : corpus) {
            int sentIndex = 0;
            for(Sentence<? extends Token> sent : doc) {
                int tokenIndex = 0;
                //for(Toponym toponym : sent.getToponyms()) {
                for(Token token : sent) {
                    if(token.isToponym()) {
                        Toponym toponym = (Toponym) token;
                        if((!outputGoldLocations && toponym.getAmbiguity() > 0 && toponym.hasSelected())
                           || (outputGoldLocations && toponym.hasGold())) {
                            opennlp.textgrounder.tr.topo.Location loc;
                            if(!outputGoldLocations)
                                loc = toponym.getCandidates().get(toponym.getSelectedIdx());
                            else
                                loc = toponym.getCandidates().get(toponym.getGoldIdx());
                            Integer prevCount = locationCounts.get(loc);
                            if(prevCount == null)
                                prevCount = 0;
                            locationCounts.put(loc, prevCount + 1);

                            List<String> curContexts = contexts.get(loc);
                            if(curContexts == null)
                                curContexts = new ArrayList<String>();
                            curContexts.add(getContextAround(doc, sentIndex, tokenIndex));
                            contexts.put(loc, curContexts);
                        }
                    }
                    tokenIndex++;
                }
                sentIndex++;
            }
          }     
      }

      private String getContextAround(Document<? extends Token> doc, int sentIndex, int tokenIndex) {
          StringBuffer sb = new StringBuffer();

          int curSentIndex = 0;
          for(Sentence<? extends Token> sent : doc) {
              if(curSentIndex == sentIndex - 1 || curSentIndex == sentIndex + 1) {
                  for(Token token : sent) {
                      sb.append(token.getOrigForm()).append(" ");
                      //if(StringUtil.containsAlphanumeric(token.getOrigForm()))
                      //    sb.append(" ");
                  }
                  if(curSentIndex == sentIndex + 1)
                      break;
              }
              else if(curSentIndex == sentIndex) {
                  int curTokenIndex = 0;
                  for(Token token : sent) {
                      if(curTokenIndex == tokenIndex)
                          sb.append("<b> ");
                      sb.append(token.getOrigForm()).append(" ");
                      if(curTokenIndex == tokenIndex)
                          sb.append("</b> ");
                      //if(StringUtil.containsAlphanumeric(token.getOrigForm()))
                      //    sb.append(" ");
                      curTokenIndex++;
                  }
              }
              curSentIndex++;
          }
          
          if(sb.length() > 0 || sb.charAt(sb.length() - 1) == ' ')
            sb.deleteCharAt(sb.length()-1);

          return sb.toString();
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
          
          countLocationsAndPopulateContexts(corpus);

          KMLUtil.writeHeader(out, "corpus");
          
          for (opennlp.textgrounder.tr.topo.Location loc : locationCounts.keySet()) {
              this.writePlacemarkAndPolygon(out, loc);
              this.writeContexts(out, loc);
          }

          KMLUtil.writeFooter(out);

          out.close();
      }

      protected void writePlacemarkAndPolygon(XMLStreamWriter out, opennlp.textgrounder.tr.topo.Location loc) throws Exception {
          String name = loc.getName();
          Coordinate coord = loc.getRegion().getCenter();
          int count = locationCounts.get(loc);

          KMLUtil.writePolygon(out, name, coord, KMLUtil.SIDES, KMLUtil.RADIUS, Math.log(count) * KMLUtil.BARSCALE);
      }

      protected void writeContexts(XMLStreamWriter out, opennlp.textgrounder.tr.topo.Location loc) {
          int i = 0;
          for(String curContext : contexts.get(loc)) {
              try {
                  KMLUtil.writeSpiralPoint(out, loc.getName(), i, curContext, loc.getRegion().getCenter().getNthSpiralPoint(i, KMLUtil.SPIRAL_RADIUS), KMLUtil.RADIUS);
              } catch(Exception e) {
                  e.printStackTrace();
                  System.exit(1);
              }
              i++;
          }
      }
}
