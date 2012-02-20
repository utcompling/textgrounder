package opennlp.textgrounder.text.io;

import opennlp.textgrounder.text.*;
import javax.xml.stream.*;
import opennlp.textgrounder.util.*;
import opennlp.textgrounder.topo.*;

public class GeoTextCorpusKMLWriter extends CorpusKMLWriter {
    public GeoTextCorpusKMLWriter(Corpus<? extends Token> corpus, boolean outputGoldLocations) {
        super(corpus, outputGoldLocations);
      }

    public GeoTextCorpusKMLWriter(Corpus<? extends Token> corpus) {
        this(corpus, false);
    }

      protected void writeDocument(XMLStreamWriter out, Document<? extends Token> document) throws XMLStreamException {
          Coordinate coord = outputGoldLocations ? document.getGoldCoord() : document.getSystemCoord();

          KMLUtil.writePlacemark(out, document.getId(), coord, KMLUtil.RADIUS);
          int sentIndex = 0;
          for(Sentence<? extends Token> sent : document) {
              StringBuffer curTweetSB = new StringBuffer();
              for(Token token : sent) {
                  if(isSanitary(token.getOrigForm()))
                     curTweetSB.append(token.getOrigForm()).append(" ");
              }
              String curTweet = curTweetSB.toString().trim();

              KMLUtil.writeSpiralPoint(out, document.getId(),
                                       sentIndex, curTweet,
                                       coord.getNthSpiralPoint(sentIndex, KMLUtil.SPIRAL_RADIUS), KMLUtil.RADIUS);
              sentIndex++;
          }
      }

    private String okChars = "!?:;,'\"|+=-_*^%$#@`~(){}[]\\/";

    private boolean isSanitary(String s) {
        for(int i = 0; i < s.length(); i++) {
            char curChar = s.charAt(i);
            if(!Character.isLetterOrDigit(curChar) && !okChars.contains(curChar + "")) {
                return false;
            }
        }
        return true;
    }

      protected void write(XMLStreamWriter out) throws Exception {

          KMLUtil.writeHeader(out, "corpus");
          
          for(Document<? extends Token> doc : corpus) {
              writeDocument(out, doc);
          }

          KMLUtil.writeFooter(out);

          out.close();
      }
}
