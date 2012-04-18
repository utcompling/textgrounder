package opennlp.textgrounder.tr.text.prep;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import opennlp.textgrounder.tr.text.Corpus;
import opennlp.textgrounder.tr.text.Document;
import opennlp.textgrounder.tr.text.DocumentSource;
import opennlp.textgrounder.tr.text.DocumentSourceWrapper;
import opennlp.textgrounder.tr.text.Sentence;
import opennlp.textgrounder.tr.text.SimpleSentence;
import opennlp.textgrounder.tr.text.SimpleToponym;
import opennlp.textgrounder.tr.text.Token;
import opennlp.textgrounder.tr.text.Toponym;
import opennlp.textgrounder.tr.topo.gaz.Gazetteer;
import opennlp.textgrounder.tr.topo.Location;
import opennlp.textgrounder.tr.util.Span;


public class CandidateRepopulator extends DocumentSourceWrapper {

  private final Gazetteer gazetteer;

    public CandidateRepopulator(DocumentSource source, Gazetteer gazetteer) {
    super(source);
    this.gazetteer = gazetteer;
  }

  public Document<Token> next() {
    final Document<Token> document = this.getSource().next();
    final Iterator<Sentence<Token>> sentences = document.iterator();

    return new Document<Token>(document.getId()) {
      private static final long serialVersionUID = 42L;
      public Iterator<Sentence<Token>> iterator() {
        return new SentenceIterator() {
          public boolean hasNext() {
            return sentences.hasNext();
          }

          public Sentence<Token> next() {
            Sentence<Token> sentence = sentences.next();
            for(Token token : sentence) {
                if(token.isToponym()) {
                    Toponym toponym = (Toponym) token;
                    List<Location> candidates = gazetteer.lookup(toponym.getForm());
                    if(candidates == null) candidates = new ArrayList<Location>();
                    toponym.setCandidates(candidates);
                    toponym.setGoldIdx(-1);
                }
            }
            return sentence;
            //return new SimpleSentence(sentence.getId(), sentence.getTokens());
          }
        };
      }
    };
  }
}

