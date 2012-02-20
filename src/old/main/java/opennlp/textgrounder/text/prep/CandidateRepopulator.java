package opennlp.textgrounder.text.prep;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import opennlp.textgrounder.text.Corpus;
import opennlp.textgrounder.text.Document;
import opennlp.textgrounder.text.DocumentSource;
import opennlp.textgrounder.text.DocumentSourceWrapper;
import opennlp.textgrounder.text.Sentence;
import opennlp.textgrounder.text.SimpleSentence;
import opennlp.textgrounder.text.SimpleToponym;
import opennlp.textgrounder.text.Token;
import opennlp.textgrounder.text.Toponym;
import opennlp.textgrounder.topo.gaz.Gazetteer;
import opennlp.textgrounder.topo.Location;
import opennlp.textgrounder.util.Span;


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

