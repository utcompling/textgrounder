/*
 * Random baseline model. Selects a random location for each toponym.
 */

package opennlp.textgrounder.model;

import opennlp.textgrounder.text.*;
import java.util.*;

public class RandomModel extends Model {

    private Random rand = new Random();

    @Override
    public Corpus<Token> disambiguate(Corpus<Token> corpus) {

        for(Document<Token> doc : corpus) {
            for(Sentence<Token> sent : doc) {
                for(Token token : sent) {
                    if(token.isToponym()) {
                        Toponym toponym = (Toponym) token;
                        
                        toponym.setSelectedIdx(rand.nextInt(toponym.getAmbiguity()));
                    }
                }
            }
        }
        
        return corpus;
    }    
}
