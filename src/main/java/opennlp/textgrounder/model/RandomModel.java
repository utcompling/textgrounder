/*
 * Random baseline model. Selects a random location for each toponym.
 */

package opennlp.textgrounder.model;

import opennlp.textgrounder.text.*;
import java.util.*;

public class RandomModel extends Model {

    private Random rand = new Random();

    @Override
    public Corpus disambiguate(Corpus corpus) {

        for(Document doc : corpus) {
            for(Sentence sent : doc) {
                for(Token token : sent) {
                    if(token.isToponym()) {
                        Toponym toponym = (Toponym) token;
                        
                        toponym.setSelected(rand.nextInt(toponym.getAmbiguity()));
                    }
                }
            }
        }
        
        return corpus;
    }    
}
