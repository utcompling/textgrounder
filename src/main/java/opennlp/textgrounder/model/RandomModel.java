/*
 * Random baseline model. Selects a random location for each toponym.
 */

package opennlp.textgrounder.model;

import opennlp.textgrounder.text.*;
import java.util.*;

public class RandomModel extends Model {

    private Random rand = new Random();

    @Override
    public StoredCorpus disambiguate(StoredCorpus corpus) {

        for(Document<StoredToken> doc : corpus) {
            for(Sentence<StoredToken> sent : doc) {
                for(Toponym toponym : sent.getToponyms()) {
                    int ambiguity = toponym.getAmbiguity();
                    if (ambiguity > 0) {
                        toponym.setSelectedIdx(rand.nextInt(ambiguity));
                    }
                }
            }
        }
        
        return corpus;
    }    
}
