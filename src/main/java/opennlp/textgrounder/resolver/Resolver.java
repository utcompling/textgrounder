/*
 * This version of Resolver (started 9/22/10) is just an abstract class with the disambiguate(Corpus) method.
 */

package opennlp.textgrounder.resolver;

import opennlp.textgrounder.text.StoredCorpus;

/**
 * @param corpus
 *        a corpus without any selected candidates for each toponym (or ignores the selections if they are present)
 * @return
 *        a corpus with selected candidates, ready for evaluation
 */
public abstract class Resolver {

    public void train(StoredCorpus corpus) {
        throw new UnsupportedOperationException("This type of resolver cannot be trained.");
    }

    public abstract StoredCorpus disambiguate(StoredCorpus corpus);
    
}
