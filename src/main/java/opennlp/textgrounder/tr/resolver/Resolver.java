/*
 * This version of Resolver (started 9/22/10) is just an abstract class with the disambiguate(Corpus) method.
 */

package opennlp.textgrounder.tr.resolver;

import opennlp.textgrounder.tr.text.*;

/**
 * @param corpus
 *        a corpus without any selected candidates for each toponym (or ignores the selections if they are present)
 * @return
 *        a corpus with selected candidates, ready for evaluation
 */
public abstract class Resolver {

    // Make this false to have a resolver only resolve toponyms that don't already have a selected candidate
    // (not implemented in all resolvers yet)
    public boolean overwriteSelecteds = true;

    public void train(StoredCorpus corpus) {
        throw new UnsupportedOperationException("This type of resolver cannot be trained.");
    }

    public abstract StoredCorpus disambiguate(StoredCorpus corpus);
    
}
