/*
 * This version of Model (started 9/22/10) is just an abstract class with the disambiguate(Corpus) method.
 */

package opennlp.textgrounder.model;

import opennlp.textgrounder.text.StoredCorpus;

/**
 * @param corpus
 *        a corpus without any selected candidates for each toponym (or ignores the selections if they are present)
 * @return
 *        a corpus with selected candidates, ready for evaluation
 */
public abstract class Model {

    public abstract StoredCorpus disambiguate(StoredCorpus corpus);
    
}
