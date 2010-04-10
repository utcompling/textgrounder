package opennlp.textgrounder.io;

import java.util.*;

import gnu.trove.*;

import edu.stanford.nlp.ling.CoreAnnotations.*;

import opennlp.textgrounder.topostructs.*;

/**
 * Class of documents as array of array of integers (i.e. indexes of word types).
 *
 * @author
 */
public class DocumentSet extends ArrayList<ArrayList<Integer>> {

    /**
     *
     */
    public TObjectIntHashMap<String> wordsToInts = new TObjectIntHashMap<String>();
    /**
     *
     */
    public TIntObjectHashMap<String> intsToWords = new TIntObjectHashMap<String>();
    /**
     * 
     */
    protected int nextInt = 0;
    /**
     * The number of paragraphs to treat as a single document.
     */
    protected int parAsDocSize;
    /**
     * 
     */
    protected ArrayList<Integer> currentDoc;

    /**
     * Default constructor. In this case, parAsDocSize is set to the maximum
     * value of the Integer type. This means that a single document is treated
     * as a single document. The constructor is only provided for completeness.
     * The constructor {@link DocumentSet#DocumentSet(int)} should be preferred
     * since it handles cases where a single document is a single document as
     * well as other cases. This simplifies the interface.
     */
    public DocumentSet() {
        parAsDocSize = Integer.MAX_VALUE;
    }

    /**
     * Constructor where parAsDocSize is specified. If parAsDocSize is handed as
     * 0, a single document is treated as a single document. Otherwise, parAsDocSize
     * is set from the commandline.
     *
     * @param parAsDocSize
     */
    public DocumentSet(int parAsDocSize) {
        if (parAsDocSize == 0) {
            this.parAsDocSize = Integer.MAX_VALUE;
        } else {
            this.parAsDocSize = parAsDocSize;
        }
    }

    public String getWordForInt(int someInt) {
        return intsToWords.get(someInt);
    }

    public int getIntForWord(String someWord) {
        return wordsToInts.get(someWord);
    }

    /**
     * Add word to sequence of indices in DocumentSet. If the word does not
     * exist in the dictionary, add it to the dictionary as well.
     *
     * @param word
     * @return index of the word that has been added
     */
    public int addWordToSeq(String word) {
        int idx = 0;
        if (!wordsToInts.containsKey(word)) {
            wordsToInts.put(word, nextInt);
            intsToWords.put(nextInt, word);
            idx = nextInt;
            nextInt += 1;
        } else {
            idx = wordsToInts.get(word);
        }
        currentDoc.add(idx);
        return idx;
    }

    /**
     * Return the context (snippet) of up to window size n for the given doc id and index
     */
    public String getContext(DocIdAndIndex dii, int windowSize) {
	String context = "";

	ArrayList<Integer> curDoc = this.get(dii.docId);

	int i = dii.docIndex - windowSize;
	if(i < 0) i = 0; // re-initialize the start of the window to be the start of the document if window too big
	for(; i < dii.docIndex + windowSize; i++) {
	    if(i >= curDoc.size()) break;

	    context += " " + getWordForInt(curDoc.get(i));
	}

	return context.trim();
    }

    /**
     * Add a new document to DocumentSet
     */
    public void newDoc() {
        currentDoc = new ArrayList<Integer>();
        add(currentDoc);
    }

    /**
     * Size of current dictionary
     * 
     * @return size of dictionary
     */
    public int getDictionarySize() {
        return nextInt;
    }

    /**
     * @return the parAsDocSize
     */
    public int getParAsDocSize() {
        return parAsDocSize;
    }
}
