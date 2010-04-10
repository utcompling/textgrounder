package opennlp.textgrounder.io;

import java.io.*;
import java.util.*;

import gnu.trove.*;

import edu.stanford.nlp.ling.CoreAnnotations.*;
import java.util.ArrayList;

/**
 * Class of documents as array of array of integers (i.e. indexes of word types).
 * It can also handle a single large document as a collection of smaller documents
 * where the documents are actually a collection of N (parAsDocSize) paragraphs.
 *
 * @author
 */
public class DocumentSet extends ArrayList<ArrayList<Integer>> {

    public TObjectIntHashMap<String> wordsToInts = new TObjectIntHashMap<String>();
    public TIntObjectHashMap<String> intsToWords = new TIntObjectHashMap<String>();
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
     * Get a single document as a single string.
     * 
     * @param idx Index of document to obtain
     * @return text of document as string.
     */
    public String getDocumentAsString(int idx) {
        ArrayList<Integer> doc = get(idx);
        StringBuffer buff = new StringBuffer();
        for (int tok : doc) {
            buff.append(getWordForInt(tok));
            buff.append(" ");
        }
        return buff.toString();
    }

    /**
     * Add word to hash tables. To be used in
     * external classes in post-initialization addition of placenames.
     * 
     * @param word word to add
     */
    public void addWord(String word) {
        wordsToInts.put(word, nextInt);
        intsToWords.put(nextInt, word);
        nextInt++;
    }

    /**
     * Add word to sequence of indices in DocumentSet. If the word does not
     * exist in the dictionary, add it to the dictionary as well.
     *
     * @param word
     */
    public void addWordToSeq(String word) {
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
    }

    /**
     * Add a new document to DocumentSet
     */
    public void newDoc() {
        currentDoc = new ArrayList<Integer>();
        add(currentDoc);
    }

    /**
     * Checks whether word is in keys of wordsToInts or not. To be used in
     * external classes in post-initialization addition of placenames.
     *
     * @param word word to check for existence
     * @return truth value of whether word exists or not
     */
    public boolean hasWord(String word) {
        return wordsToInts.containsKey(word);
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
