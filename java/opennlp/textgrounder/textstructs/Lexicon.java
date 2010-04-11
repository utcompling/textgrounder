package opennlp.textgrounder.textstructs;

import gnu.trove.*;

import edu.stanford.nlp.ling.CoreAnnotations.*;

/**
 * Class of documents as array of array of integers (i.e. indexes of word types).
 *
 * @author
 */
public class Lexicon {

    /**
     * Map from token to integer (index). Tokens might not be unigrams. Placename
     * tokens may be one or more words.
     */
    public TObjectIntHashMap<String> wordsToInts = new TObjectIntHashMap<String>();
    /**
     * Map from index to token.
     */
    public TIntObjectHashMap<String> intsToWords = new TIntObjectHashMap<String>();
    /**
     * Current size of vocabulary. Every new word entered into the lexicon
     * assumes this as the index, and nextInt is incremented.
     */
    protected int nextInt = 0;

    /**
     * Default constructor. Does nothing.
     */
    public Lexicon() {
    }

    /**
     * Get string value of some index
     *
     * @param someInt index to look up
     * @return the string corresponding to the index in intsToWords
     */
    public String getWordForInt(int someInt) {
        return intsToWords.get(someInt);
    }

    /**
     * Get index value of some string
     *
     * @param someWord the token to look up
     * @return the index corresponding to the string in wordsToInts
     */
    public int getIntForWord(String someWord) {
        return wordsToInts.get(someWord);
    }

    /**
     * Add word to sequence of indices in Lexicon. If the word does not
     * exist in the dictionary, add it to the dictionary as well.
     *
     * @param word
     * @return index of the word that has been added
     */
    public int addWord(String word) {
        int idx = 0;
        if (!wordsToInts.containsKey(word)) {
            wordsToInts.put(word, nextInt);
            intsToWords.put(nextInt, word);
            idx = nextInt;
            nextInt += 1;
        } else {
            idx = wordsToInts.get(word);
        }
        return idx;
    }

    /**
     * Size of current dictionary
     * 
     * @return size of dictionary
     */
    public int getDictionarySize() {
        return nextInt;
    }
}
