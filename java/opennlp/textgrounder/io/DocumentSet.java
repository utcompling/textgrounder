package opennlp.textgrounder.io;

import java.io.*;
import java.util.*;

import gnu.trove.*;

import edu.stanford.nlp.ling.CoreAnnotations.*;

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

    public void addDocumentFromFile(String locationOfFile) throws Exception {

        BufferedReader textIn = new BufferedReader(new FileReader(locationOfFile));

        System.out.print("Processing document at " + locationOfFile + " ...");

        ArrayList<Integer> curDoc = new ArrayList<Integer>();
        this.add(curDoc);

        int counter = 1;
        String curLine;
        while (true) {
            curLine = textIn.readLine();
            if (curLine == null || curLine.equals("")) {
                break;
            }

            for (String token : curLine.split(" ")) {
                if (wordsToInts.containsKey(token)) {
                    curDoc.add(wordsToInts.get(token));
                } else {
                    wordsToInts.put(token, nextInt);
                    intsToWords.put(nextInt, token);
                    curDoc.add(nextInt);
                    nextInt++;
                }
            }

            if (counter < parAsDocSize) {
                counter++;
            } else {
                counter = 1;
                curDoc = new ArrayList<Integer>();
                this.add(curDoc);
            }
        }

        /**
         * Remove last item if the previous loop did not populate it.
         */
        if (this.get(this.size() - 1).size() == 0) {
            this.remove(this.size() - 1);
        }

        System.out.println("done.");

        textIn.close();
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
}
