///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.textstructs;

import gnu.trove.*;

import edu.stanford.nlp.ling.CoreAnnotations.*;
import java.io.Serializable;

/**
 * Class of documents as array of array of integers (i.e. indexes of word types).
 *
 * @author
 */
public class Lexicon implements Serializable {

    static private final long serialVersionUID = 6390098L;
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
