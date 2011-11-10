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
package opennlp.textgrounder.bayesian.textstructs;

import java.util.*;

import java.io.Serializable;

/**
 * Class of documents as array of array of integers (i.e. indexes of word types).
 *
 * @author
 */
public class Lexicon implements Serializable {

    private static final long serialVersionUID = 42L;

    /**
     * Map from token to integer (index). Tokens might not be unigrams. Placename
     * tokens may be one or more words.
     */
    public HashMap<String, Integer> wordsToInts = new HashMap<String, Integer>();
    /**
     * Map from index to token.
     */
    public HashMap<Integer, String> intsToWords = new HashMap<Integer, String>();
    /**
     * Current size of vocabulary. Every new word entered into the lexicon
     * assumes this as the index, and nextInt is incremented.
     */
    protected int nextInt = 0;

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
     * Add word to Lexicon and return the index value of the word. If the word
     * already exists in the dictionary, retrieve the index and its value.
     *
     * @param word string to add and/or retrieve
     * @return index of the word that has been added or is in the lexicon
     */
    public int addOrGetWord(String word) {
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
     * Check whether word is in lexicon
     * 
     * @param word word to examine
     * @return truth value of whether word is in lexicon
     */
    public boolean contains(String word) {
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
