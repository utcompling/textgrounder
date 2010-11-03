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
package opennlp.textgrounder.old.textstructs;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import opennlp.textgrounder.old.textstructs.older.Lexicon;
import opennlp.textgrounder.old.topostructs.*;

/**
 * Class that stores data about a sequence of tokens. All tokens in all
 * documents are concatenated into a single TokenArrayBuffer. Logically the data
 * is a list of objects, one per token. For efficiency purposes, however, we
 * store the data as separate sequences of integers, one per specific piece of
 * info about the token. The lengths of all sequences in this class are
 * identical.
 * 
 * NOTE: A single "token" may correspond to multiple words, particularly in the
 * case of multi-word place names. FIXME: What process splits up the word
 * sequence into tokens?
 * 
 * FIXME: Is it really true that all arrays are the same length?
 * `trainingArrayList' is a list of tokens with numerals etc. removed; surely
 * this is shorter?
 * 
 * @author tsmoon
 */
public class TokenArrayBuffer implements Serializable {

    static private final long serialVersionUID = 10772114L;
    /**
     * Array of word indexes. The elements are integers which reference word
     * types maintained in Lexicon. The "word types" might not be unigrams.
     * If they are multiword placenames, the index will still be unary, but
     * the lexicon will reference space delimited multiple tokens.
     */
    public ArrayList<Integer> wordArrayList;
    /**
     * List of token indices removed of numerals (for now) and (in the future)
     * maybe hapax legomena and words with punctuation in 'em
     */
    public ArrayList<Integer> trainingArrayList;
    /**
     * Array of document indexes. Each element references the document which
     * the corresponding token in wordArrayList was located in. It is a monotonic
     * sequence, e.g. <p>0,0,...,0,1,1,1,1...,1,2,....,D</p> where <p>D</p>
     * is the number of documents.
     */
    public ArrayList<Integer> documentArrayList;
    /**
     * Array of toponym indicators. It is populated only with ones and zeros.
     * For a given element, if the corresponding token in wordArrayList is a
     * toponym, the element is one, zero otherwise.
     */
    public ArrayList<Integer> toponymArrayList;
    /**
     * Populated with the same elements as wordArrayList. Once input has been
     * populated in input stage with wordArrayList, the arraylist is converted
     * to this primitive array for memory and speed.
     */
    public int[] wordVector;
    /**
     * Populated with the same elements as documentArrayList. Once input has been
     * populated in input stage with documentArrayList, the arraylist is converted
     * to this primitive array for memory and speed.
     */
    public int[] documentVector;
    /**
     * Populated with the same elements as toponymArrayList. Once input has been
     * populated in input stage with toponymArrayList, the arraylist is converted
     * to this primitive array for memory and speed.
     */
    public int[] toponymVector;
    /**
     * The size of the the array fields in this class. All arrays have the
     * same size.
     */
    protected int size;
    /**
     * The total number of documents in the input
     */
    protected int numDocs;
    /**
     * The lexicon of token indexes to tokens.
     */
    protected Lexicon lexicon;

    /**
     * Constructor for derived classes only.  FIXME: Not necessary, delete me.
     */
    protected TokenArrayBuffer() {
    }

    /**
     * Default constructor. Allocates memory for arrays and assigns lexicon.
     *
     * @param lexicon
     */
    public TokenArrayBuffer(Lexicon lexicon) {
        initialize(lexicon);
    }

    /**
     * Allocation of fields, initialization of values and object assignments.
     *
     * @param lexicon
     */
    protected void initialize(Lexicon lexicon) {
        wordArrayList = new ArrayList<Integer>();
        documentArrayList = new ArrayList<Integer>();
        toponymArrayList = new ArrayList<Integer>();
        size = 0;

        this.lexicon = lexicon;
    }

    /**
     * Concatenates another TokenArrayBuffer onto this one and returns a new,
     * combined TokenArrayBuffer. For use when an evaluation TokenArrayBuffer
     * and additional unlabeled training data are desired.
     * 
     * @param otherTokenArrayBuffer
     *            the TokenArrayBuffer to be concatenated with this
     * @return the concatenated TokenArrayBuffer
     */
    @Deprecated
    public TokenArrayBuffer concatenate(TokenArrayBuffer otherTokenArrayBuffer) {
        TokenArrayBuffer toReturn = new TokenArrayBuffer(this.lexicon.concatenate(otherTokenArrayBuffer.lexicon));

        int docNumOffset = 0;
        for(int i = 0; i < this.size(); i++) {
            toReturn.addElement(this.wordVector[i], this.documentVector[i], this.toponymVector[i]);
        }
        docNumOffset = this.documentVector[this.size()-1] + 1; // need to continue document numbers where they left off, not restart at 0
        for(int i = 0; i < otherTokenArrayBuffer.size(); i++)
            toReturn.addElement(otherTokenArrayBuffer.wordVector[i], otherTokenArrayBuffer.documentVector[i] + docNumOffset,
                    otherTokenArrayBuffer.toponymVector[i]);

        toReturn.convertToPrimitiveArrays();

        return toReturn;
    }

    /**
     * Add a single logical element (i.e. all info associated with a token).
     * Adds all indexes and indicators to the array fields and increments size
     * by one.
     *
     * @param wordIdx index of token being added. may be a placename, a
     * multiword placename, a stopword, or something else.
     * @param docIdx the index of the document that the current token was
     * found in. The index grows by unit increments whenever a new document
     * has been opened.
     * @param topStatus the status of the current (multiword) token as a
     * toponym. This will be one if it is a toponym and zero otherwise.
     * @param stopStatus the status of the current token as a stopword. This
     * will be one if it is a stopword and zero otherwise.
     */
    public void addElement(int wordIdx, int docIdx, int topStatus) {
        addElement(wordIdx, docIdx, topStatus, null);
    }

    /**
     * Add a single logical element (i.e. all info associated with a token).
     * Adds all indexes and indicators to the array fields and increments size
     * by one. Same as four-argument version but takes an additional Location
     * object indicating the resolution of the toponym.
     * 
     * NOTE: The Location param is ignored in this code, but not in overridden
     * versions of this method in subclasses.
     * 
     * @param wordIdx
     *            index of token being added. may be a placename, a multiword
     *            placename, a stopword, or something else.
     * @param docIdx
     *            the index of the document that the current token was found in.
     *            The index grows by unit increments whenever a new document has
     *            been opened.
     * @param topStatus
     *            the status of the current (multiword) token as a toponym. This
     *            will be one if it is a toponym and zero otherwise.
     * @param stopStatus
     *            the status of the current token as a stopword. This will be
     *            one if it is a stopword and zero otherwise.
     */
    public void addElement(int wordIdx, int docIdx, int topStatus,
          Location loc) {
        wordArrayList.add(wordIdx);
        documentArrayList.add(docIdx);
        toponymArrayList.add(topStatus);

        String word = lexicon.getWordForInt(wordIdx);
        size += 1;
        numDocs = docIdx;
    }

    /**
     * Return the context (snippet) of up to window size n for the given doc id and index
     */
    public String getContextAround(int index, int windowSize,
          boolean boldToponym) {
        String context = "...";

        int i = index - windowSize;
        if (i < 0) {
            i = 0; // re-initialize the start of the window to be the start of the document if window too big
        }
        for (; i < index + windowSize; i++) {
            if (i >= size) {
                break;
            }
            if (boldToponym && i == index) {
                context += "<b> " + lexicon.getWordForInt(wordVector[i]) + "</b> ";
            } else {
                context += lexicon.getWordForInt(wordVector[i]) + " ";
            }
            //System.out.println("i: " + i + "; lexicon thinks " + wordArrayList.get(i) + " translates to " + lexicon.getWordForInt(wordArrayList.get(i)));
        }

        return context.trim() + "...";
    }

    /**
     * Call this after initial population to convert arraylists to primitive
     * arrays.
     */
    public void convertToPrimitiveArrays() {
        wordVector = new int[size];
        copyToArray(wordVector, wordArrayList);
        documentVector = new int[size];
        copyToArray(documentVector, documentArrayList);
        toponymVector = new int[size];
        copyToArray(toponymVector, toponymArrayList);

        wordArrayList.clear();
        documentArrayList.clear();
        toponymArrayList.clear();
        wordArrayList = documentArrayList = toponymArrayList = null;
    }

    /**
     * @return the size of the arrays in class. In other words, the size
     * in tokens of all documents combined (give or take some words, since
     * multiword placenames are considered to be a single token).
     */
    public int size() {
        return size;
    }

    /**
     * @return the number of documents in the training data. Need to add one
     * to the output since document counting starts at 0.
     */
    public int getNumDocs() {
        return numDocs + 1;
    }

    /**
     * Copy a sequence of numbers from an array to a list. FIXME: Should be in a
     * util class, not here.
     * 
     * @param <T>
     *            Any number type
     * @param ia
     *            Target array of integers to be copied to
     * @param ta
     *            Source List<T> of numbers to be copied from
     */
    protected static <T extends Number> void copyToArray(int[] ia, List<T> ta) {
        for (int i = 0; i < ta.size(); ++i) {
            ia[i] = ta.get(i).intValue();
        }
    }

    /**
     * Used as part of assert()s in TextProcessorTR. FIXME: This is ugly, should
     * be better way.
     * 
     * @return
     */
    protected boolean sanityCheck1() {
        return toponymArrayList.size() == wordArrayList.size();
    }

    protected boolean sanityCheck2() {
        return true;
    }

    protected boolean verboseSanityCheck(String curLine) {
        return true;
    }

    /**
     * @return the lexicon
     */
    public Lexicon getLexicon() {
        return lexicon;
    }
}
