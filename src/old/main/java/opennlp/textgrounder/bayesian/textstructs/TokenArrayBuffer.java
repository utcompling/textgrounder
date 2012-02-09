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

import java.io.*;

import java.util.ArrayList;

/**
 * Class of integer sequences that indicate words in a stream of space
 * delimited tokens. The lengths of all sequences in this class are identical.
 * Each element of the same offset in each sequence contains different
 * pieces of information about the same
 * 
 * @author tsmoon
 */
public class TokenArrayBuffer implements Serializable {

    private static final long serialVersionUID = 42L;

    /**
     * Array of word indexes. The elements are integers which reference word
     * types maintained in Lexicon. The "word types" may not be unigrams.
     * If they are multiword placenames, the index will still be unary, but
     * the lexicon will reference space delimited multiple tokens.
     */
    public ArrayList<Integer> wordArrayList;
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
     * Array of stopword indicators. It is populated only with ones and zeros.
     * For a given element, if the corresponding token in wordArrayList is a
     * stopword, the element is one, zero otherwise.
     */
    public ArrayList<Integer> stopwordArrayList;
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
     * Constructor for derived classes only
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
    protected final void initialize(Lexicon lexicon) {
        wordArrayList = new ArrayList<Integer>();
        documentArrayList = new ArrayList<Integer>();
        toponymArrayList = new ArrayList<Integer>();
        stopwordArrayList = new ArrayList<Integer>();
        size = 0;

        this.lexicon = lexicon;
    }

    /**
     * Add all indexes and indicators to the array fields and increment size
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
    public void addElement(int wordIdx, int docIdx, int topStatus,
          int stopStatus) {
        wordArrayList.add(wordIdx);
        documentArrayList.add(docIdx);
        toponymArrayList.add(topStatus);

        stopwordArrayList.add(stopStatus);
        size += 1;
        numDocs = docIdx;
    }

    /**
     * @return the size of the arrays in class. In other words, the size
     * in tokens of all documents combined (give or take some words, since
     * multiword placenames are considered to be a single token)
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
}
