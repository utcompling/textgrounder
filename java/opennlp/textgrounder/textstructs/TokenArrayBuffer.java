///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
//
//  This library is free software; you can redistribute it and/or
//  modify it under the terms of the GNU Lesser General Public
//  License as published by the Free Software Foundation; either
//  version 3 of the License, or (at your option) any later version.
//
//  This library is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public
//  License along with this program; if not, write to the Free Software
//  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.textstructs;

import java.util.ArrayList;
import java.util.List;

/**
 * Class of integer sequences that indicate words in a stream of space
 * delimited tokens. The lengths of all sequences in this class are identical.
 * Each element of the same offset in each sequence contains different
 * pieces of information about the same
 * 
 * @author tsmoon
 */
public class TokenArrayBuffer {

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
     * Populated with the same elements as stopwordArrayList. Once input has been
     * populated in input stage with stopwordArrayList, the arraylist is converted
     * to this primitive array for memory and speed.
     */
    public int[] stopwordVector;
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
     * Default constructor. Allocates memory for arrays and assigns lexicon.
     *
     * @param lexicon
     */
    public TokenArrayBuffer(Lexicon lexicon) {
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
        System.err.println("Converting arrays");
        System.err.println("Allocating word vector");
        wordVector = new int[size];
        System.err.println("Copying word vector");
        copyToArray(wordVector, wordArrayList);
        System.err.println("Allocating document vector");
        documentVector = new int[size];
        System.err.println("Copying document vector");
        copyToArray(documentVector, documentArrayList);
        System.err.println("Allocating toponym vector");
        toponymVector = new int[size];
        System.err.println("Copying toponym vector");
        copyToArray(toponymVector, toponymArrayList);
        System.err.println("Allocating stopword vector");
        stopwordVector = new int[size];
        System.err.println("Copying stopword vector");
        copyToArray(stopwordVector, stopwordArrayList);

        wordArrayList = documentArrayList = toponymArrayList = stopwordArrayList = null;
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

    /**
     * Copy a sequence of numbers from ta to array ia.
     *
     * @param <T>   Any number type
     * @param ia    Target array of integers to be copied to
     * @param ta    Source List<T> of numbers to be copied from
     */
    protected static <T extends Number> void copyToArray(int[] ia, List<T> ta) {
        for (int i = 0; i < ta.size(); ++i) {
            ia[i] = ta.get(i).intValue();
        }
    }
}
