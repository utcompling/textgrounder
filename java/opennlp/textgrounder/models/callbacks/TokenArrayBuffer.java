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
package opennlp.textgrounder.models.callbacks;

import java.util.ArrayList;
import opennlp.textgrounder.io.*;

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
     * types maintained in DocumentSet. The "word types" may not be unigrams.
     * If they are multiword placenames, the index will still be unary, but
     * the lexicon will reference space delimited multiple tokens.
     */
    public ArrayList<Integer> wordVector;
    /**
     * Array of document indexes. Each element references the document which
     * the corresponding token in wordVector was located in. It is a monotonic
     * sequence, e.g. <p>0,0,...,0,1,1,1,1...,1,2,....,D</p> where <p>D</p>
     * is the number of documents.
     */
    public ArrayList<Integer> documentVector;
    /**
     * Array of toponym indicators. It is populated only with ones and zeros.
     * For a given element, if the corresponding token in wordVector is a 
     * toponym, the element is one, zero otherwise.
     */
    public ArrayList<Integer> toponymVector;
    /**
     * Array of stopword indicators. It is populated only with ones and zeros.
     * For a given element, if the corresponding token in wordVector is a
     * stopword, the element is one, zero otherwise.
     */
    public ArrayList<Integer> stopwordVector;
    /**
     * The size of the the array fields in this class. All arrays have the
     * same size.
     */
    protected int size;
    /**
     * The lexicon of token indexes to tokens.
     */
    protected DocumentSet docSet;

    /**
     * Default constructor. Allocates memory for arrays and assigns docSet.
     *
     * @param docSet
     */
    public TokenArrayBuffer(DocumentSet docSet) {
        wordVector = new ArrayList<Integer>();
        documentVector = new ArrayList<Integer>();
        toponymVector = new ArrayList<Integer>();
        stopwordVector = new ArrayList<Integer>();
        size = 0;

        this.docSet = docSet;
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
        wordVector.add(wordIdx);
        documentVector.add(docIdx);
        toponymVector.add(topStatus);
        stopwordVector.add(stopStatus);
        size += 1;
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
                context += "<b> " + docSet.getWordForInt(wordVector.get(i)) + "</b> ";
            } else {
                context += docSet.getWordForInt(wordVector.get(i)) + " ";
            }
            //System.out.println("i: " + i + "; docSet thinks " + wordVector.get(i) + " translates to " + docSet.getWordForInt(wordVector.get(i)));
        }

        return context.trim() + "...";
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
     * @param docSet the docSet to set
     */
    public void setDocSet(DocumentSet docSet) {
        this.docSet = docSet;
    }
}
