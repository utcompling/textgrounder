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
 *
 * @author tsmoon
 */
public class TokenArrayBuffer {

    public ArrayList<Integer> wordVector;
    public ArrayList<Integer> docVector;
    public ArrayList<Integer> toponymVector;

    public DocumentSet docSet;

    public TokenArrayBuffer(DocumentSet docSet) {
        wordVector = new ArrayList<Integer>();
        docVector = new ArrayList<Integer>();
        toponymVector = new ArrayList<Integer>();

	this.docSet = docSet;
    }

    public TokenArrayBuffer() {
        wordVector = new ArrayList<Integer>();
        docVector = new ArrayList<Integer>();
        toponymVector = new ArrayList<Integer>();
	
	this.docSet = null;
    }

    public void addWord(int wordIdx) {
        wordVector.add(wordIdx);
    }

    public void addDoc(int docIdx) {
        docVector.add(docIdx);
    }

    public void addToponym(int topIdx) {
        toponymVector.add(topIdx);
    }

    /**
     * Return the context (snippet) of up to window size n for the given doc id and index
     */
    public String getContextAround(int index, int windowSize, boolean boldToponym) {
	String context = "...";

	int i = index - windowSize;
	if(i < 0) i = 0; // re-initialize the start of the window to be the start of the document if window too big
	for(; i < index + windowSize; i++) {
	    if(i >= wordVector.size()) break;
	    if(boldToponym && i == index)
		context += "<b> " + docSet.getWordForInt(wordVector.get(i)) + "</b>";
	    else
		context += docSet.getWordForInt(wordVector.get(i)) + " ";
	    //System.out.println("i: " + i + "; docSet thinks " + wordVector.get(i) + " translates to " + docSet.getWordForInt(wordVector.get(i)));
	}

	return context.trim() + "...";
    }
}
