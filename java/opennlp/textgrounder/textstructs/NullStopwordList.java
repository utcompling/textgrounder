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

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Empty StopwordList class for models that do not require stopword removal.
 * 
 * @author tsmoon
 */
public class NullStopwordList extends StopwordList {

    /**
     * Default constructor.
     * 
     * @throws FileNotFoundException
     * @throws IOException
     */
    public NullStopwordList() throws FileNotFoundException, IOException {
        stopwords = null;
    }

    /**
     * Empty override of isStopWord of base class. It returns false always.
     * No word is a stopword.
     *
     * @param word word to (not) examine
     * @return false always. no word is a stopword.
     */
    @Override
    public boolean isStopWord(String word) {
        return false;
    }

    /**
     * Size of the list of stopwords. Returns 0, meaning there are no stopwords.
     *
     * @return 0
     */
    @Override
    public int size() {
        return 0;
    }
}
