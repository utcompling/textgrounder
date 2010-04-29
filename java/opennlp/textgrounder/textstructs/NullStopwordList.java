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
