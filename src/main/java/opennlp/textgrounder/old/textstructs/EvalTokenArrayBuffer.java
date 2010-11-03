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

import java.util.ArrayList;

import opennlp.textgrounder.old.textstructs.older.Lexicon;
import opennlp.textgrounder.old.topostructs.*;

/**
 * Subclass of TokenArrayBuffer that also holds the disambiguated Locations for
 * each toponym. We store both the model's guess for the Location and the actual
 * Location as determined from gold-standard data.
 * 
 * @author tsmoon
 */
public class EvalTokenArrayBuffer extends TokenArrayBuffer {

    /**
     * Stores the system/model's best guesses for the disambiguated Location for each
     * toponym. Null for non-toponym indices.
     */
    public ArrayList<Location> modelLocationArrayList;
    /**
     * Stores the gold standard location information when running in evaluation mode.
     * Indices corresponding to non-toponyms are null.
     */
    public ArrayList<Location> goldLocationArrayList;

    /**
     * Default constructor. Allocates memory for arrays and assigns lexicon.
     * FIXME: Not necessary, delete me.  See below.
     *
     * @param lexicon
     */
    public EvalTokenArrayBuffer(Lexicon lexicon) {
        super();
        initialize(lexicon);
    }

    /**
     * Allocation of fields, initialization of values and object assignments.
     *
     * @param lexicon
     */
    @Override
    protected void initialize(Lexicon lexicon) {
        super.initialize(lexicon);
        modelLocationArrayList = new ArrayList<Location>();
        goldLocationArrayList = new ArrayList<Location>();
    }

    /**
     * Same as superclass version but also adds the passed-in gold standard
     * Location (which disambiguates the toponym). FIXME: No method to set the
     * model's guess, presumably is handled by `modelLocationArrayList' being
     * public.
     * 
     * @param wordIdx
     * @param docIdx
     * @param topStatus
     * @param stopStatus
     * @param loc
     */
    @Override
    public void addElement(int wordIdx, int docIdx, int topStatus,
          Location loc) {
        super.addElement(wordIdx, docIdx, topStatus, null);
        goldLocationArrayList.add(loc);
    }

    /**
     * Used by assert()s in TextProcessorTR.
     * FIXME: Ugly.
     * 
     * @return
     */
    @Override
    protected boolean sanityCheck2() {
        return toponymArrayList.size() == goldLocationArrayList.size();
    }

    /**
     * Used by assert()s in TextProcessorTR.
     * FIXME: Ugly.
     * 
     * @return
     */
    @Override
    protected boolean verboseSanityCheck(String curLine) {
        if (Math.abs(toponymArrayList.size() - goldLocationArrayList.size()) > 1) {
            System.out.println(curLine);
            System.out.println("toponym: " + toponymArrayList.size());
            System.out.println("word: " + wordArrayList.size());
            System.out.println("gold: " + goldLocationArrayList.size());
            System.exit(0);
            return false;
        }
        return true;
    }
}
