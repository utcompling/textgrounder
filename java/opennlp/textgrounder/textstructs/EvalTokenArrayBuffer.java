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

import java.util.ArrayList;
import java.util.List;
import opennlp.textgrounder.models.callbacks.NullTrainingMaterialCallback;
import opennlp.textgrounder.models.callbacks.TrainingMaterialCallback;

import opennlp.textgrounder.topostructs.Location;

/**
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
     *
     * @param lexicon
     */
    public EvalTokenArrayBuffer(Lexicon lexicon) {
        super();
        initialize(lexicon, new NullTrainingMaterialCallback(lexicon));
    }

    /**
     * Default constructor. Allocates memory for arrays and assigns lexicon.
     *
     * @param lexicon
     */
    public EvalTokenArrayBuffer(Lexicon lexicon,
          TrainingMaterialCallback trainingMaterialCallback) {
        super();
        initialize(lexicon, trainingMaterialCallback);
    }

    /**
     * Allocation of fields, initialization of values and object assignments.
     *
     * @param lexicon
     */
    @Override
    protected void initialize(Lexicon lexicon,
          TrainingMaterialCallback trainingMaterialCallback) {
        super.initialize(lexicon, trainingMaterialCallback);
        modelLocationArrayList = new ArrayList<Location>();
        goldLocationArrayList = new ArrayList<Location>();
    }

    /**
     * 
     * @param wordIdx
     * @param docIdx
     * @param topStatus
     * @param stopStatus
     * @param loc
     */
    @Override
    public void addElement(int wordIdx, int docIdx, int topStatus,
          int stopStatus, Location loc) {
        super.addElement(wordIdx, docIdx, topStatus, stopStatus, null);
        goldLocationArrayList.add(loc);
    }

    /**
     * 
     * @return
     */
    @Override
    protected boolean sanityCheck2() {
        return toponymArrayList.size() == goldLocationArrayList.size();
    }

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
