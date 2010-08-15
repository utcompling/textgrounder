///////////////////////////////////////////////////////////////////////////////
// To change this template, choose Tools | Templates
// and open the template in the editor.
///////////////////////////////////////////////////////////////////////////////
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

package opennlp.textgrounder.bayesian.rlda.converters.callbacks;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import opennlp.textgrounder.bayesian.rlda.textstructs.Lexicon;

/**
 * A class for filtering whether words should be considered as training material
 * or not
 * 
 * @author tsmoon
 */
public class TrainingMaterialCallback {

    protected Lexicon lexicon;

    public TrainingMaterialCallback(Lexicon lexicon) {
        this.lexicon = lexicon;
    }

    /**
     * Checks whether the incoming word fits some pattern. For now, checks
     * whether a word has a number in it, then returns false if it does
     * 
     * @param word
     * @return
     */
    public boolean isTrainable(String word) {
        boolean trainable = true;

        Pattern hasNumbers = Pattern.compile("\\d");
        Matcher fit = hasNumbers.matcher(word);
        trainable = !fit.find();
        return trainable;
    }
}
