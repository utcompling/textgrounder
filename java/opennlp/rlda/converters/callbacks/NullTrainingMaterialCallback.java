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

package opennlp.rlda.converters.callbacks;

import opennlp.wrapper.rlda.textstructs.Lexicon;

/**
 * An empty placeholder class for projects that do not require validation
 * of training material
 * 
 * @author tsmoon
 */
public class NullTrainingMaterialCallback extends TrainingMaterialCallback {

    public NullTrainingMaterialCallback(Lexicon lexicon) {
        super(lexicon);
    }

    /**
     * returns true all the time.
     * 
     * @param word
     * @return
     */
    @Override
    public boolean isTrainable(String word) {
        return true;
    }
}
