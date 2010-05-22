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
package opennlp.textgrounder.models;

import gnu.trove.TIntIntHashMap;
import opennlp.textgrounder.models.callbacks.RegionMapperCallback;
import opennlp.textgrounder.topostructs.Region;

/**
 *
 * @author tsmoon
 */
public class RegionModelBridge {

    /**
     *
     */
    protected int[] regionIdToRegionId;
    /**
     *
     */
    protected int[] wordIdToWordId;
    /**
     * 
     */
    protected RegionModel from, to;

    /**
     *
     * @param from
     * @param to
     */
    public RegionModelBridge(RegionModel from, RegionModel to) {

        this.from = from;
        this.to = to;
    }

    /**
     * 
     */
    public int[] matchRegionId() {
        RegionMapperCallback rm1 = from.regionMapperCallback;
        RegionMapperCallback rm2 = to.regionMapperCallback;

        regionIdToRegionId = new int[from.T];

        for (int w = 0; w < to.regionArrayWidth; w++) {
            for (int h = 0; h < to.regionArrayHeight; h++) {
                Region r1 = from.regionArray[w][h];
                Region r2 = to.regionArray[w][h];
                if (r1 != null) {
                    int i1 = rm1.getReverseRegionMap().get(r1);
                    if (r2 != null) {
                        int i2 = rm2.getReverseRegionMap().get(r2);
                        regionIdToRegionId[i1] = i2;
                    } else {
                        regionIdToRegionId[i1] = -1;
                    }
                }
            }
        }

        return regionIdToRegionId;
    }

    /**
     * 
     * @return
     */
    public int[] matchWordId() {
        wordIdToWordId = new int[from.lexicon.getDictionarySize()];

        for (int i = 0;
              i < from.lexicon.getDictionarySize(); ++i) {
            String word = from.lexicon.getWordForInt(i);
            if (to.lexicon.contains(word)) {
                wordIdToWordId[i] = to.lexicon.getIntForWord(word);
            } else {
                wordIdToWordId[i] = -1;
            }
        }

        return wordIdToWordId;
    }

    /**
     * @return the regionIdToRegionId
     */
    public int[] getRegionIdToRegionId() {
        return regionIdToRegionId;
    }

    /**
     * @return the wordIdToWordId
     */
    public int[] getWordIdToWordId() {
        return wordIdToWordId;
    }
}
