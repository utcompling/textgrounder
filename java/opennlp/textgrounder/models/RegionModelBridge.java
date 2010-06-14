///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed _to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.models;

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
    protected int[] fromRegionIdToToRegionIdMap;
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
     * @param _from
     * @param _to
     */
    public RegionModelBridge(RegionModel _from, RegionModel _to) {

        from = _from;
        to = _to;
    }

    /**
     * 
     */
    public int[] matchRegionId() {
        RegionMapperCallback fromRegionMapper = from.regionMapperCallback;
        RegionMapperCallback toRegionMapper = to.regionMapperCallback;

        fromRegionIdToToRegionIdMap = new int[from.T];

        for (int w = 0; w < to.regionArrayWidth; w++) {
            for (int h = 0; h < to.regionArrayHeight; h++) {
                Region fromRegion = from.regionArray[w][h];
                Region toRegion = to.regionArray[w][h];
                if (fromRegion != null) {
                    int fromIdx = fromRegionMapper.getRegionToIdxMap().get(fromRegion);
                    if (toRegion != null) {
                        int toIdx = toRegionMapper.getRegionToIdxMap().get(toRegion);
                        fromRegionIdToToRegionIdMap[fromIdx] = toIdx;
                    } else {
                        fromRegionIdToToRegionIdMap[fromIdx] = -1;
                    }
                }
            }
        }

        return fromRegionIdToToRegionIdMap;
    }

    /**
     * 
     * @return
     */
    public int[] matchWordId() {
        wordIdToWordId = new int[from.lexicon.getDictionarySize()];

        for (int i = 0; i < from.lexicon.getDictionarySize(); ++i) {
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
     * @return the fromRegionIdToToRegionIdMap
     */
    public int[] getRegionIdToRegionId() {
        return fromRegionIdToToRegionIdMap;
    }

    /**
     * @return the wordIdToWordId
     */
    public int[] getWordIdToWordId() {
        return wordIdToWordId;
    }
}
