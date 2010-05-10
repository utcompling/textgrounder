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
package opennlp.textgrounder.models.callbacks;

import gnu.trove.TIntHashSet;
import gnu.trove.TIntObjectHashMap;
import gnu.trove.TIntObjectIterator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import opennlp.textgrounder.textstructs.Lexicon;
import opennlp.textgrounder.topostructs.*;

/**
 * A callback class to 
 *
 * @author tsmoon
 */
public class RegionMapperCallback {

    /**
     * Table from index to region
     */
    protected Map<Integer, Region> regionMap;
    /**
     * Table from region to index. Reverse storage table for regionMap.
     */
    protected Map<Region, Integer> reverseRegionMap;
    /**
     * Table from placename to set of region indexes. The indexes and their
     * referents are stored in regionMap.
     */
    protected TIntObjectHashMap<TIntHashSet> nameToRegionIndex;
    /**
     *
     */
    protected TIntObjectHashMap<LocationRegionPair> currentLocationRegions;
    /**
     * 
     */
    protected TIntObjectHashMap<HashSet<Location>> toponymRegionToLocations;
    /**
     *
     */
    protected int numRegions;

    /**
     *
     */
    public RegionMapperCallback() {
        numRegions = 0;
        regionMap = new HashMap<Integer, Region>();
        reverseRegionMap = new HashMap<Region, Integer>();
        nameToRegionIndex = new TIntObjectHashMap<TIntHashSet>();
        toponymRegionToLocations = new TIntObjectHashMap<HashSet<Location>>();
        currentLocationRegions = new TIntObjectHashMap<LocationRegionPair>();
    }

    /**
     *
     * @param loc 
     * @param region
     */
    public void addToPlace(Location loc, Region region) {
        if (!reverseRegionMap.containsKey(region)) {
            reverseRegionMap.put(region, numRegions);
            regionMap.put(numRegions, region);
            numRegions += 1;
        }
        int regionid = reverseRegionMap.get(region);
        LocationRegionPair lrp = new LocationRegionPair(loc, regionid);
        currentLocationRegions.put(lrp.hashCode(), lrp);
    }

    /**
     *
     * @param placename
     * @param lexicon
     */
    public void addAll(String placename, Lexicon lexicon) {
        int wordid = lexicon.getIntForWord(placename);

        if (!nameToRegionIndex.containsKey(wordid)) {
            nameToRegionIndex.put(wordid, new TIntHashSet());
        }
        TIntHashSet currentRegions = nameToRegionIndex.get(wordid);

        for (TIntObjectIterator<LocationRegionPair> it = currentLocationRegions.iterator();
              it.hasNext();) {
            it.advance();
            LocationRegionPair lrp = it.value();
            ToponymRegionPair trp = new ToponymRegionPair(wordid, lrp.regionIndex);
            if (!toponymRegionToLocations.containsKey(trp.hashCode())) {
                toponymRegionToLocations.put(trp.hashCode(), new HashSet<Location>());
            }
            toponymRegionToLocations.get(trp.hashCode()).add(lrp.location);
            currentRegions.add(lrp.regionIndex);
        }

        currentLocationRegions = new TIntObjectHashMap<LocationRegionPair>();
    }

    /**
     * @return the regionMap
     */
    public Map<Integer, Region> getRegionMap() {
        return regionMap;
    }

    /**
     * @return the reverseRegionMap
     */
    public Map<Region, Integer> getReverseRegionMap() {
        return reverseRegionMap;
    }

    /**
     * @return the nameToRegionIndex
     */
    public TIntObjectHashMap<TIntHashSet> getNameToRegionIndex() {
        return nameToRegionIndex;
    }

    /**
     * @return the numRegions
     */
    public int getNumRegions() {
        return numRegions;
    }

    /**
     * @return the toponymRegionToLocations
     */
    public TIntObjectHashMap<HashSet<Location>> getToponymRegionToLocations() {
        return toponymRegionToLocations;
    }
}
