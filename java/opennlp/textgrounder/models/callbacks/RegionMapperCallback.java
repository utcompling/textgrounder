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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
    protected Map<String, HashSet<Integer>> nameToRegionIndex;
    /**
     *
     */
    protected HashSet<LocationRegionPair> currentLocationRegions;
    /**
     * 
     */
    protected Map<ToponymRegionPair, HashSet<Location>> toponymRegionToLocations;
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
        nameToRegionIndex = new HashMap<String, HashSet<Integer>>();
        toponymRegionToLocations = new HashMap<ToponymRegionPair, HashSet<Location>>();
        currentLocationRegions = new HashSet<LocationRegionPair>();
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
        currentLocationRegions.add(new LocationRegionPair(loc, regionid));
    }

    /**
     *
     * @param placename
     * @param docSet
     */
    public void addAll(String placename, Lexicon docSet) {
        int wordid = docSet.getIntForWord(placename);

        if(!nameToRegionIndex.containsKey(placename)) {
            nameToRegionIndex.put(placename, new HashSet<Integer>());
        }
        HashSet<Integer> currentRegions = nameToRegionIndex.get(placename);

        for (LocationRegionPair lrp : currentLocationRegions) {
            ToponymRegionPair trp = new ToponymRegionPair(wordid, lrp.regionIndex);
            if (!toponymRegionToLocations.containsKey(trp)) {
                toponymRegionToLocations.put(trp, new HashSet<Location>());
            }
            toponymRegionToLocations.get(trp).add(lrp.location);
            currentRegions.add(lrp.regionIndex);
        }
        
        currentLocationRegions = new HashSet<LocationRegionPair>();
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
    public Map<String, HashSet<Integer>> getNameToRegionIndex() {
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
    public Map<ToponymRegionPair, HashSet<Location>> getToponymRegionToLocations() {
        return toponymRegionToLocations;
    }
}
