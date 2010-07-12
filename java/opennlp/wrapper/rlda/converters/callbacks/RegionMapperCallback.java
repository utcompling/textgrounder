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
package opennlp.wrapper.rlda.converters.callbacks;

import gnu.trove.TIntHashSet;
import gnu.trove.TIntObjectHashMap;
import gnu.trove.TIntObjectIterator;
import gnu.trove.TObjectIntHashMap;
import java.util.HashSet;
import opennlp.wrapper.rlda.textstructs.*;
import opennlp.wrapper.rlda.topostructs.*;

/**
 * A callback class to 
 *
 * @author tsmoon
 */
public class RegionMapperCallback {

    /**
     * Table from index to region
     */
    protected TIntObjectHashMap<Region> idxToRegionMap;
    /**
     * Table from region to index. Reverse storage table for idxToRegionMap.
     */
    protected TObjectIntHashMap<Region> regionToIdxMap;
    /**
     * Table from placename to set of region indexes. The indexes and their
     * referents are stored in idxToRegionMap.
     */
    protected TIntObjectHashMap<TIntHashSet> placenameIdxToRegionIndexSet;
    /**
     * Temporary variable for keeping track of regions that have been assigned
     * to the current location
     */
    protected HashSet<LocationRegionPair> currentLocationRegions;
    /**
     * 
     */
    protected TIntObjectHashMap<TIntHashSet> placenameRegionToLocationIndexSet;
    /**
     *
     */
    protected int numRegions;

    /**
     *
     */
    public RegionMapperCallback() {
        numRegions = 0;
        idxToRegionMap = new TIntObjectHashMap<Region>();
        regionToIdxMap = new TObjectIntHashMap<Region>();
        placenameIdxToRegionIndexSet = new TIntObjectHashMap<TIntHashSet>();
        placenameRegionToLocationIndexSet = new TIntObjectHashMap<TIntHashSet>();
        currentLocationRegions = new HashSet<LocationRegionPair>();
    }

    /**
     *
     * @param _loc
     * @param _region
     */
    public void addToPlace(Location _loc, Region _region) {
        if (!regionToIdxMap.containsKey(_region)) {
            regionToIdxMap.put(_region, numRegions);
            idxToRegionMap.put(numRegions, _region);
            numRegions += 1;
        }
        int regionid = regionToIdxMap.get(_region);
        LocationRegionPair locationRegionPair = new LocationRegionPair(_loc, regionid);
        currentLocationRegions.add(locationRegionPair);
    }

    /**
     *
     * @param placename
     * @param lexicon
     */
    public void addAll(String placename, Lexicon lexicon) {
        int wordid = lexicon.getIntForWord(placename);
        addAll(wordid);
    }

    public void addAll(int _placenameid) {
        if (!placenameIdxToRegionIndexSet.containsKey(_placenameid)) {
            placenameIdxToRegionIndexSet.put(_placenameid, new TIntHashSet());
        }
        TIntHashSet currentRegions = placenameIdxToRegionIndexSet.get(_placenameid);

        for (LocationRegionPair locationRegionPair : currentLocationRegions) {
            ToponymRegionPair toponymRegionPair = new ToponymRegionPair(_placenameid, locationRegionPair.regionIndex);
            if (!placenameRegionToLocationIndexSet.containsKey(toponymRegionPair.hashCode())) {
                placenameRegionToLocationIndexSet.put(toponymRegionPair.hashCode(), new TIntHashSet());
            }
            placenameRegionToLocationIndexSet.get(toponymRegionPair.hashCode()).add(locationRegionPair.locationIndex);
            currentRegions.add(locationRegionPair.regionIndex);
        }
        currentLocationRegions = new HashSet<LocationRegionPair>();
    }

    public void trimToSize() {
        idxToRegionMap.trimToSize();
        regionToIdxMap.trimToSize();
        for (TIntObjectIterator<TIntHashSet> it = placenameIdxToRegionIndexSet.iterator();
              it.hasNext();) {
            it.advance();
            it.value().trimToSize();
        }
        placenameIdxToRegionIndexSet.trimToSize();

        for (TIntObjectIterator<TIntHashSet> it = placenameRegionToLocationIndexSet.iterator();
              it.hasNext();) {
            it.advance();
            it.value().trimToSize();
        }
        placenameRegionToLocationIndexSet.trimToSize();
    }

    /**
     * @return the idxToRegionMap
     */
    public TIntObjectHashMap<Region> getIdxToRegionMap() {
        return idxToRegionMap;
    }

    /**
     * @return the regionToIdxMap
     */
    public TObjectIntHashMap<Region> getRegionToIdxMap() {
        return regionToIdxMap;
    }

    /**
     * @return the placenameIdxToRegionIndexSet
     */
    public TIntObjectHashMap<TIntHashSet> getPlacenameIdxToRegionIndexSet() {
        return placenameIdxToRegionIndexSet;
    }

    /**
     * @return the numRegions
     */
    public int getNumRegions() {
        return numRegions;
    }

    /**
     * @return the placenameRegionToLocationIndexSet
     */
    public TIntObjectHashMap<TIntHashSet> getPlacenameRegionToLocationIndexSet() {
        return placenameRegionToLocationIndexSet;
    }
}
