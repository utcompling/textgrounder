///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
//
//  This library is free software; you can redistribute it and/or
//  modify it under the terms of the GNU Lesser General Public
//  License as published by the Free Software Foundation; either
//  version 3 of the License, or (at your option) any later version.
//
//  This library is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public
//  License along with this program; if not, write to the Free Software
//  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.models.callbacks;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import opennlp.textgrounder.io.DocumentSet;
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
    public void addAll(String placename, DocumentSet docSet) {
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
