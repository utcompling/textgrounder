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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import opennlp.textgrounder.io.DocumentSet;

import opennlp.textgrounder.topostructs.Region;

/**
 * A callback class to 
 *
 * @author tsmoon
 */
public abstract class RegionMapperCallback {

    /**
     * Table from index to region
     */
    protected Hashtable<Integer, Region> regionMap;
    /**
     * Table from region to index. Reverse storage table for regionMap.
     */
    protected Hashtable<Region, Integer> reverseRegionMap;
    /**
     * Table from placename to set of region indexes. The indexes and their
     * referents are stored in regionMap.
     */
    protected Hashtable<String, HashSet<Integer>> nameToRegionIndex;
    /**
     * 
     */
    protected HashSet<Integer> currentRegionHashSet;
    /**
     * 
     */
    protected int numRegions;

    /**
     *
     */
    public RegionMapperCallback() {
        numRegions = 0;
        regionMap = new Hashtable<Integer, Region>();
        reverseRegionMap = new Hashtable<Region, Integer>();
        nameToRegionIndex = new Hashtable<String, HashSet<Integer>>();
    }

    public RegionMapperCallback(Hashtable<Integer, Region> regionMap,
          Hashtable<Region, Integer> reverseRegionMap,
          Hashtable<String, HashSet<Integer>> nameToRegionIndex) {
        setMaps(regionMap, reverseRegionMap, nameToRegionIndex);
    }

    /**
     * 
     */
    public void setMaps(Hashtable<Integer, Region> regionMap,
          Hashtable<Region, Integer> reverseRegionMap,
          Hashtable<String, HashSet<Integer>> nameToRegionIndex) {
        this.regionMap = regionMap;
        this.reverseRegionMap = reverseRegionMap;
        this.nameToRegionIndex = nameToRegionIndex;
    }

    /**
     *
     * @param region
     */
    public abstract void addRegion(Region region);

    /**
     *
     * @param region
     */
    public abstract void addToPlace(Region region);

    /**
     * 
     * @param placename
     */
    public abstract void setCurrentRegion(String placename);

    public abstract void addPlacenameTokens(String placename, DocumentSet docSet,
          ArrayList<Integer> wordVector, ArrayList<Integer> toponymVector);

    /**
     * 
     * @param placename
     * @param docSet
     */
    public abstract void confirmPlacenameTokens(String placename,
          DocumentSet docSet);

    /**
     * @return the regionMap
     */
    public Hashtable<Integer, Region> getRegionMap() {
        return regionMap;
    }

    /**
     * @return the reverseRegionMap
     */
    public Hashtable<Region, Integer> getReverseRegionMap() {
        return reverseRegionMap;
    }

    /**
     * @return the nameToRegionIndex
     */
    public Hashtable<String, HashSet<Integer>> getNameToRegionIndex() {
        return nameToRegionIndex;
    }

    /**
     * @return the numRegions
     */
    public int getNumRegions() {
        return numRegions;
    }
}
