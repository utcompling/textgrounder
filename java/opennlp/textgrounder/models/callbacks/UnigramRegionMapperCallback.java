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

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import opennlp.textgrounder.io.DocumentSet;
import opennlp.textgrounder.topostructs.*;
import opennlp.textgrounder.util.Constants;

/**
 * A callback class to 
 *
 * @author tsmoon
 */
public class UnigramRegionMapperCallback extends RegionMapperCallback {

    /**
     *
     */
    private String[] currentPlacenames;
    /**
     * 
     */
    protected HashSet<LocationRegionPair> currentLocationRegions;

    /**
     *
     */
    public UnigramRegionMapperCallback() {
        super();
        currentLocationRegions = new HashSet<LocationRegionPair>();
    }

    /**
     *
     * @param regionMap
     * @param reverseRegionMap
     * @param nameToRegionIndex
     */
    public UnigramRegionMapperCallback(Map<Integer, Region> regionMap,
          Map<Region, Integer> reverseRegionMap,
          Map<String, HashSet<Integer>> nameToRegionIndex) {
        super(regionMap, reverseRegionMap, nameToRegionIndex);
        currentLocationRegions = new HashSet<LocationRegionPair>();
    }

    /**
     * 
     * @param region
     */
    @Override
    public void addToPlace(Location loc, Region region) {
//        if (loc.coord.latitude > Constants.EPSILON && loc.coord.longitude > Constants.EPSILON) {
        if (!reverseRegionMap.containsKey(region)) {
            reverseRegionMap.put(region, numRegions);
            regionMap.put(numRegions, region);
            numRegions += 1;
        }
        int regionid = reverseRegionMap.get(region);
        currentLocationRegions.add(new LocationRegionPair(loc, regionid));
//        }
    }

    /**
     *
     * @param placename
     */
    @Override
    public void setCurrentRegion(String placename) {
        currentPlacenames = placename.split(" ");
        for (String name : currentPlacenames) {
            if (!nameToRegionIndex.containsKey(name)) {
                nameToRegionIndex.put(name, new HashSet<Integer>());
            }
        }
    }

    /**
     *
     * @param region
     */
    public void addRegion(Region region) {
        if (!reverseRegionMap.containsKey(region)) {
            regionMap.put(numRegions, region);
            reverseRegionMap.put(region, numRegions);
            numRegions += 1;
        }
    }

    /**
     * 
     * @param placename
     * @param docSet
     * @param wordVector
     * @param toponymVector
     * @param documentVector
     * @param docIndex
     * @param locs
     */
    @Override
    public void addAll(String placename, DocumentSet docSet,
          List<Integer> wordVector, List<Integer> toponymVector,
          List<Integer> documentVector, int docIndex, List<Location> locs) {
        String[] names = placename.split(" ");
        for (String name : names) {
            addAll(name, docSet, wordVector, toponymVector, documentVector, docIndex);
//            confirmPlacenameTokens(name, docSet);
//            int wordid = docSet.getIntForWord(name);
//            wordVector.add(wordid);
//            toponymVector.add(1);
//            documentVector.add(docIndex);
//            if (!nameToRegionIndex.containsKey(name)) {
//                nameToRegionIndex.put(name, new HashSet<Integer>());
//            }
//            HashSet<Integer> currentRegions = nameToRegionIndex.get(name);
//
//
//            for (LocationRegionPair lrp : currentLocationRegions) {
//                ToponymRegionPair trp = new ToponymRegionPair(wordid, lrp.regionIndex);
//                if (!toponymRegionToLocations.containsKey(trp)) {
//                    toponymRegionToLocations.put(trp, new HashSet<Location>());
//                }
//                toponymRegionToLocations.get(trp).add(lrp.location);
//                currentRegions.add(lrp.regionIndex);
//            }
        }

        currentLocationRegions = new HashSet<LocationRegionPair>();
    }

    /**
     *
     * @param placename
     * @param docSet
     * @param wordVector
     * @param toponymVector
     * @param documentVector
     * @param docIndex
     */
    public void addAll(String name, DocumentSet docSet,
          List<Integer> wordVector, List<Integer> toponymVector,
          List<Integer> documentVector, int docIndex) {
        confirmPlacenameTokens(name, docSet);
        int wordid = docSet.getIntForWord(name);
        wordVector.add(wordid);
        toponymVector.add(1);
        documentVector.add(docIndex);
        if (!nameToRegionIndex.containsKey(name)) {
            nameToRegionIndex.put(name, new HashSet<Integer>());
        }
        HashSet<Integer> currentRegions = nameToRegionIndex.get(name);

        for (LocationRegionPair lrp : currentLocationRegions) {
            ToponymRegionPair trp = new ToponymRegionPair(wordid, lrp.regionIndex);
            if (!toponymRegionToLocations.containsKey(trp)) {
                toponymRegionToLocations.put(trp, new HashSet<Location>());
            }
            toponymRegionToLocations.get(trp).add(lrp.location);
            currentRegions.add(lrp.regionIndex);
        }
    }
}
