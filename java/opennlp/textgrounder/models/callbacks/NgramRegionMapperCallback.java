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

/**
 * A callback class to 
 *
 * @author tsmoon
 */
public class NgramRegionMapperCallback extends UnigramRegionMapperCallback {

    /**
     *
     */
    public NgramRegionMapperCallback() {
        super();
    }

    /**
     *
     * @param regionMap
     * @param reverseRegionMap
     * @param nameToRegionIndex
     */
    public NgramRegionMapperCallback(Map<Integer, Region> regionMap,
          Map<Region, Integer> reverseRegionMap,
          Map<String, HashSet<Integer>> nameToRegionIndex) {
        super(regionMap, reverseRegionMap, nameToRegionIndex);
    }

    /**
     * 
     * @param placename
     */
    @Override
    public void setCurrentRegion(String placename) {
        if (!nameToRegionIndex.containsKey(placename)) {
            nameToRegionIndex.put(placename, new HashSet<Integer>());
        }
    }

    /**
     * 
     * @param placename
     * @param docSet
     */
    @Override
    public void confirmPlacenameTokens(String placename, DocumentSet docSet) {
        if (!docSet.hasWord(placename)) {
            docSet.addWord(placename);
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
        confirmPlacenameTokens(placename, docSet);
        int wordid = docSet.getIntForWord(placename);
        wordVector.add(wordid);
        toponymVector.add(1);
        documentVector.add(docIndex);

        for (LocationRegionPair lrp : currentLocationRegions) {
            ToponymRegionPair trp = new ToponymRegionPair(wordid, lrp.regionIndex);
            if (!toponymRegionToLocations.containsKey(trp)) {
                getToponymRegionToLocations().put(trp, new HashSet<Location>());
            }
            getToponymRegionToLocations().get(trp).add(lrp.location);
        }

        currentLocationRegions = new HashSet<LocationRegionPair>();
    }
}
