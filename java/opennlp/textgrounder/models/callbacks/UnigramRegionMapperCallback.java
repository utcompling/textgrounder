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
import java.util.Hashtable;
import opennlp.textgrounder.io.DocumentSet;

import opennlp.textgrounder.topostructs.Region;

/**
 * A callback class to 
 *
 * @author tsmoon
 */
public class UnigramRegionMapperCallback extends NgramRegionMapperCallback {

    /**
     * 
     */
    HashSet<HashSet<Integer>> associatedSets;

    /**
     *
     */
    public UnigramRegionMapperCallback() {
    }

    /**
     *
     * @param regionMap
     * @param reverseRegionMap
     * @param nameToRegionIndex
     */
    public UnigramRegionMapperCallback(Hashtable<Integer, Region> regionMap,
          Hashtable<Region, Integer> reverseRegionMap,
          Hashtable<String, HashSet<Integer>> nameToRegionIndex) {
        super(regionMap, reverseRegionMap, nameToRegionIndex);
    }

    /**
     * 
     * @param region
     */
    @Override
    public void addToPlace(Region region) {
        int regionid = reverseRegionMap.get(region);
        for (HashSet<Integer> as : associatedSets) {
            as.add(regionid);
        }
    }

    /**
     *
     * @param placename
     */
    @Override
    public void setCurrentRegion(String placename) {
        String[] names = placename.split(" ");
        associatedSets = new HashSet<HashSet<Integer>>();
        for (String name : names) {
            if (!nameToRegionIndex.contains(name)) {
                nameToRegionIndex.put(name, new HashSet<Integer>());
            }
            associatedSets.add(nameToRegionIndex.get(name));
        }
    }

    /**
     *
     * @param placename
     * @param docSet
     */
    @Override
    public void confirmPlacenameTokens(String placename, DocumentSet docSet) {
        String[] names = placename.split(" ");
        for (String name : names) {
            if (!docSet.hasWord(name)) {
                docSet.addWord(name);
            }
        }
    }
}
