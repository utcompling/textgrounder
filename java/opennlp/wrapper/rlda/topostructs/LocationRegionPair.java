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
package opennlp.wrapper.rlda.topostructs;

/**
 *
 * @author tsmoon
 */
public class LocationRegionPair {

    public final int locationIndex;
    public final int regionIndex;

    public LocationRegionPair(Location loc, int ridx) {
        locationIndex = loc.hashCode();
        regionIndex = ridx;
    }

    public LocationRegionPair(int _locationIndex, int _regionIndex) {
        locationIndex = _locationIndex;
        regionIndex = _regionIndex;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final LocationRegionPair other = (LocationRegionPair) obj;
        if (this.locationIndex != other.locationIndex) {
            return false;
        }
        if (this.regionIndex != other.regionIndex) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 47 * hash + this.locationIndex;
        hash = 47 * hash + this.regionIndex;
        return hash;
    }
}
