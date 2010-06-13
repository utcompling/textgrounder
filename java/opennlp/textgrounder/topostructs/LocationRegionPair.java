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
package opennlp.textgrounder.topostructs;

/**
 *
 * @author tsmoon
 */
public class LocationRegionPair<E extends SmallLocation> {

    public final E location;
    public final int regionIndex;

    public LocationRegionPair(E loc, int ridx) {
        location = loc;
        regionIndex = ridx;
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
        if (!this.location.equals(other.location)) {
            return false;
        }
        if (this.regionIndex != other.regionIndex) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 59 * hash + this.location.id;
        hash = 59 * hash + this.regionIndex;
        return hash;
    }
}
