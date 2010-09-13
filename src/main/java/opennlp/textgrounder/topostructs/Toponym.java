///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Ben Wing, The University of Texas at Austin
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

import java.util.List;
import java.util.ArrayList;

/**
 * A Toponym represents a potentially ambiguous location name (e.g. "London"),
 * and lists the name of the toponym along with the locations that it maps to.
 * 
 * @author Ben Wing
 * 
 */
public class Toponym {

    public int id;
    public List<Location> locations;
    
    public Toponym(int id) {
        this.id = id;
        this.locations = new ArrayList<Location>();
    }

    public Toponym(int id, List<Location> locations) {
        this.id = id;
        this.locations = locations;
    }
    
    public void addLocation(Location loc) {
        locations.add(loc);
    }
    public String toString() {
        return id + ":" + locations;
    }

    /**
     * equals() and hashCode() auto-generated
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Toponym other = (Toponym) obj;
        if (id != other.id)
            return false;
        if (locations == null) {
            if (other.locations != null)
                return false;
        } else if (!locations.equals(other.locations))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        result = prime * result
                + ((locations == null) ? 0 : locations.hashCode());
        return result;
    }
}
