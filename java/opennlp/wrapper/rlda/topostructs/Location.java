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

import java.io.Serializable;

/**
 * A class encapsulating a location on a sphere along with various properties.
 * This is an extension of SmallLocation and adds a number of new properties:
 * <ul>
 * <li>The name of the location
 * <li>The type of the location (FIXME which types can occur?)
 * <li>The location's population
 * <li>The container of the location (FIXME document this)
 * </ul>
 * <p>
 * The methods of this class are basically uninteresting -- mostly just getter
 * and setter methods.
 * 
 * @author Taesun Moon
 * 
 */
public class Location implements Serializable {

    public int id;
    public int nameid;
    public Coordinate coord;

    public Location() {
    }

    public Location(int _id, int _nameid, Coordinate _coord) {
        id = _id;
        nameid = _nameid;
        coord = _coord;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Location other = (Location) obj;
        if (this.id != other.id) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return this.id;
    }
}
