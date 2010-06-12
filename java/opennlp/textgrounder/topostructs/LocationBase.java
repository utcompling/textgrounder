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

import java.io.*;

public class LocationBase implements Serializable {

    static private final long serialVersionUID = 13371511L;
    public int id;
    public int nameid;
    public Coordinate coord;
    /**
     * Counts of location in given text. Is double type to accomodate
     * hyperparameters and fractional counts;
     */
    public double count;

    public LocationBase(int _id, int _nameid, Coordinate _coord, int _count) {
        id = _id;
        nameid = _nameid;
        coord = _coord;
        count = _count;
    }

    public boolean looselyMatches(LocationBase other, double maxDiff) {
        return this.name.equals(other.name) && this.coord.looselyMatches(other.coord, maxDiff);
    }

    /*    public Location(int _id, String _nameid, String type, double lon, double lat, int pop, String container, int count) {
    Location(_id, _nameid, type, new Coordinate(lon, lat), pop, container, count);
    }*/
    @Override
    public String toString() {
        return id + ", " + name + ", " + type + ", (" + coord + "), " + pop + ", " + container;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final LocationBase other = (LocationBase) obj;
        if (this.id != other.id) {
            return false;
        }
        return true;
    }

    public double computeDistanceTo(LocationBase other) {
        return this.coord.computeDistanceTo(other.coord);
    }
}
