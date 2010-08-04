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
package opennlp.rlda.topostructs;

import opennlp.textgrounder.topostructs.*;
import java.io.*;

/**
 * A Coordinate represents a location somewhere on a sphere and just
 * encapsulates a latitude and longitude. Various additional methods are created
 * to compare coordinates loosely and exactly, return points along a spiral radiating outward from the coordinate, 
 * 
 * @author Taesun Moon
 * 
 */
public class Coordinate implements Serializable {

    private final static double twoPI = 2 * Math.PI;
    public double longitude;
    public double latitude;

    public Coordinate(double lon, double lat) {
        longitude = lon;
        latitude = lat;
    }

    /**
     * Two coordinates are the same if they have the same type as well as
     * latitude and longitude.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Coordinate other = (Coordinate) obj;
        if (this.longitude != other.longitude) {
            return false;
        }
        if (this.latitude != other.latitude) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.longitude) ^ (Double.doubleToLongBits(this.longitude) >>> 32));
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.latitude) ^ (Double.doubleToLongBits(this.latitude) >>> 32));
        return hash;
    }
}
