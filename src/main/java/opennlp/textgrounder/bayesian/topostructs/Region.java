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
package opennlp.textgrounder.bayesian.topostructs;

import java.io.Serializable;

/**
 * A rectangular region somewhere on the surface of the earth (most commonly,
 * 3x3 in degrees). Regions have the following properties:
 * <ul>
 * <li>minimum and maximum latitude
 * <li>minimum and maximum longitude
 * <li>center latitude and longitude (derived from the previous two)
 * </ul>
 * 
 * The only interesting method is `contains()', to determine whether the region
 * contains a specified point.
 * 
 * @author Taesun Moon
 * 
 */
public class Region implements Serializable {

    private static final long serialVersionUID = 42L;

    public final int id;
    public final double minLon;
    public final double maxLon;
    public final double minLat;
    public final double maxLat;
    /**
     * The longitude center of the region
     */
    public final double centLon;
    /**
     * The latitude center of the region
     */
    public final double centLat;

    public Region(int _id, double _minLon, double _maxLon, double _minLat,
          double _maxLat) {
        id = _id;

        minLon = _minLon;
        maxLon = _maxLon;
        minLat = _minLat;
        maxLat = _maxLat;

        centLon = (_maxLon + _minLon) / 2;
        centLat = (_maxLat + _minLat) / 2;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Region other = (Region) obj;
        if (this.centLon != other.centLon) {
            return false;
        }
        if (this.centLat != other.centLat) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 41 * hash + (int) (Double.doubleToLongBits(this.centLon) ^ (Double.doubleToLongBits(this.centLon) >>> 32));
        hash = 41 * hash + (int) (Double.doubleToLongBits(this.centLat) ^ (Double.doubleToLongBits(this.centLat) >>> 32));
        return hash;
    }
}
