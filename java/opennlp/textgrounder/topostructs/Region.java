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

public class Region {

    public double minLon;
    public double maxLon;
    public double minLat;
    public double maxLat;
    /**
     * The longitude center of the region
     */
    public double centLon;
    /**
     * The latitude center of the region
     */
    public double centLat;

    public Region(double minLon, double maxLon, double minLat, double maxLat) {
	this.minLon = minLon;
	this.maxLon = maxLon;
	this.minLat = minLat;
	this.maxLat = maxLat;

        centLon = (maxLon + minLon) / 2;
        centLat = (maxLat + minLat) / 2;
    }

    public boolean contains(Coordinate coord) {
	if(coord.longitude >= minLon && coord.longitude <= maxLon
	   && coord.latitude >= minLat && coord.latitude <= maxLat)
	    return true;
	return false;
    }

    public boolean contains(double lon, double lat) {
	return contains(new Coordinate(lon, lat));
    }

}
