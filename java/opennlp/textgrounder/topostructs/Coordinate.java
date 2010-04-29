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

public class Coordinate {
    private final static double twoPI = 2*Math.PI;

    public double longitude;
    public double latitude;
    
    public Coordinate(double lon, double lat) {
	longitude = lon;
	latitude = lat;
    }

    public String toKMLPolygon(int sides, double radius, double height) {
	final double radianUnit = twoPI/sides;
	final double startRadian = radianUnit/2;
	double currentRadian = startRadian;

	StringBuilder sb = new StringBuilder("<coordinates>\n\t\t\t\t\t\t\t\t");
	
	while (currentRadian <= twoPI+startRadian) {
	    sb.append(latitude+radius*Math.cos(currentRadian)).append(",").append(longitude+radius*Math.sin(currentRadian)).append(",").append(height).append("\n\t\t\t\t\t\t\t\t");
	    currentRadian += radianUnit;
	}
	sb.append("</coordinates>");

	return sb.toString();
    }

    public Coordinate getNthSpiralPoint(int n, double initRadius) {
	if(n == 0)
	  return this;
	
	final double radianUnit = twoPI/20;//10
	
	double radius = initRadius + (initRadius * .1) * n;

	double angle = radianUnit/2 + 1.1 * radianUnit * n;

	return new Coordinate(this.longitude + radius * Math.sin(angle), this.latitude + radius * Math.cos(angle));
    }

    public String toString() {
	return latitude + "," + longitude;
    }
}
