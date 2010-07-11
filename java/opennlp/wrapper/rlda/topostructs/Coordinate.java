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

    static private final long serialVersionUID = 10427645L;

    private final static double twoPI = 2*Math.PI;
    /**
     * Radius of the Earth in miles
     */
    private final static double EARTH_RADIUS_MI = 3963.191;

    public double longitude;
    public double latitude;
    
    public Coordinate(double lon, double lat) {
	longitude = lon;
	latitude = lat;
    }

    /**
     * Compare two coordinates to see if they're sufficiently close together.
     * @param other The other coordinate being compared
     * @param maxDiff Both latitude and longitude must be within this value
     * @return Whether the two coordinates are sufficiently close
     */
    public boolean looselyMatches(Coordinate other, double maxDiff) {
	return Math.abs(this.longitude - other.longitude) <= maxDiff && Math.abs(this.latitude - other.latitude) <= maxDiff;
    }

    /**
     * Create a KML expression corresponding to a polygonal cylinder (i.e. a
     * cylinder with a polygon as its base instead of a circle) located around
     * the given coordinate.
     * 
     * @param sides
     *            Number of sides of the polygon
     * @param radius
     *            Radius of the circle that circumscribes the polygon
     * @param height
     *            Height of the cylinder
     * @return A string representing a KML expression describing the coordinates
     *         of the cylinder
     */
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

    /**
     * Generate a new Coordinate that is the `n'th point along a spiral
     * radiating outward from the given coordinate. `initRadius' controls where
     * on the spiral the zeroth point is located. The constant local variable
     * `radianUnit' controls the spacing of the points (FIXME, should be an
     * optional parameter). The radius of the spiral increases by 1/10 (FIXME,
     * should be controllable) of `initRadius' every point.
     * 
     * @param n
     *            How far along the spiral to return a coordinate for
     * @param initRadius
     *            Where along the spiral the 0th point is located; this also
     *            controls how quickly the spiral grows outward
     * @return A new coordinate along the spiral
     */
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

    /**
     * Compute distance between two coordinates (along a great circle?), in
     * miles.
     * 
     * @param other
     * @return
     */
    public double computeDistanceTo(Coordinate other) {
	double thisRadLat = (this.latitude / 180) * Math.PI;
	double thisRadLong = (this.longitude / 180) * Math.PI;
	double otherRadLat = (other.latitude / 180) * Math.PI;
	double otherRadLong = (other.longitude / 180) * Math.PI;
	
	return EARTH_RADIUS_MI * Math.acos(Math.sin(thisRadLat)*Math.sin(otherRadLat)
					   + Math.cos(thisRadLat)*Math.cos(otherRadLat)*Math.cos(otherRadLong-thisRadLong));
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
