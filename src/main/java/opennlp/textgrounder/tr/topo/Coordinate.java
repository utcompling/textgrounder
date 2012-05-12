///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Travis Brown, The University of Texas at Austin
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
package opennlp.textgrounder.tr.topo;

import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;

import opennlp.textgrounder.tr.util.FastTrig;

public class Coordinate implements Serializable {

    private static final long serialVersionUID = 42L;

    private final double lng;
    private final double lat;
    
    public Coordinate(double lat, double lng) {
      this.lng = lng;
      this.lat = lat;
    }

    public static Coordinate fromRadians(double lat, double lng) {
      return new Coordinate(lat, lng);
    }

    public static Coordinate fromDegrees(double lat, double lng) {
      return new Coordinate(lat * Math.PI / 180.0, lng * Math.PI / 180.0);
    }

    public double getLat() {
      return this.lat;
    }

    public double getLng() {
      return this.lng;
    }

    public double getLatDegrees() {
      return this.lat * 180.0 / Math.PI;
    }

    public double getLngDegrees() {
      return this.lng * 180.0 / Math.PI;
    }

    /**
     * Compare two coordinates to see if they're sufficiently close together.
     * @param other The other coordinate being compared
     * @param maxDiff Both lat and lng must be within this value
     * @return Whether the two coordinates are sufficiently close
     */
    public boolean looselyMatches(Coordinate other, double maxDiff) {
	return Math.abs(this.lat - other.lat) <= maxDiff &&
               Math.abs(this.lng - other.lng) <= maxDiff;
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
      if (n == 0) {
        return this;
      }

      final double radianUnit = Math.PI / 10.0;
      double radius = initRadius + (initRadius * 0.1) * n;
      double angle = radianUnit / 2.0 + 1.1 * radianUnit * n;

      double newLatDegrees = this.getLatDegrees() + radius * Math.cos(angle);
      double newLngDegrees = this.getLngDegrees() + radius * Math.sin(angle);

      return new Coordinate(newLatDegrees * Math.PI / 180.0, newLngDegrees * Math.PI / 180.0);
    }

    public String toString() {
      return String.format("%.02f,%.02f", this.getLatDegrees(), this.getLngDegrees());
    }

    public double distance(Coordinate other) {
      if(this.lat == other.lat && this.lng == other.lng)
        return 0;
      return Math.acos(Math.sin(this.lat) * Math.sin(other.lat)
                                + Math.cos(this.lat) * Math.cos(other.lat) * Math.cos(other.lng - this.lng));
    }

    public double distanceInKm(Coordinate other) {
        return 6372.8 * this.distance(other);
    }

    public double distanceInMi(Coordinate other) {
        return .621371 * this.distanceInKm(other);
    }

    /**
     * Compute the approximate centroid by taking the average of the latitudes
     * and longitudes.
     */
    public static Coordinate centroid(List<Coordinate> coordinates) {
      double latSins = 0.0;
      double latCoss = 0.0;
      double lngSins = 0.0;
      double lngCoss = 0.0;

      for (int i = 0; i < coordinates.size(); i++) {
        latSins += Math.sin(coordinates.get(i).getLat());
        latCoss += Math.cos(coordinates.get(i).getLat());
        lngSins += Math.sin(coordinates.get(i).getLng());
        lngCoss += Math.cos(coordinates.get(i).getLng());
      }

      latSins /= coordinates.size();
      latCoss /= coordinates.size();
      lngSins /= coordinates.size();
      lngCoss /= coordinates.size();

      double lat = Math.atan2(latSins, latCoss);
      double lng = Math.atan2(lngSins, lngCoss);

      return Coordinate.fromRadians(lat, lng);
    }

    public static List<Coordinate> removeNaNs(List<Coordinate> coordinates) {
        List<Coordinate> toReturn = new ArrayList<Coordinate>();
        for(Coordinate coord : coordinates) {
            if(!(Double.isNaN(coord.getLatDegrees()) || Double.isNaN(coord.getLngDegrees()))) {
                toReturn.add(coord);
            }
        }
        return toReturn;
    }

    @Override
    public boolean equals(Object other) {
      return other != null &&
             other.getClass() == this.getClass() &&
             ((Coordinate) other).lat == this.lat &&
             ((Coordinate) other).lng == this.lng;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.lng) ^ (Double.doubleToLongBits(this.lng) >>> 32));
        hash = 29 * hash + (int) (Double.doubleToLongBits(this.lat) ^ (Double.doubleToLongBits(this.lat) >>> 32));
        return hash;
    }
}

