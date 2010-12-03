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
package opennlp.textgrounder.topo;

import java.util.List;

public class PointSetRegion extends Region {
  private final List<Coordinate> coordinates;
  private final Coordinate center;
  private final double minLat;
  private final double maxLat;
  private final double minLng;
  private final double maxLng;

  public PointSetRegion(List<Coordinate> coordinates) {
    this.coordinates = coordinates;
    this.center = Coordinate.centroid(this.coordinates);

    double minLat = Double.POSITIVE_INFINITY;
    double maxLat = Double.NEGATIVE_INFINITY;
    double minLng = Double.POSITIVE_INFINITY;
    double maxLng = Double.NEGATIVE_INFINITY;

    for (Coordinate coordinate : this.coordinates) {
      double lat = coordinate.getLat();
      double lng = coordinate.getLng();

      if (lat < minLat) minLat = lat;
      if (lat > maxLat) maxLat = lat;
      if (lng < minLng) minLng = lng;
      if (lng > maxLng) maxLng = lng;
    }

    this.minLat = minLat;
    this.maxLat = maxLat;
    this.minLng = minLng;
    this.maxLng = maxLng;
  }

  public Coordinate getCenter() {
    return this.center;
  }

  public boolean contains(double lat, double lng) {
    return lat == this.center.getLat() && lng == this.center.getLng();
  }

  public double getMinLat() {
    return this.minLat;
  }

  public double getMaxLat() {
    return this.maxLat;
  }

  public double getMinLng() {
    return this.minLng;
  }

  public double getMaxLng() {
    return this.maxLng;
  }

  public List<Coordinate> getRepresentatives() {
    return this.coordinates;
  }

  @Override
  public double distance(Coordinate coordinate) {
    double minDistance = Double.POSITIVE_INFINITY;

    for (Coordinate representative : this.coordinates) {
      double distance = representative.distance(coordinate);
      if (distance < minDistance) {
        minDistance = distance;
      }
    }

    return minDistance;
  }
}

