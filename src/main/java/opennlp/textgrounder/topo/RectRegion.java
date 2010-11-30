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

import java.util.ArrayList;
import java.util.List;

public class RectRegion extends Region {
  private final double minLat;
  private final double maxLat;
  private final double minLng;
  private final double maxLng;

  private RectRegion(double minLat, double maxLat, double minLng, double maxLng) {
    this.minLat = minLat;
    this.maxLat = maxLat;
    this.minLng = minLng;
    this.maxLng = maxLng;
  }

  public static RectRegion fromRadians(double minLat, double maxLat, double minLng, double maxLng) {
    return new RectRegion(minLat, maxLat, minLng, maxLng);
  }

  public static RectRegion fromDegrees(double minLat, double maxLat, double minLng, double maxLng) {
    return new RectRegion(minLat * Math.PI / 180.0,
                          maxLat * Math.PI / 180.0,
                          minLng * Math.PI / 180.0,
                          maxLng * Math.PI / 180.0);
  }

  /**
   * Returns the average of the minimum and maximum values for latitude and
   * longitude. Should be changed to avoid problems around zero degrees.
   */
  public Coordinate getCenter() {
    return Coordinate.fromRadians((this.maxLat - this.minLat) / 2.0,
                                  (this.maxLng - this.minLng) / 2.0);
  }

  public boolean contains(double lat, double lng) {
    return lat >= this.minLat &&
           lat <= this.maxLat &&
           lng >= this.minLng &&
           lng <= this.maxLng;
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
    List<Coordinate> representatives = new ArrayList<Coordinate>(4);
    representatives.add(Coordinate.fromRadians(this.minLat, this.minLng));
    representatives.add(Coordinate.fromRadians(this.maxLat, this.minLng));
    representatives.add(Coordinate.fromRadians(this.maxLat, this.maxLng));
    representatives.add(Coordinate.fromRadians(this.minLat, this.maxLng));
    return representatives;
  }
}

