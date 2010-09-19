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

public class RectRegion extends Region {
  private final double minLat;
  private final double maxLat;
  private final double minLng;
  private final double maxLng;

  public RectRegion(double minLat, double maxLat, double minLng, double maxLng) {
    this.minLat = minLat;
    this.maxLat = maxLat;
    this.minLng = minLng;
    this.maxLng = maxLng;
  }

  public Coordinate getCenter() {
    return new Coordinate(this.maxLat - this.minLat, this.maxLng - this.minLng);
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
}

